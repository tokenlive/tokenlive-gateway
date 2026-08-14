package providers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/upstream"

	"go.uber.org/zap"
)

// anthropicResponsesInvoker serves client /responses requests on an Anthropic Messages
// upstream: request Responses→Messages, response Messages→Responses (stream + non-stream).
type anthropicResponsesInvoker struct{}

func (i *anthropicResponsesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	ap, ok := p.(*AnthropicProvider)
	if !ok {
		return fmt.Errorf("expected *AnthropicProvider, got %T", p)
	}

	// 1. Protocol translation: Responses -> Anthropic Messages (pure function).
	// gctx.Model is the engine-resolved upstream model name.
	var maxOutputTokens int
	if gctx.SelectedEndpoint != nil {
		maxOutputTokens = int(gctx.SelectedEndpoint.MaxOutputTokens)
	}
	req, err := translate.ResponsesRequestToMessages(gctx.RawBody, gctx.Model, maxOutputTokens)
	if err != nil {
		return fmt.Errorf("translate responses request: %w", err)
	}
	for _, w := range req.Warnings {
		gctx.Logger(zap.L()).Warn("responses to messages translation degraded",
			zap.String("warning", w), zap.String("model", gctx.Model))
	}
	gctx.RawBody = req.Body

	// 2. Call upstream /messages (same auth as native messages invoker).
	endpoint := ap.baseURL + "/messages"

	h := make(http.Header)
	h.Set("Content-Type", "application/json")

	isOAuth := false
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.AuthType == "oauth_token" {
		isOAuth = true
	}

	h.Set("Authorization", "Bearer "+ap.apiKey)
	if !isOAuth {
		h.Set("x-api-key", ap.apiKey)
	}
	h.Set("anthropic-version", "2023-06-01")

	streamMode := upstream.Consume
	if gctx.IsStream {
		streamMode = upstream.Handoff
	}

	resp, err := upstream.Call(gctx, upstream.Request{
		Client: ap.client,
		URL:    endpoint,
		Body:   gctx.RawBody,
		Header: h,
		Stream: streamMode,
	})
	if err != nil {
		return enrichAnthropicError(err, gctx.UpstreamBody)
	}

	// 3. Translate response Messages -> Responses.
	if gctx.IsStream {
		// Handoff: handleAnthropicResponsesStream owns and closes the body.
		return handleAnthropicResponsesStream(gctx, resp)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	gctx.TriggerFirstByte()

	res, err := translate.MessagesResponseToResponses(body, responsesReplyModel(gctx))
	if err != nil {
		return fmt.Errorf("translate messages response: %w", err)
	}
	gctx.UpstreamBody = res.Body
	var result map[string]interface{}
	if err := json.Unmarshal(res.Body, &result); err != nil {
		return fmt.Errorf("parse translated response: %w", err)
	}
	gctx.Response = result
	llm.ApplyUsage(gctx, res.Usage.InputTokens, res.Usage.OutputTokens, res.CachedTokens, res.CacheCreationTokens)
	return nil
}

// responsesReplyModel returns the client-facing model name for response payloads.
func responsesReplyModel(gctx *core.GatewayContext) string {
	if gctx.OriginalModel != "" {
		return gctx.OriginalModel
	}
	return gctx.Model
}

// enrichAnthropicError rewrites the upstream error body into the Responses error
// envelope inside the error string, keeping the "upstream error: status N" prefix
// that engine.getErrorCode parses. Falls back to the original error otherwise.
func enrichAnthropicError(err error, upstreamBody []byte) error {
	if len(upstreamBody) == 0 {
		return err
	}
	converted, ok := translate.MessagesErrorToResponses(upstreamBody)
	if !ok {
		return err
	}
	status := 0
	if _, scanErr := fmt.Sscanf(err.Error(), "upstream error: status %d", &status); scanErr != nil || status == 0 {
		return err
	}
	return fmt.Errorf("upstream error: status %d, body: %s", status, string(converted))
}

// handleAnthropicResponsesStream converts upstream Anthropic Messages SSE into
// Responses SSE events, writing them directly to the client. Token stats are
// written to gctx from the translation side channel (AnthropicTokenExtractor semantics).
func handleAnthropicResponsesStream(gctx *core.GatewayContext, resp *http.Response) error {
	defer resp.Body.Close()

	gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
	gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
	gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
	gctx.ResponseWriter.WriteHeader(http.StatusOK)

	flusher, hasFlusher := gctx.ResponseWriter.(http.Flusher)

	parser := llm.NewSSEParser()
	stream := translate.NewMessagesToResponsesStream(responsesReplyModel(gctx))
	buf := make([]byte, 4096)

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			gctx.TriggerFirstByte()

			events := parser.Feed(buf[:n])
			for _, ev := range events {
				if ev.Done {
					continue // Anthropic sends no [DONE]; guard against compat proxies
				}

				out, meta := stream.FeedJSON(ev.Data)
				llm.ApplyUsage(gctx, meta.InputTokens, meta.OutputTokens, meta.CachedTokens, meta.CacheCreationTokens)
				gctx.TransmittedChars += meta.TransmittedChars

				if gctx.Tags == nil {
					gctx.Tags = make(map[string]string)
				}
				if gctx.Tags["response_id"] == "" && stream.ResponseID() != "" {
					gctx.Tags["response_id"] = stream.ResponseID()
				}
				if gctx.Tags["response_model"] == "" {
					gctx.Tags["response_model"] = responsesReplyModel(gctx)
				}

				for _, oe := range out {
					if _, werr := fmt.Fprintf(gctx.ResponseWriter, "event: %s\ndata: %s\n\n", oe.Event, string(oe.Data)); werr != nil {
						return werr
					}
				}
				if hasFlusher {
					flusher.Flush()
				}

				if meta.EmitDone {
					// A terminal response event (completed/failed) was emitted above,
					// so engine's premature-close detector must stay quiet.
					gctx.Tags["response_completed_sent"] = "true"
					_, _ = fmt.Fprintf(gctx.ResponseWriter, "data: [DONE]\n\n")
					if hasFlusher {
						flusher.Flush()
					}
					return nil
				}
			}
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read upstream stream: %w", err)
		}
	}

	// EOF without message_stop: response_completed_sent stays unset so the engine
	// flags the stream as prematurely closed and appends a failed event.
	return nil
}
