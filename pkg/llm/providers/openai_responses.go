package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"

	"go.uber.org/zap"
)

type openaiResponsesInvoker struct{}

func (i *openaiResponsesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	gctx.Logger(zap.L()).Info("openaiResponsesInvoker Invoke entry called", zap.String("model", gctx.Model), zap.String("req_type", string(gctx.RequestType)))
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}

	// 1. Branch decision: does the endpoint natively support responses?
	hasResponseCapability := false
	if gctx.SelectedEndpoint != nil {
		for _, cap := range gctx.SelectedEndpoint.RequestTypes {
			if cap == core.RequestTypeResponses {
				hasResponseCapability = true
				break
			}
		}
	}

	// Branch A: native same-name forwarding.
	// Codex sends a client-side description on server-executed tool_search.
	// OpenAI rejects that field with 400 invalid_request_error; strip only it.
	if hasResponseCapability {
		if body, stripped, err := translate.StripToolSearchDescription(gctx.RawBody); err != nil {
			gctx.Logger(zap.L()).Warn("failed to strip tool_search description", zap.Error(err))
		} else if stripped {
			gctx.RawBody = body
			gctx.Logger(zap.L()).Debug("stripped tool_search description for native responses")
		}
		endpoint := op.baseURL + "/responses"
		return op.doRequest(gctx, endpoint)
	}

	// Branch B: protocol downgrade and translation (Responses -> Chat/Completions)
	newBody, toolMapper, err := translate.ResponsesRequestToChat(gctx.RawBody)
	if err != nil {
		return err
	}
	gctx.RawBody = newBody

	// Redirect to upstream /chat/completions
	endpoint := op.baseURL + "/chat/completions"
	if err := op.doRequest(gctx, endpoint); err != nil {
		return err
	}

	// Translate response body (OpenAI Chat -> Responses)
	if gctx.IsStream {
		return handleResponsesStream(gctx, gctx.UpstreamResponse, toolMapper)
	}
	if err := translateResponsesNonStreamResponse(gctx, toolMapper); err != nil {
		return fmt.Errorf("translate response: %w", err)
	}
	return nil
}

// Compatible with same-package callers (joycode / tests)
func translateResponsesToChatCompletion(rawBody []byte) ([]byte, error) {
	body, _, err := translate.ResponsesRequestToChat(rawBody)
	return body, err
}

func translateResponsesNonStreamResponse(gctx *core.GatewayContext, toolMapper *translate.ToolNameMapper) error {
	res, err := translate.ChatCompletionToResponses(gctx.UpstreamBody, gctx.Model, toolMapper)
	if err != nil {
		return err
	}
	gctx.UpstreamBody = res.Body
	var result map[string]interface{}
	if err := json.Unmarshal(res.Body, &result); err != nil {
		return err
	}
	gctx.Response = result
	llm.ApplyUsage(gctx, res.Usage.InputTokens, res.Usage.OutputTokens, 0, 0)
	return nil
}

func handleResponsesStream(gctx *core.GatewayContext, resp *http.Response, toolMapper *translate.ToolNameMapper) error {
	defer resp.Body.Close()

	defer func() {
		if r := recover(); r != nil {
			gctx.Logger(zap.L()).Error("[DEBUG-responses-stream] panic captured in handleResponsesStream",
				zap.Any("panic_info", r),
				zap.String("stack", string(debug.Stack())),
			)
			panic(r)
		}
	}()

	flusher, hasFlusher := gctx.ResponseWriter.(http.Flusher)

	parser := llm.NewSSEParser()
	buf := make([]byte, 4096)
	stream := translate.NewChatToResponsesStream(gctx.Model, toolMapper, translate.TokenUsage{
		InputTokens: gctx.InputTokens, OutputTokens: gctx.OutputTokens,
		CachedTokens: gctx.CachedTokens, CacheCreationTokens: gctx.CacheCreationTokens,
	})

	// Set once an upstream chat-completion frame carries a non-null finish_reason.
	// Some aggregated backends (e.g. JoyCode gen- pool) emit a spurious SSE error
	// event after the turn is already complete; that must not abort the translated
	// Responses stream.
	turnCompleted := false

	headersSent := false

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			// 1. Sniff SSE error events from all frames
			events := parser.Feed(buf[:n])
			for _, ev := range events {
				// Frames arrive in order: a finish_reason earlier in this batch must
				// already be visible when deciding on a later error frame.
				if !turnCompleted && strings.Contains(ev.Data, `"finish_reason"`) {
					var frChunk struct {
						Choices []struct {
							FinishReason *string `json:"finish_reason"`
						} `json:"choices"`
					}
					cleanData := strings.TrimSpace(ev.Data)
					if strings.HasPrefix(cleanData, "data:") {
						cleanData = strings.TrimSpace(strings.TrimPrefix(cleanData, "data:"))
					}
					if json.Unmarshal([]byte(cleanData), &frChunk) == nil {
						for _, c := range frChunk.Choices {
							if c.FinishReason != nil {
								turnCompleted = true
							}
						}
					}
				}

				if strings.Contains(ev.Data, `"error"`) {
					cleanData := strings.TrimSpace(ev.Data)
					if strings.HasPrefix(cleanData, "data:") {
						cleanData = strings.TrimSpace(strings.TrimPrefix(cleanData, "data:"))
					}

					var errChunk struct {
						Error *struct {
							Message string `json:"message"`
							Type    string `json:"type"`
							Code    any    `json:"code"`
							Cause   string `json:"cause"`
						} `json:"error"`
					}
					if json.Unmarshal([]byte(cleanData), &errChunk) == nil && errChunk.Error != nil && (errChunk.Error.Message != "" || errChunk.Error.Type != "") {
						errMsg := errChunk.Error.Message
						if errChunk.Error.Cause != "" {
							var innerErr struct {
								Error struct {
									Message string `json:"message"`
								} `json:"error"`
							}
							if json.Unmarshal([]byte(errChunk.Error.Cause), &innerErr) == nil && innerErr.Error.Message != "" {
								errMsg = fmt.Sprintf("%s (cause: %s)", errMsg, innerErr.Error.Message)
							} else {
								errMsg = fmt.Sprintf("%s (cause: %s)", errMsg, errChunk.Error.Cause)
							}
						}
						if turnCompleted {
							gctx.Logger(zap.L()).Warn("ignoring upstream SSE error received after stream completion",
								zap.String("response_id", gctx.GetTagValue("response_id")),
								zap.String("model", gctx.Model),
								zap.String("upstream_error", errMsg))
							continue
						}
						return fmt.Errorf("upstream stream returned error event: %s", errMsg)
					}
				}
			}

			if !headersSent {
				// 2. Sniff first frame raw data for plain JSON error
				trimmed := strings.TrimSpace(string(buf[:n]))
				if strings.HasPrefix(trimmed, "{") {
					var errJSON struct {
						Error struct {
							Message string `json:"message"`
							Type    string `json:"type"`
							Code    any    `json:"code"`
						} `json:"error"`
						Message string `json:"message"`
					}
					if jsonErr := json.Unmarshal([]byte(trimmed), &errJSON); jsonErr == nil {
						errMsg := errJSON.Error.Message
						if errMsg == "" {
							errMsg = errJSON.Message
						}
						if errMsg == "" {
							errMsg = trimmed
						}
						return fmt.Errorf("upstream returned JSON error: %s", errMsg)
					}
					return fmt.Errorf("upstream stream returned JSON error body: %s", trimmed)
				}

				// If headers haven't been sent yet, now is the best time to send them and trigger first-byte timing
				gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
				gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
				gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
				gctx.ResponseWriter.WriteHeader(http.StatusOK)
				gctx.TriggerFirstByte()
				headersSent = true
			}
			hasDone := false
			for _, ev := range events {
				if ev.Done {
					stream.FeedJSON(ev.Data)
					hasDone = true
					break
				}

				translated, meta := stream.FeedJSON(ev.Data)
				applyChatResponsesMeta(gctx, meta)
				for _, event := range translated {
					if err := writeSSEEvent(gctx.ResponseWriter, event.Event, event.Data); err != nil {
						return err
					}
					gctx.TransmittedChars += event.TransmittedChars
					if event.Flush && hasFlusher {
						flusher.Flush()
					}
				}
				if meta.Flush && hasFlusher {
					flusher.Flush()
				}

			}
			if hasDone {
				break
			}
		}

		if err != nil {
			if err == io.EOF {
				if !headersSent {
					return fmt.Errorf("upstream stream closed before sending any data (EOF)")
				}
				break
			}
			if errors.Is(err, context.Canceled) && gctx.Request != nil && gctx.Request.Context().Err() != nil {
				if turnCompleted {
					break
				}
				return fmt.Errorf("%w: %v", core.ErrClientDisconnected, err)
			}
			return fmt.Errorf("read upstream stream: %w", err)
		}
	}

	if !headersSent {
		gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
		gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
		gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
		gctx.ResponseWriter.WriteHeader(http.StatusOK)
		gctx.TriggerFirstByte()
	}

	// 输出 interceptor 可能改变 gctx usage；末尾必须以 caller-latest 有效值封装 completion。
	translated, meta := stream.Finish(translate.TokenUsage{
		InputTokens: gctx.InputTokens, OutputTokens: gctx.OutputTokens,
		CachedTokens: gctx.CachedTokens, CacheCreationTokens: gctx.CacheCreationTokens,
	})
	applyChatResponsesMeta(gctx, meta)
	for _, event := range translated {
		if err := writeSSEEvent(gctx.ResponseWriter, event.Event, event.Data); err != nil {
			return err
		}
	}
	if meta.Completed {
		if gctx.Tags == nil {
			gctx.Tags = make(map[string]string)
		}
		gctx.Tags["response_completed_sent"] = "true"
	}
	if meta.EmitDone {
		_ = writeSSEData(gctx.ResponseWriter, []byte("[DONE]"))
		if hasFlusher {
			flusher.Flush()
		}
	}

	return nil
}

// caller 写回 usage/tags，FSM 不依赖 GatewayContext。
func applyChatResponsesMeta(gctx *core.GatewayContext, meta translate.StreamChunkMeta) {
	llm.ApplyUsage(gctx, meta.InputTokens, meta.OutputTokens, meta.CachedTokens, meta.CacheCreationTokens)
	if meta.ResponseID != "" || meta.ResponseModel != "" {
		if gctx.Tags == nil {
			gctx.Tags = make(map[string]string)
		}
		if meta.ResponseID != "" {
			gctx.Tags["response_id"] = meta.ResponseID
		}
		if meta.ResponseModel != "" {
			gctx.Tags["response_model"] = meta.ResponseModel
		}
	}
}
