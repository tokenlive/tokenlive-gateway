package providers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
	"go.uber.org/zap"
)

type openaiMessagesInvoker struct{}

func (i *openaiMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}

	// 1. Translate request body (Anthropic -> OpenAI)
	var payload map[string]interface{}
	if err := json.Unmarshal(gctx.RawBody, &payload); err != nil {
		return fmt.Errorf("parse raw body: %w", err)
	}

	mocked, err := llm.TryMockMessagesProbe(gctx)
	if err != nil {
		return err
	}
	if mocked {
		return nil
	}

	// 1b. Protocol translation: Anthropic Messages -> OpenAI Chat (pure function)
	newBody, err := translate.MessagesRequestToChat(gctx.RawBody, translate.MessagesToChatOptions{
		OfficialOrTest: translate.IsOfficialOrTestBaseURL(op.baseURL),
	})
	if err != nil {
		return err
	}
	gctx.RawBody = newBody

	// 2. Override request URL — redirect to upstream provider's /chat/completions endpoint and execute
	endpoint := op.baseURL + "/chat/completions"
	if err := op.doRequest(gctx, endpoint); err != nil {
		return err
	}

	// 3. Translate response body (OpenAI -> Anthropic)
	if gctx.IsStream {
		return handleMessagesStream(gctx, gctx.UpstreamResponse)
	}
	if err := translateNonStreamResponse(gctx); err != nil {
		return fmt.Errorf("translate response: %w", err)
	}
	return nil
}

func translateNonStreamResponse(gctx *core.GatewayContext) error {
	respModel := gctx.OriginalModel
	if respModel == "" {
		respModel = gctx.Model
	}

	res, err := translate.ChatCompletionToMessages(gctx.UpstreamBody, respModel)
	if err != nil {
		return err
	}

	if gctx.Request != nil {
		if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
			gctx.ResponseWriter.Header().Set("anthropic-version", ver)
		}
	}

	gctx.UpstreamBody = res.Body
	var result map[string]interface{}
	if err := json.Unmarshal(res.Body, &result); err != nil {
		return err
	}
	gctx.Response = result
	gctx.InputTokens = res.Usage.InputTokens
	gctx.OutputTokens = res.Usage.OutputTokens
	return nil
}

func markMessagesStreamCompleted(gctx *core.GatewayContext) {
	if gctx.Tags == nil {
		gctx.Tags = make(map[string]string)
	}
	gctx.Tags["message_stop_sent"] = "true"
}

func setStreamDiagTag(gctx *core.GatewayContext, key, value string) {
	if gctx == nil || value == "" {
		return
	}
	if gctx.Tags == nil {
		gctx.Tags = make(map[string]string)
	}
	gctx.Tags[key] = value
}

func handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		gctx.UpstreamBody = body
		gctx.ResponseWriter.Header().Set("Content-Type", "application/json; charset=utf-8")
		if gctx.Request != nil {
			if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
				gctx.ResponseWriter.Header().Set("anthropic-version", ver)
			}
		}
		gctx.ResponseWriter.WriteHeader(resp.StatusCode)
		errResp := map[string]interface{}{
			"type": "error",
			"error": map[string]interface{}{
				"type":    "api_error",
				"message": fmt.Sprintf("Upstream API returned status %d: %s", resp.StatusCode, string(body)),
			},
		}
		jsonErr, _ := json.Marshal(errResp)
		_, _ = gctx.ResponseWriter.Write(jsonErr)
		return fmt.Errorf("upstream returned status %d: %s", resp.StatusCode, string(body))
	}

	gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
	gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
	gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
	gctx.ResponseWriter.Header().Set("X-Accel-Buffering", "no")
	if gctx.Request != nil {
		if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
			gctx.ResponseWriter.Header().Set("anthropic-version", ver)
		}
	}
	gctx.ResponseWriter.WriteHeader(http.StatusOK)

	flusher, hasFlusher := gctx.ResponseWriter.(http.Flusher)
	flush := func() {
		if hasFlusher {
			flusher.Flush()
		}
	}
	writeEvents := func(events []translate.MessagesStreamEvent, flushEvents bool) error {
		for _, event := range events {
			if err := writeSSEEvent(gctx.ResponseWriter, event.Event, event.Data); err != nil {
				return err
			}
			gctx.TransmittedChars += event.TransmittedChars
			if flushEvents && event.Flush {
				flush()
			}
		}
		return nil
	}

	respModel := gctx.OriginalModel
	if respModel == "" {
		respModel = gctx.Model
	}
	stream := translate.NewChatToMessagesStream(respModel, translate.TokenUsage{
		InputTokens: gctx.InputTokens, OutputTokens: gctx.OutputTokens,
		CachedTokens: gctx.CachedTokens, CacheCreationTokens: gctx.CacheCreationTokens,
	})
	parser := llm.NewSSEParser()
	buf := make([]byte, 4096)
	firstRead := true

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if firstRead {
				firstRead = false
				trimmed := bytes.TrimSpace(buf[:n])
				if bytes.HasPrefix(trimmed, []byte("<!DOCTYPE")) || bytes.HasPrefix(trimmed, []byte("<html")) || bytes.HasPrefix(trimmed, []byte("<HTML")) {
					return fmt.Errorf("upstream returned HTML error response instead of SSE stream")
				}
			}

			gctx.TriggerFirstByte()
			for _, ev := range parser.Feed(buf[:n]) {
				data := ev.Data
				if ev.Done {
					data = "[DONE]"
				}
				events, meta := stream.FeedJSON(data)
				// 保留 Messages 原有 input/output 正数覆盖规则，不扩展缓存控制。
				if meta.InputTokens > 0 {
					gctx.InputTokens = meta.InputTokens
				}
				if meta.OutputTokens > 0 {
					gctx.OutputTokens = meta.OutputTokens
				}
				if err := writeEvents(events, true); err != nil {
					return err
				}
				if meta.Flush {
					flush()
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

	// 完成信号不提前终止读取：EOF 时使用 caller 最新有效 usage 生成 terminal。
	events, meta := stream.Finish(translate.TokenUsage{
		InputTokens: gctx.InputTokens, OutputTokens: gctx.OutputTokens,
		CachedTokens: gctx.CachedTokens, CacheCreationTokens: gctx.CacheCreationTokens,
	})
	if meta.ErrorMessage != "" {
		if len(events) > 0 {
			// partial EOF 的 block close 写错误保持忽略，并保留末尾 flush。
			_ = writeEvents(events, false)
			flush()
		}
		return fmt.Errorf("%s", meta.ErrorMessage)
	}
	if err := writeEvents(events, true); err != nil {
		return err
	}
	if meta.Completed {
		markMessagesStreamCompleted(gctx)
		// 仅作诊断，不改写合法的 upstream completion。
		setStreamDiagTag(gctx, "upstream_finish_reason", meta.FinishReason)
		setStreamDiagTag(gctx, "anthropic_stop_reason", meta.StopReason)
		setStreamDiagTag(gctx, "stream_saw_done", strconv.FormatBool(meta.SawDone))
		setStreamDiagTag(gctx, "transmitted_chars", strconv.Itoa(gctx.TransmittedChars))
		setStreamDiagTag(gctx, "text_chars", strconv.Itoa(meta.TextChars))
		setStreamDiagTag(gctx, "thinking_chars", strconv.Itoa(meta.ThinkingChars))
		if meta.StopReason == "end_turn" && !meta.HasToolUse && gctx.InputTokens >= 50000 && meta.TextChars < 100 {
			gctx.Logger(zap.L()).Info("messages stream short end_turn on large context",
				zap.String("finish_reason", meta.FinishReason),
				zap.String("stop_reason", meta.StopReason),
				zap.Bool("saw_done", meta.SawDone),
				zap.Int("input_tokens", gctx.InputTokens),
				zap.Int("output_tokens", gctx.OutputTokens),
				zap.Int("text_chars", meta.TextChars),
				zap.Int("thinking_chars", meta.ThinkingChars),
				zap.String("model", gctx.Model),
				zap.String("original_model", gctx.OriginalModel),
			)
		}
	}
	return nil
}
