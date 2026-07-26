package providers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
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

type messageStartEvent struct {
	Type    string `json:"type"`
	Message struct {
		ID           string      `json:"id"`
		Type         string      `json:"type"`
		Role         string      `json:"role"`
		Content      []string    `json:"content"`
		Model        string      `json:"model"`
		StopReason   *string     `json:"stop_reason"`
		StopSequence interface{} `json:"stop_sequence"`
		Usage        struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

type contentBlockStartEvent struct {
	Type         string `json:"type"`
	Index        int    `json:"index"`
	ContentBlock struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content_block"`
}

type contentBlockDeltaEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	Delta struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"delta"`
}

type contentBlockStopEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
}

type messageDeltaEvent struct {
	Type  string `json:"type"`
	Delta struct {
		StopReason   string      `json:"stop_reason"`
		StopSequence interface{} `json:"stop_sequence"`
	} `json:"delta"`
	Usage struct {
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

type messageStopEvent struct {
	Type string `json:"type"`
}

func writeEvent(w io.Writer, eventType string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, string(jsonData))
	return err
}

// mapOpenAIFinishReason maps OpenAI chat.completion finish_reason to Anthropic stop_reason.
func mapOpenAIFinishReason(finishReason string) string {
	switch finishReason {
	case "length":
		return "max_tokens"
	case "tool_calls", "function_call":
		return "tool_use"
	case "content_filter":
		return "end_turn"
	case "stop", "":
		return "end_turn"
	default:
		return "end_turn"
	}
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

	parser := llm.NewSSEParser()
	buf := make([]byte, 4096)
	started := false
	firstRead := true
	sawDone := false
	finishReason := ""
	textChars := 0
	thinkingChars := 0
	hasToolUse := false

	var lastMessageID string

	activeBlocks := make(map[int]bool)
	oaiToAnthropicIndex := make(map[int]int)
	nextBlockIndex := 0
	thinkingBlockIndex := -1
	textBlockIndex := -1

	startMessage := func() error {
		if started {
			return nil
		}
		started = true
		msgID := translate.NormalizeAnthropicID(lastMessageID)
		respModel := gctx.OriginalModel
		if respModel == "" {
			respModel = gctx.Model
		}

		var startEv messageStartEvent
		startEv.Type = "message_start"
		startEv.Message.ID = msgID
		startEv.Message.Type = "message"
		startEv.Message.Role = "assistant"
		startEv.Message.Content = []string{}
		startEv.Message.Model = respModel
		startEv.Message.Usage.InputTokens = gctx.InputTokens
		startEv.Message.Usage.OutputTokens = gctx.OutputTokens

		if err := writeEvent(gctx.ResponseWriter, "message_start", startEv); err != nil {
			return err
		}
		flush()
		return nil
	}

	ensureBlock := func(blockType string) (int, error) {
		switch blockType {
		case "thinking":
			if thinkingBlockIndex >= 0 {
				return thinkingBlockIndex, nil
			}
		case "text":
			if textBlockIndex >= 0 {
				return textBlockIndex, nil
			}
		}

		idx := nextBlockIndex
		nextBlockIndex++
		activeBlocks[idx] = true

		switch blockType {
		case "thinking":
			thinkingBlockIndex = idx
			var blockStartEv struct {
				Type         string `json:"type"`
				Index        int    `json:"index"`
				ContentBlock struct {
					Type     string `json:"type"`
					Thinking string `json:"thinking"`
				} `json:"content_block"`
			}
			blockStartEv.Type = "content_block_start"
			blockStartEv.Index = idx
			blockStartEv.ContentBlock.Type = "thinking"
			blockStartEv.ContentBlock.Thinking = ""
			if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
				return 0, err
			}
		case "text":
			textBlockIndex = idx
			var blockStartEv contentBlockStartEvent
			blockStartEv.Type = "content_block_start"
			blockStartEv.Index = idx
			blockStartEv.ContentBlock.Type = "text"
			blockStartEv.ContentBlock.Text = ""
			if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("unsupported content block type: %s", blockType)
		}
		return idx, nil
	}

	emitTextDelta := func(idx int, text string) error {
		var deltaEv contentBlockDeltaEvent
		deltaEv.Type = "content_block_delta"
		deltaEv.Index = idx
		deltaEv.Delta.Type = "text_delta"
		deltaEv.Delta.Text = text
		if err := writeEvent(gctx.ResponseWriter, "content_block_delta", deltaEv); err != nil {
			return err
		}
		textChars += len(text)
		gctx.TransmittedChars += len(text)
		flush()
		return nil
	}

	emitThinkingDelta := func(idx int, text string) error {
		var deltaEv struct {
			Type  string `json:"type"`
			Index int    `json:"index"`
			Delta struct {
				Type     string `json:"type"`
				Thinking string `json:"thinking"`
			} `json:"delta"`
		}
		deltaEv.Type = "content_block_delta"
		deltaEv.Index = idx
		deltaEv.Delta.Type = "thinking_delta"
		deltaEv.Delta.Thinking = text
		if err := writeEvent(gctx.ResponseWriter, "content_block_delta", deltaEv); err != nil {
			return err
		}
		thinkingChars += len(text)
		gctx.TransmittedChars += len(text)
		flush()
		return nil
	}

	closeOpenBlocks := func() error {
		if len(activeBlocks) == 0 {
			return nil
		}
		var activeIndices []int
		for idx := range activeBlocks {
			activeIndices = append(activeIndices, idx)
		}
		sort.Ints(activeIndices)
		for _, idx := range activeIndices {
			var blockStopEv contentBlockStopEvent
			blockStopEv.Type = "content_block_stop"
			blockStopEv.Index = idx
			if err := writeEvent(gctx.ResponseWriter, "content_block_stop", blockStopEv); err != nil {
				return err
			}
		}
		activeBlocks = make(map[int]bool)
		return nil
	}

	finalizeSuccess := func(stopReason string) error {
		if !started {
			if err := startMessage(); err != nil {
				return err
			}
		}
		if len(activeBlocks) == 0 {
			// Anthropic clients expect at least one content block before stop.
			if _, err := ensureBlock("text"); err != nil {
				return err
			}
		}
		if err := closeOpenBlocks(); err != nil {
			return err
		}

		var msgDeltaEv messageDeltaEvent
		msgDeltaEv.Type = "message_delta"
		msgDeltaEv.Delta.StopReason = stopReason
		msgDeltaEv.Usage.OutputTokens = gctx.OutputTokens
		if err := writeEvent(gctx.ResponseWriter, "message_delta", msgDeltaEv); err != nil {
			return err
		}

		var stopEv messageStopEvent
		stopEv.Type = "message_stop"
		if err := writeEvent(gctx.ResponseWriter, "message_stop", stopEv); err != nil {
			return err
		}
		flush()
		markMessagesStreamCompleted(gctx)
		// Diagnostics only — never rewrite a legitimate upstream completion.
		setStreamDiagTag(gctx, "upstream_finish_reason", finishReason)
		setStreamDiagTag(gctx, "anthropic_stop_reason", stopReason)
		setStreamDiagTag(gctx, "stream_saw_done", strconv.FormatBool(sawDone))
		setStreamDiagTag(gctx, "transmitted_chars", strconv.Itoa(gctx.TransmittedChars))
		setStreamDiagTag(gctx, "text_chars", strconv.Itoa(textChars))
		setStreamDiagTag(gctx, "thinking_chars", strconv.Itoa(thinkingChars))
		if stopReason == "end_turn" && !hasToolUse && gctx.InputTokens >= 50000 && textChars < 100 {
			gctx.Logger(zap.L()).Info("messages stream short end_turn on large context",
				zap.String("finish_reason", finishReason),
				zap.String("stop_reason", stopReason),
				zap.Bool("saw_done", sawDone),
				zap.Int("input_tokens", gctx.InputTokens),
				zap.Int("output_tokens", gctx.OutputTokens),
				zap.Int("text_chars", textChars),
				zap.Int("thinking_chars", thinkingChars),
				zap.String("model", gctx.Model),
				zap.String("original_model", gctx.OriginalModel),
			)
		}
		return nil
	}

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

			// Trigger first byte
			gctx.TriggerFirstByte()

			events := parser.Feed(buf[:n])
			for _, ev := range events {
				if ev.Done {
					sawDone = true
					continue
				}

				// Extract tokens
				if ev.InputTokens > 0 {
					gctx.InputTokens = ev.InputTokens
				}
				if ev.OutputTokens > 0 {
					gctx.OutputTokens = ev.OutputTokens
				}

				// Parse OpenAI chunk
				var chunk struct {
					ID      string `json:"id"`
					Model   string `json:"model"`
					Choices []struct {
						Delta struct {
							Content          string `json:"content"`
							ReasoningContent string `json:"reasoning_content"`
							Thinking         string `json:"thinking"`
							Reasoning        string `json:"reasoning"`
							Thought          string `json:"thought"`
							ToolCalls        []struct {
								Index    int    `json:"index"`
								ID       string `json:"id"`
								Type     string `json:"type"`
								Function struct {
									Name      string `json:"name"`
									Arguments string `json:"arguments"`
								} `json:"function"`
							} `json:"tool_calls"`
						} `json:"delta"`
						FinishReason *string `json:"finish_reason"`
					} `json:"choices"`
				}

				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					continue
				}

				if chunk.ID != "" {
					lastMessageID = chunk.ID
				}

				if len(chunk.Choices) == 0 {
					continue
				}
				choice := chunk.Choices[0]

				// 1. Process reasoning/thinking as Anthropic thinking blocks.
				thinkingText := choice.Delta.ReasoningContent
				if thinkingText == "" {
					thinkingText = choice.Delta.Thinking
				}
				if thinkingText == "" {
					thinkingText = choice.Delta.Reasoning
				}
				if thinkingText == "" {
					thinkingText = choice.Delta.Thought
				}
				if thinkingText != "" {
					if err := startMessage(); err != nil {
						return err
					}
					idx, err := ensureBlock("thinking")
					if err != nil {
						return err
					}
					if err := emitThinkingDelta(idx, thinkingText); err != nil {
						return err
					}
				}

				// 2. Process standard assistant text.
				if choice.Delta.Content != "" {
					if err := startMessage(); err != nil {
						return err
					}
					idx, err := ensureBlock("text")
					if err != nil {
						return err
					}
					if err := emitTextDelta(idx, choice.Delta.Content); err != nil {
						return err
					}
				}

				// 3. Process tool calls
				if len(choice.Delta.ToolCalls) > 0 {
					if err := startMessage(); err != nil {
						return err
					}
					for _, tc := range choice.Delta.ToolCalls {
						anthropicIdx, mapped := oaiToAnthropicIndex[tc.Index]
						if !mapped {
							anthropicIdx = nextBlockIndex
							oaiToAnthropicIndex[tc.Index] = anthropicIdx
							nextBlockIndex++
						}

						if !activeBlocks[anthropicIdx] {
							activeBlocks[anthropicIdx] = true
							hasToolUse = true

							var blockStartEv struct {
								Type         string `json:"type"`
								Index        int    `json:"index"`
								ContentBlock struct {
									Type  string                 `json:"type"`
									ID    string                 `json:"id"`
									Name  string                 `json:"name"`
									Input map[string]interface{} `json:"input"`
								} `json:"content_block"`
							}
							blockStartEv.Type = "content_block_start"
							blockStartEv.Index = anthropicIdx
							blockStartEv.ContentBlock.Type = "tool_use"
							blockStartEv.ContentBlock.ID = translate.NormalizeToolUseID(tc.ID)
							blockStartEv.ContentBlock.Name = tc.Function.Name
							blockStartEv.ContentBlock.Input = make(map[string]interface{})

							if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
								return err
							}
						} else {
							hasToolUse = true
						}

						if tc.Function.Arguments != "" {
							var deltaEv struct {
								Type  string `json:"type"`
								Index int    `json:"index"`
								Delta struct {
									Type        string `json:"type"`
									PartialJSON string `json:"partial_json"`
								} `json:"delta"`
							}
							deltaEv.Type = "content_block_delta"
							deltaEv.Index = anthropicIdx
							deltaEv.Delta.Type = "input_json_delta"
							deltaEv.Delta.PartialJSON = tc.Function.Arguments

							if err := writeEvent(gctx.ResponseWriter, "content_block_delta", deltaEv); err != nil {
								return err
							}
						}

						flush()
					}
				}

				if choice.FinishReason != nil && *choice.FinishReason != "" {
					finishReason = *choice.FinishReason
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

	// Normal completion: OpenAI finish_reason and/or [DONE] sentinel.
	// Many OpenAI-compatible providers (incl. JoyCode/GLM) only send [DONE].
	if finishReason != "" || sawDone {
		return finalizeSuccess(mapOpenAIFinishReason(finishReason))
	}

	// Upstream closed the body without a completion signal. Do NOT forge end_turn —
	// that makes Claude Code treat a truncated stream as a successful reply.
	if started {
		_ = closeOpenBlocks()
		flush()
		return fmt.Errorf("upstream stream closed prematurely without completion event")
	}
	return fmt.Errorf("empty upstream stream: no content or completion signal received")
}
