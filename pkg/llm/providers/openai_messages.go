package providers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
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

	// Detect Claude Code / Anthropic SDK connectivity probe requests.
	// Signature: max_tokens == 1, single message with content "."
	isProbe := false
	if maxTokens, ok := payload["max_tokens"].(float64); ok && maxTokens == 1 {
		if msgs, ok := payload["messages"].([]interface{}); ok && len(msgs) == 1 {
			if firstMsg, ok := msgs[0].(map[string]interface{}); ok {
				if content, ok := firstMsg["content"].(string); ok && content == "." {
					isProbe = true
				}
			}
		}
	}

	if isProbe {
		if gctx.Request != nil {
			if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
				gctx.ResponseWriter.Header().Set("anthropic-version", ver)
			} else {
				gctx.ResponseWriter.Header().Set("anthropic-version", "2023-06-01")
			}
		}
		respModel := gctx.OriginalModel
		if respModel == "" {
			respModel = gctx.Model
		}
		mockResp := map[string]interface{}{
			"id":    translate.NormalizeAnthropicID(fmt.Sprintf("probe%d", time.Now().UnixNano())),
			"type":  "message",
			"role":  "assistant",
			"model": respModel,
			"content": []interface{}{
				map[string]interface{}{
					"type": "text",
					"text": ".",
				},
			},
			"stop_reason":   "max_tokens",
			"stop_sequence": nil,
			"usage": map[string]interface{}{
				"input_tokens":  5,
				"output_tokens": 1,
			},
		}
		respBytes, err := json.Marshal(mockResp)
		if err != nil {
			return err
		}
		gctx.UpstreamBody = respBytes
		gctx.Response = mockResp
		gctx.InputTokens = 5
		gctx.OutputTokens = 1
		gctx.TriggerFirstByte()
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

func handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
	defer resp.Body.Close()

	gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
	gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
	gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
	if gctx.Request != nil {
		if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
			gctx.ResponseWriter.Header().Set("anthropic-version", ver)
		}
	}
	gctx.ResponseWriter.WriteHeader(http.StatusOK)

	flusher, hasFlusher := gctx.ResponseWriter.(http.Flusher)

	parser := llm.NewSSEParser()
	buf := make([]byte, 4096)
	started := false

	var lastMessageID string

	activeBlocks := make(map[int]bool)
	blockTypes := make(map[int]string) // index -> "text" or "tool_use"
	oaiToAnthropicIndex := make(map[int]int)
	nextBlockIndex := 0

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

		if hasFlusher {
			flusher.Flush()
		}
		return nil
	}

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			// Trigger first byte
			gctx.TriggerFirstByte()

			events := parser.Feed(buf[:n])
			for _, ev := range events {
				if ev.Done {
					break
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
							Content   string `json:"content"`
							ToolCalls []struct {
								Index    int    `json:"index"`
								ID       string `json:"id"`
								Type     string `json:"type"`
								Function struct {
									Name      string `json:"name"`
									Arguments string `json:"arguments"`
								} `json:"function"`
							} `json:"tool_calls"`
						} `json:"delta"`
					} `json:"choices"`
				}

				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					continue
				}

				if chunk.ID != "" {
					lastMessageID = chunk.ID
				}


				if len(chunk.Choices) > 0 {
					choice := chunk.Choices[0]

					// 1. Process text
					txt := choice.Delta.Content
					if txt != "" {
						if err := startMessage(); err != nil {
							return err
						}
						textIdx := 0 // text is always index 0 in Anthropic
						if !activeBlocks[textIdx] {
							activeBlocks[textIdx] = true
							blockTypes[textIdx] = "text"
							if nextBlockIndex == 0 {
								nextBlockIndex = 1
							}

							// Send content_block_start (text)
							var blockStartEv contentBlockStartEvent
							blockStartEv.Type = "content_block_start"
							blockStartEv.Index = textIdx
							blockStartEv.ContentBlock.Type = "text"
							blockStartEv.ContentBlock.Text = ""

							if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
								return err
							}
						}

						// Send content_block_delta
						var deltaEv contentBlockDeltaEvent
						deltaEv.Type = "content_block_delta"
						deltaEv.Index = textIdx
						deltaEv.Delta.Type = "text_delta"
						deltaEv.Delta.Text = txt

						if err := writeEvent(gctx.ResponseWriter, "content_block_delta", deltaEv); err != nil {
							return err
						}

						gctx.TransmittedChars += len(txt)

						if hasFlusher {
							flusher.Flush()
						}
					}

					// 2. Process tool calls
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
								blockTypes[anthropicIdx] = "tool_use"

								// Send content_block_start (tool_use)
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
							}

							// Send arguments delta
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

							if hasFlusher {
								flusher.Flush()
							}
						}
					}
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

	if !started {
		return fmt.Errorf("empty upstream stream: no content or tool calls received")
	}

	if started {
		// If no blocks were activated, send a fallback empty text content block before stopping
		if len(activeBlocks) == 0 {
			textIdx := 0
			activeBlocks[textIdx] = true
			blockTypes[textIdx] = "text"
			var blockStartEv contentBlockStartEvent
			blockStartEv.Type = "content_block_start"
			blockStartEv.Index = textIdx
			blockStartEv.ContentBlock.Type = "text"
			blockStartEv.ContentBlock.Text = ""
			if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
				return err
			}
		}

		// Send content_block_stop events in index order
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

		// Send message_stop
		var stopEv messageStopEvent
		stopEv.Type = "message_stop"

		if err := writeEvent(gctx.ResponseWriter, "message_stop", stopEv); err != nil {
			return err
		}

		if hasFlusher {
			flusher.Flush()
		}
	}

	return nil
}
