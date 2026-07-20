package providers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
)

type openaiMessagesInvoker struct{}

func (i *openaiMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}

	// 1. 翻译请求体 (Anthropic -> OpenAI)
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

	// 处理 system prompt (支持 string 与 []interface{})
	var systemPrompt string
	if sys, exists := payload["system"]; exists {
		if sysStr, ok := sys.(string); ok {
			systemPrompt = sysStr
		} else if sysArr, ok := sys.([]interface{}); ok {
			var parts []string
			for _, part := range sysArr {
				if partMap, ok := part.(map[string]interface{}); ok {
					if t, ok := partMap["type"].(string); ok && t == "text" {
						if txt, ok := partMap["text"].(string); ok {
							parts = append(parts, txt)
						}
					}
				}
			}
			systemPrompt = strings.Join(parts, "\n")
		}
	}

	var openAIMessages []interface{}
	if systemPrompt != "" {
		if !strings.Contains(op.baseURL, "api.openai.com") {
			systemPrompt = cleanSystemPrompt(systemPrompt)
		}
		openAIMessages = append(openAIMessages, map[string]interface{}{
			"role":    "system",
			"content": systemPrompt,
		})
	}

	// 翻译并合并 messages (Anthropic -> OpenAI)
	if msgs, ok := payload["messages"].([]interface{}); ok {
		for _, m := range msgs {
			mMap, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			role, _ := mMap["role"].(string)
			content := mMap["content"]

			if contentStr, ok := content.(string); ok {
				// 简单的字符串 content，直接保留
				openAIMessages = append(openAIMessages, map[string]interface{}{
					"role":    role,
					"content": contentStr,
				})
			} else if contentArr, ok := content.([]interface{}); ok {
				// 数组格式的 content，需要按 block 解析
				var textParts []string
				var toolCalls []interface{}
				var hasToolResult bool

				for _, block := range contentArr {
					blockMap, ok := block.(map[string]interface{})
					if !ok {
						continue
					}
					blockType, _ := blockMap["type"].(string)
					switch blockType {
					case "text":
						if txt, ok := blockMap["text"].(string); ok {
							textParts = append(textParts, txt)
						}
					case "tool_use":
						id, _ := blockMap["id"].(string)
						name, _ := blockMap["name"].(string)
						input := blockMap["input"]
						var arguments string
						if inputBytes, err := json.Marshal(input); err == nil {
							arguments = string(inputBytes)
						} else {
							arguments = "{}"
						}
						toolCalls = append(toolCalls, map[string]interface{}{
							"id":   id,
							"type": "function",
							"function": map[string]interface{}{
								"name":      name,
								"arguments": arguments,
							},
						})
					case "tool_result":
						hasToolResult = true
						toolUseID, _ := blockMap["tool_use_id"].(string)
						var resultContext string
						if resContent, ok := blockMap["content"]; ok {
							if resStr, ok := resContent.(string); ok {
								resultContext = resStr
							} else if resArr, ok := resContent.([]interface{}); ok {
								var innerText []string
								for _, part := range resArr {
									if partMap, ok := part.(map[string]interface{}); ok {
										if t, ok := partMap["type"].(string); ok && t == "text" {
											if txt, ok := partMap["text"].(string); ok {
												innerText = append(innerText, txt)
											}
										}
									}
								}
								resultContext = strings.Join(innerText, "\n")
							}
						}
						openAIMessages = append(openAIMessages, map[string]interface{}{
							"role":         "tool",
							"tool_call_id": toolUseID,
							"content":      resultContext,
						})
					}
				}

				// 如果有 tool_result，且伴随有普通文本，我们需要追加普通文本的 user 消息
				if hasToolResult {
					if len(textParts) > 0 {
						openAIMessages = append(openAIMessages, map[string]interface{}{
							"role":    role,
							"content": strings.Join(textParts, "\n"),
						})
					}
				} else {
					// 普通消息，将所有文本块拼装成字符串发给 OpenAI 保证完美兼容性
					mergedText := strings.Join(textParts, "\n")
					// 兜底 user 的 content 不能为空
					if role == "user" && mergedText == "" {
						mergedText = " "
					}

					msgObj := map[string]interface{}{
						"role":    role,
						"content": mergedText,
					}
					if len(toolCalls) > 0 {
						msgObj["tool_calls"] = toolCalls
					}
					openAIMessages = append(openAIMessages, msgObj)
				}
			}
		}
	}
	if !strings.Contains(op.baseURL, "api.openai.com") {
		openAIMessages = degradeMessagesToTextOnly(openAIMessages)
	}
	payload["messages"] = openAIMessages
	delete(payload, "system")

	// 翻译 stop_sequences -> stop
	if stops, ok := payload["stop_sequences"]; ok {
		payload["stop"] = stops
		delete(payload, "stop_sequences")
	}

	// 剔除第三方 OpenAI 兼容端点不支持的参数 (如 top_k、metadata、output_config)
	if !strings.Contains(op.baseURL, "api.openai.com") {
		delete(payload, "top_k")
		delete(payload, "metadata")
		delete(payload, "output_config")
	}

	// 映射 max_tokens 到 max_completion_tokens
	if maxTokens, ok := payload["max_tokens"]; ok {
		// 为了与各大第三方 OpenAI 兼容上游（如商汤 Sensenova, DeepSeek 等）保持最佳兼容性（它们通常不支持 max_completion_tokens 字段，只接受 max_tokens），
		// 我们只有在明确请求官方 OpenAI 或是本地测试时才进行新版字段转换
		isOfficialOrTest := strings.Contains(op.baseURL, "api.openai.com") ||
			strings.Contains(op.baseURL, "127.0.0.1") ||
			strings.Contains(op.baseURL, "localhost")
		if isOfficialOrTest {
			payload["max_completion_tokens"] = maxTokens
			delete(payload, "max_tokens")
		}
	}

	// 翻译 thinking (Anthropic -> OpenAI)
	if _, exists := payload["thinking"]; exists {
		isOfficialOrTest := strings.Contains(op.baseURL, "api.openai.com") ||
			strings.Contains(op.baseURL, "127.0.0.1") ||
			strings.Contains(op.baseURL, "localhost")
		if isOfficialOrTest {
			if thinking, ok := payload["thinking"].(map[string]interface{}); ok {
				if t, ok := thinking["type"].(string); ok {
					if t == "adaptive" {
						thinking["type"] = "auto"
					}
				}
			}
		} else {
			// 对于第三方兼容端点，由于不支持 thinking 字段，直接剔除以防 400 报错
			delete(payload, "thinking")
		}
	}


	// 翻译 tools (Anthropic -> OpenAI)
	if tools, ok := payload["tools"].([]interface{}); ok {
		var oaiTools []interface{}
		for _, t := range tools {
			tMap, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			name, _ := tMap["name"].(string)
			if name == "" {
				continue
			}
			fnMap := make(map[string]interface{})
			fnMap["name"] = name
			if desc, ok := tMap["description"].(string); ok {
				fnMap["description"] = desc
			}
			if inputSchema, ok := tMap["input_schema"]; ok {
				if isM, ok := inputSchema.(map[string]interface{}); ok {
					fnMap["parameters"] = cleanJSONSchema(isM, !strings.Contains(op.baseURL, "api.openai.com"))
				} else {
					fnMap["parameters"] = inputSchema
				}
			} else {
				fnMap["parameters"] = map[string]interface{}{
					"type":       "object",
					"properties": map[string]interface{}{},
				}
			}
			oaiTools = append(oaiTools, map[string]interface{}{
				"type":     "function",
				"function": fnMap,
			})
		}
		if len(oaiTools) > 0 {
			// 第三方 OpenAI 兼容端点通常无法在单次请求中高效解析海量工具（如 50+ 个），强行发送会导致上游 Schema 校验超时或返回 400 错误。
			// 因此我们对非官方端点限制 tools 的最大数量为 32 个以确保可用性与稳定性。
			if !strings.Contains(op.baseURL, "api.openai.com") && len(oaiTools) > 32 {
				oaiTools = oaiTools[:32]
			}
			payload["tools"] = oaiTools
		} else {
			delete(payload, "tools")
		}
	}

	// 翻译 tool_choice (Anthropic -> OpenAI)
	if tc, ok := payload["tool_choice"].(map[string]interface{}); ok {
		tcType, _ := tc["type"].(string)
		if tcType == "auto" {
			payload["tool_choice"] = "auto"
		} else if tcType == "any" {
			payload["tool_choice"] = "required"
		} else if tcType == "tool" {
			toolName, _ := tc["name"].(string)
			payload["tool_choice"] = map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name": toolName,
				},
			}
		}
	}

	newBody, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal translated body: %w", err)
	}
	gctx.RawBody = newBody

	// 2. 覆盖请求 URL，同名重定向到上游 Provider 的 /chat/completions 端点，执行请求
	endpoint := op.baseURL + "/chat/completions"
	if err := op.doRequest(gctx, endpoint); err != nil {
		return err
	}

	// 3. 翻译响应体 (OpenAI -> Anthropic)
	if gctx.IsStream {
		return handleMessagesStream(gctx, gctx.UpstreamResponse)
	} else {
		if err := translateNonStreamResponse(gctx); err != nil {
			return fmt.Errorf("translate response: %w", err)
		}
	}

	return nil
}

func translateNonStreamResponse(gctx *core.GatewayContext) error {
	var oaiResp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Role      string `json:"role"`
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(gctx.UpstreamBody, &oaiResp); err != nil {
		return err
	}

	var anthropicContent []interface{}
	if len(oaiResp.Choices) > 0 {
		msg := oaiResp.Choices[0].Message
		if msg.Content != "" {
			anthropicContent = append(anthropicContent, map[string]interface{}{
				"type": "text",
				"text": msg.Content,
			})
		}
		for _, tc := range msg.ToolCalls {
			var input map[string]interface{}
			if tc.Function.Arguments != "" {
				_ = json.Unmarshal([]byte(tc.Function.Arguments), &input)
			}
			if input == nil {
				input = make(map[string]interface{})
			}
			anthropicContent = append(anthropicContent, map[string]interface{}{
				"type":  "tool_use",
				"id":    normalizeToolUseID(tc.ID),
				"name":  tc.Function.Name,
				"input": input,
			})
		}
	}

	if len(anthropicContent) == 0 {
		anthropicContent = append(anthropicContent, map[string]interface{}{
			"type": "text",
			"text": "",
		})
	}

	stopReason := "end_turn"
	if len(oaiResp.Choices) > 0 {
		fr := oaiResp.Choices[0].FinishReason
		if fr == "length" {
			stopReason = "max_tokens"
		} else if fr == "tool_calls" {
			stopReason = "tool_use"
		}
	}

	msgID := normalizeAnthropicID(oaiResp.ID)

	respModel := gctx.OriginalModel
	if respModel == "" {
		respModel = gctx.Model
	}

	anthropicResp := map[string]interface{}{
		"id":            msgID,
		"type":          "message",
		"role":          "assistant",
		"model":         respModel,
		"content":       anthropicContent,
		"stop_reason":   stopReason,
		"stop_sequence": nil,
		"usage": map[string]int{
			"input_tokens":  oaiResp.Usage.PromptTokens,
			"output_tokens": oaiResp.Usage.CompletionTokens,
		},
	}

	if gctx.Request != nil {
		if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
			gctx.ResponseWriter.Header().Set("anthropic-version", ver)
		}
	}

	translatedBody, err := json.Marshal(anthropicResp)
	if err != nil {
		return err
	}

	gctx.UpstreamBody = translatedBody

	// 同时将 Response 属性也更新为翻译后的 map[string]interface{}
	var result map[string]interface{}
	if err := json.Unmarshal(translatedBody, &result); err != nil {
		return err
	}
	gctx.Response = result

	// 记录 Token 统计信息，保证结算与日志能提取到
	gctx.InputTokens = oaiResp.Usage.PromptTokens
	gctx.OutputTokens = oaiResp.Usage.CompletionTokens

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
		msgID := normalizeAnthropicID(lastMessageID)
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
			// 触发首字节返回
			gctx.TriggerFirstByte()

			events := parser.Feed(buf[:n])
			for _, ev := range events {
				if ev.Done {
					break
				}

				// 提取 token
				if ev.InputTokens > 0 {
					gctx.InputTokens = ev.InputTokens
				}
				if ev.OutputTokens > 0 {
					gctx.OutputTokens = ev.OutputTokens
				}

				// 解析 OpenAI Chunk
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

					// 1. 处理文本
					txt := choice.Delta.Content
					if txt != "" {
						if err := startMessage(); err != nil {
							return err
						}
						textIdx := 0 // 文本固定为 Anthropic 里的 index 0
						if !activeBlocks[textIdx] {
							activeBlocks[textIdx] = true
							blockTypes[textIdx] = "text"
							if nextBlockIndex == 0 {
								nextBlockIndex = 1
							}

							// 发送 content_block_start (text)
							var blockStartEv contentBlockStartEvent
							blockStartEv.Type = "content_block_start"
							blockStartEv.Index = textIdx
							blockStartEv.ContentBlock.Type = "text"
							blockStartEv.ContentBlock.Text = ""

							if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
								return err
							}
						}

						// 发送 content_block_delta
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

					// 2. 处理工具调用
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

								// 发送 content_block_start (tool_use)
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
								blockStartEv.ContentBlock.ID = normalizeToolUseID(tc.ID)
								blockStartEv.ContentBlock.Name = tc.Function.Name
								blockStartEv.ContentBlock.Input = make(map[string]interface{})

								if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
									return err
								}
							}

							// 发送 arguments delta
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
		// 如果没有任何 block 激活发送过，则在停止前，兜底发送一个空文本 content block
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

		// 按照 index 顺序发送 content_block_stop
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

		// 发送 message_stop
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

func normalizeAnthropicID(id string) string {
	if id == "" {
		return "msg_mockprobe1234567890"
	}
	orig := id
	if strings.HasPrefix(orig, "chatcmpl-") {
		orig = orig[9:]
	} else if strings.HasPrefix(orig, "msg_") {
		orig = orig[4:]
	}
	var sb strings.Builder
	sb.WriteString("msg_")
	for _, ch := range orig {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			sb.WriteRune(ch)
		}
	}
	res := sb.String()
	if len(res) <= 4 {
		return "msg_mockprobe1234567890"
	}
	return res
}

func cleanJSONSchema(m map[string]interface{}, removeAdditionalProps bool) map[string]interface{} {
	if m == nil {
		return m
	}
	res := make(map[string]interface{})
	for k, v := range m {
		// 剔除第三方端点可能无法解析并导致 400 报错的高级/冷门 JSON Schema 元属性
		if k == "$schema" || k == "propertyNames" || k == "minItems" || k == "maxItems" || 
		   k == "minLength" || k == "maxLength" || k == "default" || k == "pattern" {
			continue
		}
		if k == "additionalProperties" && removeAdditionalProps {
			continue
		}
		if subMap, ok := v.(map[string]interface{}); ok {
			res[k] = cleanJSONSchema(subMap, removeAdditionalProps)
		} else if subArr, ok := v.([]interface{}); ok {
			var newArr []interface{}
			for _, item := range subArr {
				if itemMap, ok := item.(map[string]interface{}); ok {
					newArr = append(newArr, cleanJSONSchema(itemMap, removeAdditionalProps))
				} else {
					newArr = append(newArr, item)
				}
			}
			res[k] = newArr
		} else {
			res[k] = v
		}
	}
	return res
}

func degradeMessagesToTextOnly(msgs []interface{}) []interface{} {
	var temp []interface{}
	for _, m := range msgs {
		mMap, ok := m.(map[string]interface{})
		if !ok {
			temp = append(temp, m)
			continue
		}
		role, _ := mMap["role"].(string)
		if role == "system" && len(temp) > 0 {
			// 非最开头消息，将 system 角色退化为 user 角色，防止上游报错 InvalidParameter
			role = "user"
			mMap["role"] = "user"
		}
		if role == "tool" {
			toolCallID, _ := mMap["tool_call_id"].(string)
			content, _ := mMap["content"].(string)
			temp = append(temp, map[string]interface{}{
				"role":    "user",
				"content": fmt.Sprintf("<historical_tool_result id=\"%s\">\n%s\n</historical_tool_result>", toolCallID, content),
			})
		} else if role == "assistant" {
			content, _ := mMap["content"].(string)
			temp = append(temp, map[string]interface{}{
				"role":    "assistant",
				"content": content,
			})
		} else {
			temp = append(temp, mMap)
		}
	}

	// 合并相邻角色相同的消息
	var res []interface{}
	for _, m := range temp {
		mMap, ok := m.(map[string]interface{})
		if !ok {
			res = append(res, m)
			continue
		}
		if len(res) == 0 {
			res = append(res, mMap)
			continue
		}
		lastMap, ok := res[len(res)-1].(map[string]interface{})
		if !ok {
			res = append(res, mMap)
			continue
		}
		if lastMap["role"] == mMap["role"] {
			lastContent, _ := lastMap["content"].(string)
			thisContent, _ := mMap["content"].(string)
			lastMap["content"] = lastContent + "\n\n" + thisContent
		} else {
			res = append(res, mMap)
		}
	}
	return res
}

func cleanSystemPrompt(prompt string) string {
	lines := strings.Split(prompt, "\n")
	var cleanLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "x-anthropic-") {
			continue
		}
		cleanLines = append(cleanLines, line)
	}
	return strings.Join(cleanLines, "\n")
}

func normalizeToolUseID(id string) string {
	if id == "" {
		return "toolu_mock"
	}
	if strings.HasPrefix(id, "toolu_") {
		return id
	}
	res := strings.TrimPrefix(id, "call_")
	res = strings.TrimPrefix(res, "toolu-")
	return "toolu_" + res
}
