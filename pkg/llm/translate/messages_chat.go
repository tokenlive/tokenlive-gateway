package translate

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// MessagesToChatOptions controls Messages→Chat request translation.
type MessagesToChatOptions struct {
	// OfficialOrTest true: official OpenAI field mapping (max_completion_tokens, thinking, …).
	// false: third-party compat (strip thinking/top_k, tools cap 32, schema sanitize).
	OfficialOrTest bool
}

// TokenUsage is token stats from translation (does not touch GatewayContext).
type TokenUsage struct {
	InputTokens         int
	OutputTokens        int
	CachedTokens        int
	CacheCreationTokens int
}

// ChatCompletionToMessagesResult is non-stream Chat→Messages result.
type ChatCompletionToMessagesResult struct {
	Body  []byte
	Usage TokenUsage
}

// MessagesToChatCompletionResult is non-stream Messages→Chat result.
type MessagesToChatCompletionResult struct {
	Body  []byte
	Usage TokenUsage
}

// MessagesRequestToChat translates Anthropic Messages request to OpenAI Chat.
func MessagesRequestToChat(rawBody []byte, opts MessagesToChatOptions) ([]byte, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("parse raw body: %w", err)
	}

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
		if !opts.OfficialOrTest {
			systemPrompt = cleanSystemPrompt(systemPrompt)
		}
		openAIMessages = append(openAIMessages, map[string]interface{}{
			"role":    "system",
			"content": systemPrompt,
		})
	}

	if msgs, ok := payload["messages"].([]interface{}); ok {
		for _, m := range msgs {
			mMap, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			role, _ := mMap["role"].(string)
			content := mMap["content"]

			if contentStr, ok := content.(string); ok {
				openAIMessages = append(openAIMessages, map[string]interface{}{
					"role":    role,
					"content": contentStr,
				})
			} else if contentArr, ok := content.([]interface{}); ok {
				var textParts []string
				var toolCalls []interface{}
				var imageParts []interface{}
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
					case "image":
						if imgURL := anthropicImageToOpenAIURL(blockMap); imgURL != "" {
							imageParts = append(imageParts, map[string]interface{}{
								"type": "image_url",
								"image_url": map[string]interface{}{
									"url": imgURL,
								},
							})
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

				if hasToolResult {
					if len(textParts) > 0 || len(imageParts) > 0 {
						openAIMessages = append(openAIMessages, buildOpenAIContentMessage(role, textParts, imageParts))
					}
				} else {
					mergedText := strings.Join(textParts, "\n")
					if len(imageParts) > 0 {
						msgObj := buildOpenAIContentMessage(role, textParts, imageParts)
						if len(toolCalls) > 0 {
							msgObj["tool_calls"] = toolCalls
						}
						openAIMessages = append(openAIMessages, msgObj)
					} else {
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
	}
	if !opts.OfficialOrTest {
		openAIMessages = degradeMessagesToTextOnly(openAIMessages)
	}
	payload["messages"] = openAIMessages
	delete(payload, "system")

	if stops, ok := payload["stop_sequences"]; ok {
		payload["stop"] = stops
		delete(payload, "stop_sequences")
	}

	if !opts.OfficialOrTest {
		delete(payload, "top_k")
		delete(payload, "metadata")
		delete(payload, "output_config")
	}

	if maxTokens, ok := payload["max_tokens"]; ok {
		if opts.OfficialOrTest {
			payload["max_completion_tokens"] = maxTokens
			delete(payload, "max_tokens")
		}
	}

	if _, exists := payload["thinking"]; exists {
		if opts.OfficialOrTest {
			if thinking, ok := payload["thinking"].(map[string]interface{}); ok {
				if t, ok := thinking["type"].(string); ok {
					if t == "adaptive" {
						thinking["type"] = "auto"
					}
				}
			}
		} else {
			delete(payload, "thinking")
		}
	}

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
					fnMap["parameters"] = cleanJSONSchema(isM, !opts.OfficialOrTest)
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
			if !opts.OfficialOrTest && len(oaiTools) > 32 {
				oaiTools = oaiTools[:32]
			}
			payload["tools"] = oaiTools
		} else {
			delete(payload, "tools")
		}
	}

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

	return json.Marshal(payload)
}

// ChatRequestToMessages translates OpenAI Chat request to Anthropic Messages.
func ChatRequestToMessages(rawBody []byte, model string) ([]byte, error) {
	var oaiReq map[string]interface{}
	if err := json.Unmarshal(rawBody, &oaiReq); err != nil {
		return nil, err
	}

	anthropicReq := make(map[string]interface{})
	anthropicReq["model"] = model

	var systemPrompts []string
	var anthropicMsgs []map[string]interface{}

	if msgs, exists := oaiReq["messages"].([]interface{}); exists {
		for _, m := range msgs {
			mMap, ok := m.(map[string]interface{})
			if !ok {
				continue
			}
			role, _ := mMap["role"].(string)
			content := mMap["content"]

			if role == "system" {
				if contentStr, ok := content.(string); ok && contentStr != "" {
					systemPrompts = append(systemPrompts, contentStr)
				}
			} else {
				newMsg := make(map[string]interface{})
				newMsg["role"] = role

				if role == "tool" {
					newMsg["role"] = "user"
					toolCallID, _ := mMap["tool_call_id"].(string)
					contentStr, _ := content.(string)

					toolResultBlock := map[string]interface{}{
						"type":        "tool_result",
						"tool_use_id": toolCallID,
						"content":     contentStr,
					}

					if len(anthropicMsgs) > 0 && anthropicMsgs[len(anthropicMsgs)-1]["role"] == "user" {
						lastMsg := anthropicMsgs[len(anthropicMsgs)-1]
						if contentArr, ok := lastMsg["content"].([]interface{}); ok {
							lastMsg["content"] = append(contentArr, toolResultBlock)
							continue
						}
					}
					newMsg["content"] = []interface{}{toolResultBlock}

				} else if role == "assistant" {
					var contentBlocks []interface{}
					if contentStr, ok := content.(string); ok && contentStr != "" {
						contentBlocks = append(contentBlocks, map[string]interface{}{
							"type": "text",
							"text": contentStr,
						})
					}
					if tcs, hasTcs := mMap["tool_calls"].([]interface{}); hasTcs {
						for _, tc := range tcs {
							tcMap, ok := tc.(map[string]interface{})
							if !ok {
								continue
							}
							id, _ := tcMap["id"].(string)
							funcMap, _ := tcMap["function"].(map[string]interface{})
							name, _ := funcMap["name"].(string)
							argsStr, _ := funcMap["arguments"].(string)

							var input interface{}
							if argsStr != "" {
								_ = json.Unmarshal([]byte(argsStr), &input)
							}
							if input == nil {
								input = map[string]interface{}{}
							}

							contentBlocks = append(contentBlocks, map[string]interface{}{
								"type":  "tool_use",
								"id":    id,
								"name":  name,
								"input": input,
							})
						}
					}
					newMsg["content"] = contentBlocks
				} else if role == "user" {
					if contentArr, ok := content.([]interface{}); ok {
						var contentBlocks []interface{}
						for _, block := range contentArr {
							blockMap, ok := block.(map[string]interface{})
							if !ok {
								continue
							}
							t, _ := blockMap["type"].(string)
							if t == "image_url" {
								iu, _ := blockMap["image_url"].(map[string]interface{})
								urlStr, _ := iu["url"].(string)
								if strings.HasPrefix(urlStr, "data:") {
									stripped := strings.TrimPrefix(urlStr, "data:")
									if parts := strings.Split(stripped, ";base64,"); len(parts) == 2 {
										contentBlocks = append(contentBlocks, map[string]interface{}{
											"type": "image",
											"source": map[string]interface{}{
												"type":       "base64",
												"media_type": parts[0],
												"data":       parts[1],
											},
										})
										continue
									}
								}
							}
							contentBlocks = append(contentBlocks, block)
						}
						newMsg["content"] = contentBlocks
					} else {
						newMsg["content"] = content
					}
				} else {
					newMsg["content"] = content
				}

				anthropicMsgs = append(anthropicMsgs, newMsg)
			}
		}
	}

	if len(systemPrompts) > 0 {
		anthropicReq["system"] = strings.Join(systemPrompts, "\n")
	}
	anthropicReq["messages"] = anthropicMsgs

	if oaiTools, exists := oaiReq["tools"].([]interface{}); exists {
		var anthropicTools []interface{}
		for _, tool := range oaiTools {
			toolMap, ok := tool.(map[string]interface{})
			if !ok {
				continue
			}
			t, _ := toolMap["type"].(string)
			if t == "function" {
				funcMap, _ := toolMap["function"].(map[string]interface{})
				name, _ := funcMap["name"].(string)
				desc, _ := funcMap["description"].(string)
				params := funcMap["parameters"]
				if params == nil {
					params = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
				}
				anthropicTools = append(anthropicTools, map[string]interface{}{
					"name":         name,
					"description":  desc,
					"input_schema": params,
				})
			}
		}
		if len(anthropicTools) > 0 {
			anthropicReq["tools"] = anthropicTools
		}
	}

	// Reverse-map tool_choice
	if tc, exists := oaiReq["tool_choice"]; exists {
		switch v := tc.(type) {
		case string:
			if v == "auto" {
				anthropicReq["tool_choice"] = map[string]interface{}{"type": "auto"}
			} else if v == "required" {
				anthropicReq["tool_choice"] = map[string]interface{}{"type": "any"}
			} else if v == "none" {
				// Anthropic has no none; omit
			}
		case map[string]interface{}:
			if fn, ok := v["function"].(map[string]interface{}); ok {
				if name, ok := fn["name"].(string); ok && name != "" {
					anthropicReq["tool_choice"] = map[string]interface{}{
						"type": "tool",
						"name": name,
					}
				}
			}
		}
	}

	if stops, ok := oaiReq["stop"]; ok {
		anthropicReq["stop_sequences"] = stops
	}

	maxTokens := 4000
	if mt, exists := oaiReq["max_tokens"].(float64); exists {
		maxTokens = int(mt)
	} else if mct, exists := oaiReq["max_completion_tokens"].(float64); exists {
		maxTokens = int(mct)
	}
	anthropicReq["max_tokens"] = maxTokens

	for _, key := range []string{"temperature", "top_p", "stream"} {
		if val, exists := oaiReq[key]; exists {
			anthropicReq[key] = val
		}
	}

	return json.Marshal(anthropicReq)
}

// ChatCompletionToMessages translates non-stream Chat response to Messages.
func ChatCompletionToMessages(chatBody []byte, model string) (ChatCompletionToMessagesResult, error) {
	var oaiResp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Role             string `json:"role"`
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				ToolCalls        []struct {
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

	if err := json.Unmarshal(chatBody, &oaiResp); err != nil {
		return ChatCompletionToMessagesResult{}, err
	}

	var anthropicContent []interface{}
	if len(oaiResp.Choices) > 0 {
		msg := oaiResp.Choices[0].Message
		if msg.ReasoningContent != "" {
			anthropicContent = append(anthropicContent, map[string]interface{}{
				"type":     "thinking",
				"thinking": msg.ReasoningContent,
			})
		}
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
				"id":    NormalizeToolUseID(tc.ID),
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

	respModel := model
	if respModel == "" {
		respModel = oaiResp.Model
	}

	anthropicResp := map[string]interface{}{
		"id":            NormalizeAnthropicID(oaiResp.ID),
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

	body, err := json.Marshal(anthropicResp)
	if err != nil {
		return ChatCompletionToMessagesResult{}, err
	}

	return ChatCompletionToMessagesResult{
		Body: body,
		Usage: TokenUsage{
			InputTokens:  oaiResp.Usage.PromptTokens,
			OutputTokens: oaiResp.Usage.CompletionTokens,
		},
	}, nil
}

// MessagesToChatCompletion translates non-stream Messages response to Chat.
// Maps text+tool_use→tool_calls and stop_reason→finish_reason.
func MessagesToChatCompletion(anthropicBody []byte, model string) (MessagesToChatCompletionResult, error) {
	var aResp map[string]interface{}
	if err := json.Unmarshal(anthropicBody, &aResp); err != nil {
		return MessagesToChatCompletionResult{}, err
	}

	if errMap, exists := aResp["error"].(map[string]interface{}); exists {
		msg, _ := errMap["message"].(string)
		t, _ := errMap["type"].(string)
		body, err := json.Marshal(map[string]interface{}{
			"error": map[string]interface{}{
				"message": msg,
				"type":    t,
				"code":    errMap["code"],
			},
		})
		if err != nil {
			return MessagesToChatCompletionResult{}, err
		}
		return MessagesToChatCompletionResult{Body: body}, nil
	}

	id, _ := aResp["id"].(string)
	modelName, _ := aResp["model"].(string)
	if modelName == "" {
		modelName = model
	}

	var textContent strings.Builder
	var thinkingContent strings.Builder
	var toolCalls []interface{}
	if contentArr, exists := aResp["content"].([]interface{}); exists {
		for _, block := range contentArr {
			blockMap, ok := block.(map[string]interface{})
			if !ok {
				continue
			}
			t, _ := blockMap["type"].(string)
			switch t {
			case "text":
				txt, _ := blockMap["text"].(string)
				textContent.WriteString(txt)
			case "thinking":
				think, _ := blockMap["thinking"].(string)
				thinkingContent.WriteString(think)
			case "tool_use":
				toolID, _ := blockMap["id"].(string)
				name, _ := blockMap["name"].(string)
				input := blockMap["input"]
				var arguments string
				if inputBytes, err := json.Marshal(input); err == nil {
					arguments = string(inputBytes)
				} else {
					arguments = "{}"
				}
				toolCalls = append(toolCalls, map[string]interface{}{
					"id":   toolID,
					"type": "function",
					"function": map[string]interface{}{
						"name":      name,
						"arguments": arguments,
					},
				})
			}
		}
	}

	var inputTokens, outputTokens, cachedTokens, cacheCreationTokens int
	if usage, exists := aResp["usage"].(map[string]interface{}); exists {
		it, _ := usage["input_tokens"].(float64)
		ot, _ := usage["output_tokens"].(float64)
		cr, _ := usage["cache_read_input_tokens"].(float64)
		cc, _ := usage["cache_creation_input_tokens"].(float64)
		cachedTokens = int(cr)
		cacheCreationTokens = int(cc)
		outputTokens = int(ot)
		// Normalize to total input (input + cache read + cache creation), aligning
		// with OpenAI prompt_tokens semantics (which already include cached tokens).
		inputTokens = int(it) + cachedTokens + cacheCreationTokens
	}

	finishReason := "stop"
	if sr, ok := aResp["stop_reason"].(string); ok {
		switch sr {
		case "max_tokens":
			finishReason = "length"
		case "tool_use":
			finishReason = "tool_calls"
		case "end_turn", "stop_sequence":
			finishReason = "stop"
		}
	}
	if len(toolCalls) > 0 && finishReason == "stop" {
		// Prefer tool_calls when tool_use present, even if stop_reason missing
		finishReason = "tool_calls"
	}

	message := map[string]interface{}{
		"role":    "assistant",
		"content": textContent.String(),
	}
	if thinkingContent.Len() > 0 {
		message["reasoning_content"] = thinkingContent.String()
	}
	if len(toolCalls) > 0 {
		message["tool_calls"] = toolCalls
		// OpenAI: content may be null with tool_calls; keep empty string for clients
		if textContent.Len() == 0 {
			message["content"] = nil
		}
	}

	usageMap := map[string]interface{}{
		"prompt_tokens":     inputTokens,
		"completion_tokens": outputTokens,
		"total_tokens":      inputTokens + outputTokens,
	}
	if cachedTokens > 0 {
		usageMap["prompt_tokens_details"] = map[string]interface{}{
			"cached_tokens": cachedTokens,
		}
	}

	openaiResp := map[string]interface{}{
		"id":      id,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   modelName,
		"choices": []interface{}{
			map[string]interface{}{
				"index":         0,
				"message":       message,
				"finish_reason": finishReason,
			},
		},
		"usage": usageMap,
	}

	body, err := json.Marshal(openaiResp)
	if err != nil {
		return MessagesToChatCompletionResult{}, err
	}

	return MessagesToChatCompletionResult{
		Body: body,
		Usage: TokenUsage{
			InputTokens:         inputTokens,
			OutputTokens:        outputTokens,
			CachedTokens:        cachedTokens,
			CacheCreationTokens: cacheCreationTokens,
		},
	}, nil
}

// anthropicImageToOpenAIURL converts an Anthropic image block to an OpenAI image_url string.
// Supports base64 sources (→ data: URI) and url sources (passed through).
func anthropicImageToOpenAIURL(blockMap map[string]interface{}) string {
	source, ok := blockMap["source"].(map[string]interface{})
	if !ok {
		return ""
	}
	srcType, _ := source["type"].(string)
	switch srcType {
	case "base64":
		mediaType, _ := source["media_type"].(string)
		data, _ := source["data"].(string)
		if data == "" {
			return ""
		}
		if mediaType == "" {
			mediaType = "image/png"
		}
		return fmt.Sprintf("data:%s;base64,%s", mediaType, data)
	case "url":
		url, _ := source["url"].(string)
		return url
	default:
		return ""
	}
}

// buildOpenAIContentMessage assembles an OpenAI message whose content is a
// multimodal array (text parts followed by image parts).
func buildOpenAIContentMessage(role string, textParts []string, imageParts []interface{}) map[string]interface{} {
	var parts []interface{}
	if merged := strings.Join(textParts, "\n"); merged != "" {
		parts = append(parts, map[string]interface{}{
			"type": "text",
			"text": merged,
		})
	}
	parts = append(parts, imageParts...)
	return map[string]interface{}{
		"role":    role,
		"content": parts,
	}
}

// IsOfficialOrTestBaseURL reports whether baseURL uses official OpenAI compat.
func IsOfficialOrTestBaseURL(baseURL string) bool {
	return strings.Contains(baseURL, "api.openai.com") ||
		strings.Contains(baseURL, "127.0.0.1") ||
		strings.Contains(baseURL, "localhost")
}

// CorrectNativeMessagesRequest sanitizes tool input_schema in native Anthropic /messages requests.
// Only tools[].input_schema is rewritten (e.g. required:null → []); message/tool semantics are preserved.
func CorrectNativeMessagesRequest(rawBody []byte) ([]byte, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("parse raw body: %w", err)
	}

	tools, ok := payload["tools"].([]interface{})
	if !ok || len(tools) == 0 {
		return rawBody, nil
	}

	changed := false
	for _, t := range tools {
		tMap, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		if schema, ok := tMap["input_schema"].(map[string]interface{}); ok {
			tMap["input_schema"] = cleanJSONSchema(schema, true)
			changed = true
		}
	}
	if !changed {
		return rawBody, nil
	}
	return json.Marshal(payload)
}

