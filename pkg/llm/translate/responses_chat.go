package translate

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// builtinToolStandardFields lists allowed fields for OpenAI native responses tools
// (non function/mcp/shell). Sanitize keeps only these; strip client-private fields.
var builtinToolStandardFields = map[string][]string{
	"web_search":           {"search_context_size", "user_location", "filters"},
	"web_search_preview":   {"search_context_size", "user_location", "filters"},
	"code_interpreter":     {"container"},
	"image_generation":     {"model", "size", "quality", "background", "output_format"},
	"file_search":          {"vector_store_ids", "max_num_results", "filters", "ranking_options"},
	"computer_use_preview": {"display_width", "display_height", "environment"},
	"tool_search":          {"query", "max_num_results", "filters"},
}

// ResponsesRequestToChat translates Responses request to Chat Completions.
// The returned ToolNameMapper records the (namespace, name) -> sanitized-name
// mapping used on the request side; pass it to ChatCompletionToResponses /
// handleResponsesStream so the response path can restore original namespaces.
// Strict upstreams (DeepSeek, Qwen) reject dots in tools[].function.name, so
// namespace-qualified names are sanitized to ^[a-zA-Z0-9_-]+$.
func ResponsesRequestToChat(rawBody []byte) ([]byte, *ToolNameMapper, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, nil, fmt.Errorf("parse raw body: %w", err)
	}

	mapper := NewToolNameMapper()

	// In thinking/reasoning mode, upstream protocols expect assistant history turns
	// to carry reasoning_content. We infer thinking mode purely from protocol signals:
	// 1. Explicit reasoning parameters (e.g. reasoning.effort != "none")
	// 2. Or the presence of reasoning items in conversation history
	hasReasoningConfig := false
	if reasoningVal, ok := payload["reasoning"].(map[string]interface{}); ok {
		if effort, _ := reasoningVal["effort"].(string); effort != "" && effort != "none" {
			hasReasoningConfig = true
		}
	}

	var openAIMessages []interface{}
	if instructions, ok := payload["instructions"].(string); ok && instructions != "" {
		openAIMessages = append(openAIMessages, map[string]interface{}{
			"role":    "system",
			"content": instructions,
		})
	}

	if inputVal, ok := payload["input"]; ok && inputVal != nil {
		if inputStr, ok := inputVal.(string); ok {
			if inputStr != "" {
				openAIMessages = append(openAIMessages, map[string]interface{}{
					"role":    "user",
					"content": inputStr,
				})
			}
		} else if inputArr, ok := inputVal.([]interface{}); ok {
			// First pass: collect valid tool_call_ids and detect if history contains reasoning items
			validToolCallIDs := make(map[string]bool)
			hasReasoningHistory := false
			for _, item := range inputArr {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				itemType, _ := itemMap["type"].(string)
				if itemType == "function_call_output" {
					if callID, _ := itemMap["call_id"].(string); callID != "" {
						validToolCallIDs[callID] = true
					}
				} else if itemType == "reasoning" {
					hasReasoningHistory = true
				}
			}

			inThinkingMode := hasReasoningConfig || hasReasoningHistory

			// Second pass: assemble openAIMessages while merging contiguous function_call items
			var pendingToolCalls []interface{}
			// pendingReasoning carries the text of the most recent reasoning item so
			// it can be folded into the next assistant message's `reasoning_content`
			// field. Thinking models reject multi-turn history that omits it.
			var pendingReasoning string

			flushPendingToolCalls := func() {
				if len(pendingToolCalls) > 0 {
					msg := map[string]interface{}{
						"role":       "assistant",
						"tool_calls": pendingToolCalls,
					}
					if pendingReasoning != "" {
						msg["reasoning_content"] = pendingReasoning
						pendingReasoning = ""
					} else if inThinkingMode {
						// Thinking models require reasoning_content on every assistant turn
						msg["reasoning_content"] = ""
					}
					openAIMessages = append(openAIMessages, msg)
					pendingToolCalls = nil
				}
			}

			for _, item := range inputArr {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				itemType, _ := itemMap["type"].(string)

				// Reasoning items carry the prior turn's thinking. DeepSeek (and
				// other thinking models) require reasoning_content to be passed
				// back in multi-turn history; stash the summary text so the next
				// assistant message can carry it. Codex may send reasoning as
				// either a `summary` array or an `encrypted_content` blob — only
				// the summary text form is recoverable for upstream re-injection.
				if itemType == "reasoning" {
					if text := extractReasoningSummary(itemMap); text != "" {
						pendingReasoning = text
					}
					continue
				}

				// 1. Function Call item
				if itemType == "function_call" {
					callID, _ := itemMap["call_id"].(string)
					if callID == "" {
						callID, _ = itemMap["id"].(string)
					}
					// Only keep tool calls that have a corresponding output in context to avoid "unresponded tool_call_id" upstream error
					if !validToolCallIDs[callID] {
						continue
					}
					name, _ := itemMap["name"].(string)
					args, _ := itemMap["arguments"].(string)
					if ns, _ := itemMap["namespace"].(string); ns != "" {
						name = mapper.SanitizeAndRegister(ns, name)
					}
					pendingToolCalls = append(pendingToolCalls, map[string]interface{}{
						"id":   callID,
						"type": "function",
						"function": map[string]interface{}{
							"name":      name,
							"arguments": args,
						},
					})
					continue
				}

				// Flush pending tool_calls before any non-function_call item
				flushPendingToolCalls()

				// 2. Function Call Output item from Responses payload (Tool Result)
				if itemType == "function_call_output" {
					callID, _ := itemMap["call_id"].(string)
					outputStr := ""
					if outVal, ok := itemMap["output"]; ok {
						if str, ok := outVal.(string); ok {
							outputStr = str
						} else if blocks, ok := outVal.([]interface{}); ok {
							var blockText strings.Builder
							for _, block := range blocks {
								blockMap, ok := block.(map[string]interface{})
								if !ok {
									continue
								}
								if text, _ := blockMap["text"].(string); text != "" {
									blockText.WriteString(text)
								}
							}
							outputStr = blockText.String()
							if outputStr == "" {
								if bytes, err := json.Marshal(outVal); err == nil {
									outputStr = string(bytes)
								}
							}
						} else if bytes, err := json.Marshal(outVal); err == nil {
							outputStr = string(bytes)
						}
					}
					openAIMessages = append(openAIMessages, map[string]interface{}{
						"role":         "tool",
						"tool_call_id": callID,
						"content":      outputStr,
					})
					continue
				}

				// 3. Regular Message item
				role, _ := itemMap["role"].(string)
				openAIRole := "user"
				if role == "developer" || role == "system" {
					openAIRole = "system"
				} else if role == "assistant" {
					openAIRole = "assistant"
				} else if role != "" {
					openAIRole = role
				}

				var textContent strings.Builder
				if contentVal, ok := itemMap["content"]; ok {
					if contentStr, ok := contentVal.(string); ok {
						textContent.WriteString(contentStr)
					} else if contentArr, ok := contentVal.([]interface{}); ok {
						for _, c := range contentArr {
							if cMap, ok := c.(map[string]interface{}); ok {
								if text, ok := cMap["text"].(string); ok && text != "" {
									textContent.WriteString(text)
								} else if text, ok := cMap["input_text"].(string); ok && text != "" {
									textContent.WriteString(text)
								} else if text, ok := cMap["value"].(string); ok && text != "" {
									textContent.WriteString(text)
								}
							}
						}
					}
				}

				// Filter out empty assistant messages without tool calls, as they corrupt upstream context
				if openAIRole == "assistant" && strings.TrimSpace(textContent.String()) == "" {
					if toolCalls, ok := itemMap["tool_calls"]; !ok || toolCalls == nil {
						continue
					}
				}

				msg := map[string]interface{}{
					"role":    openAIRole,
					"content": textContent.String(),
				}
				if name, ok := itemMap["name"].(string); ok && name != "" {
					msg["name"] = name
				}
				if toolCallID, ok := itemMap["tool_call_id"].(string); ok && toolCallID != "" {
					msg["tool_call_id"] = toolCallID
				}
				if toolCalls, ok := itemMap["tool_calls"]; ok {
					msg["tool_calls"] = toolCalls
				}
				// Fold any pending reasoning into this assistant message so thinking
				// models receive reasoning_content back in multi-turn history.
				if openAIRole == "assistant" {
					if pendingReasoning != "" {
						msg["reasoning_content"] = pendingReasoning
						pendingReasoning = ""
					} else if inThinkingMode {
						// Thinking mode requires reasoning_content on every assistant turn.
						// Fall back to empty string to satisfy upstream protocol validation.
						if _, exists := msg["reasoning_content"]; !exists {
							msg["reasoning_content"] = ""
						}
					}
				}

				openAIMessages = append(openAIMessages, msg)
			}
			flushPendingToolCalls()
		}
	}
	payload["messages"] = openAIMessages
	delete(payload, "input")
	delete(payload, "instructions")

	// Responses-only state and formatting parameters are invalid on Chat
	// Completions. Preserve the reasoning effort in the Chat-native field.
	if reasoningVal, ok := payload["reasoning"].(map[string]interface{}); ok {
		if effort, _ := reasoningVal["effort"].(string); effort != "" {
			if _, exists := payload["reasoning_effort"]; !exists {
				payload["reasoning_effort"] = effort
			}
		}
	}
	for _, key := range []string{"store", "previous_response_id", "include", "background", "truncation", "text", "reasoning"} {
		delete(payload, key)
	}

	if maxOutputTokens, ok := payload["max_output_tokens"]; ok {
		payload["max_completion_tokens"] = maxOutputTokens
		delete(payload, "max_output_tokens")
	}

	if tools, ok := payload["tools"].([]interface{}); ok {
		var finalTools []interface{}
		for _, t := range tools {
			toolMap, ok := t.(map[string]interface{})
			if !ok {
				continue
			}
			toolType, _ := toolMap["type"].(string)

			if toolType == "namespace" {
				if subTools, ok := toolMap["tools"].([]interface{}); ok {
					for _, st := range subTools {
						subToolMap, ok := st.(map[string]interface{})
						if !ok {
							continue
						}
						stdTool := BuildStandardTool(subToolMap)
						if stdTool != nil && stdTool["type"] == "function" {
							if ns, _ := toolMap["name"].(string); ns != "" {
								stdTool["namespace"] = ns
							}
							finalTools = append(finalTools, WrapFlatToolToNestedOpenAI(stdTool, mapper))
						}
					}
				}
			} else {
				stdTool := BuildStandardTool(toolMap)
				if stdTool != nil && stdTool["type"] == "function" {
					finalTools = append(finalTools, WrapFlatToolToNestedOpenAI(stdTool, mapper))
				}
			}
		}

		if len(finalTools) > 0 {
			payload["tools"] = finalTools
		} else {
			delete(payload, "tools")
			delete(payload, "tool_choice")
		}
	}

	newBody, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal translated body: %w", err)
	}
	return newBody, mapper, nil
}

// ChatCompletionToResponsesResult is non-stream Chat→Responses result.
type ChatCompletionToResponsesResult struct {
	Body  []byte
	Usage TokenUsage
}

// ChatCompletionToResponses translates non-stream Chat response to Responses.
// mapper (from ResponsesRequestToChat) restores original (namespace, name) for
// sanitized tool names the upstream echoed back. nil mapper falls back to the
// dot-split heuristic.
func ChatCompletionToResponses(chatBody []byte, model string, mapper *ToolNameMapper) (ChatCompletionToResponsesResult, error) {
	var oaiResp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Role             string `json:"role"`
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				Reasoning        string `json:"reasoning"`
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
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(chatBody, &oaiResp); err != nil {
		return ChatCompletionToResponsesResult{}, err
	}

	now := time.Now().Unix()
	respID := oaiResp.ID
	if strings.HasPrefix(respID, "chatcmpl-") {
		respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
	} else if respID == "" {
		respID = "resp_mock"
	} else if !strings.HasPrefix(respID, "resp_") {
		respID = "resp_" + respID
	}

	msgID := oaiResp.ID
	if strings.HasPrefix(msgID, "chatcmpl-") {
		msgID = strings.Replace(msgID, "chatcmpl-", "msg_", 1)
	} else if msgID == "" {
		msgID = "msg_mock"
	} else if !strings.HasPrefix(msgID, "msg_") {
		msgID = "msg_" + msgID
	}

	var outputList []map[string]interface{}

	if len(oaiResp.Choices) > 0 {
		choice := oaiResp.Choices[0]
		reasoningText := choice.Message.ReasoningContent
		if reasoningText == "" {
			reasoningText = choice.Message.Reasoning
		}
		if reasoningText != "" {
			reasoningID := oaiResp.ID
			if strings.HasPrefix(reasoningID, "chatcmpl-") {
				reasoningID = strings.Replace(reasoningID, "chatcmpl-", "rs_", 1)
			} else if reasoningID == "" {
				reasoningID = "rs_mock"
			} else if !strings.HasPrefix(reasoningID, "rs_") {
				reasoningID = "rs_" + reasoningID
			}
			outputList = append(outputList, map[string]interface{}{
				"id":     reasoningID,
				"type":   "reasoning",
				"status": "completed",
				"summary": []map[string]interface{}{
					{
						"type": "summary_text",
						"text": reasoningText,
					},
				},
			})
		}
		if len(choice.Message.ToolCalls) > 0 {
			for _, tc := range choice.Message.ToolCalls {
				toolName := tc.Function.Name
				var toolNamespace string
				if mapper != nil {
					toolNamespace, toolName = mapper.Restore(toolName)
				} else {
					toolNamespace = splitChatToolNamespace(toolName)
					toolName = chatToolLocalName(toolName)
				}
				outputList = append(outputList, map[string]interface{}{
					"id":        tc.ID,
					"call_id":   tc.ID,
					"type":      "function_call",
					"status":    "completed",
					"name":      toolName,
					"namespace": toolNamespace,
					"arguments": tc.Function.Arguments,
				})
			}
		} else {
			content := choice.Message.Content
			outputList = append(outputList, map[string]interface{}{
				"type":   "message",
				"id":     msgID,
				"status": "completed",
				"role":   "assistant",
				"content": []map[string]interface{}{
					{
						"type":        "output_text",
						"text":        content,
						"annotations": []interface{}{},
					},
				},
			})
		}
	}

	respModel := model
	if respModel == "" {
		respModel = oaiResp.Model
	}

	totalTokens := oaiResp.Usage.TotalTokens
	if totalTokens == 0 {
		totalTokens = oaiResp.Usage.PromptTokens + oaiResp.Usage.CompletionTokens
	}

	responsesResp := map[string]interface{}{
		"id":           respID,
		"object":       "response",
		"created_at":   now,
		"status":       "completed",
		"completed_at": now + 1,
		"model":        respModel,
		"output":       outputList,
		"usage": map[string]interface{}{
			"input_tokens":  oaiResp.Usage.PromptTokens,
			"output_tokens": oaiResp.Usage.CompletionTokens,
			"total_tokens":  totalTokens,
		},
	}

	translatedBody, err := json.Marshal(responsesResp)
	if err != nil {
		return ChatCompletionToResponsesResult{}, err
	}

	return ChatCompletionToResponsesResult{
		Body: translatedBody,
		Usage: TokenUsage{
			InputTokens:  oaiResp.Usage.PromptTokens,
			OutputTokens: oaiResp.Usage.CompletionTokens,
		},
	}, nil
}

// StripToolSearchDescription removes description from server-executed tool_search
// tools. OpenAI rejects tools.tool_search.description with 400 invalid_request_error.
// Other tools and other tool_search fields are left untouched. stripped is false
// when the body has no such field, including when the body is not valid JSON.
func StripToolSearchDescription(rawBody []byte) (body []byte, stripped bool, err error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return rawBody, false, nil
	}
	if !stripToolSearchDescription(payload["tools"]) {
		return rawBody, false, nil
	}
	body, err = json.Marshal(payload)
	if err != nil {
		return nil, false, fmt.Errorf("marshal stripped body: %w", err)
	}
	return body, true, nil
}

func stripToolSearchDescription(toolsVal interface{}) bool {
	tools, ok := toolsVal.([]interface{})
	if !ok {
		return false
	}
	stripped := false
	for _, item := range tools {
		tool, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		toolType, _ := tool["type"].(string)
		if toolType == "tool_search" {
			if _, exists := tool["description"]; exists {
				delete(tool, "description")
				stripped = true
			}
		}
		if stripToolSearchDescription(tool["tools"]) {
			stripped = true
		}
	}
	return stripped
}

// CorrectNativeResponsesRequest sanitizes native /responses (roles/types + tools).
// Returns body and final tools summary for logging.
func CorrectNativeResponsesRequest(rawBody []byte) (body []byte, originalToolCount, finalToolCount int, toolSummary []string, err error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, 0, 0, nil, fmt.Errorf("parse raw body: %w", err)
	}

	correctInputForNativeResponses(payload)

	tools, ok := payload["tools"].([]interface{})
	if !ok {
		newBody, mErr := json.Marshal(payload)
		if mErr != nil {
			return nil, 0, 0, nil, fmt.Errorf("marshal corrected body: %w", mErr)
		}
		return newBody, 0, 0, nil, nil
	}

	originalToolCount = len(tools)
	var finalTools []interface{}
	hasDeferredOrNamespace := false

	for _, t := range tools {
		toolMap, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		toolType, _ := toolMap["type"].(string)

		if toolType == "namespace" {
			hasDeferredOrNamespace = true
			nsTool := make(map[string]interface{})
			for k, v := range toolMap {
				if k != "tools" {
					nsTool[k] = v
				}
			}
			nsTool["type"] = "namespace"

			var cleanedSubTools []interface{}
			if subTools, ok := toolMap["tools"].([]interface{}); ok {
				for _, st := range subTools {
					subToolMap, ok := st.(map[string]interface{})
					if !ok {
						continue
					}
					stdTool := BuildStandardTool(subToolMap)
					if stdTool != nil {
						cleanedSubTools = append(cleanedSubTools, stdTool)
					}
				}
			}
			nsTool["tools"] = cleanedSubTools
			finalTools = append(finalTools, nsTool)
		} else {
			stdTool := BuildStandardTool(toolMap)
			if stdTool != nil {
				if dl, ok := stdTool["defer_loading"].(bool); ok && dl {
					hasDeferredOrNamespace = true
				}
				finalTools = append(finalTools, stdTool)
			}
		}
	}

	// Defensive check: if tool_search exists but there is no namespace or deferred tool,
	// strip isolated tool_search to avoid upstream 400 invalid_request_error: requires at least one deferred tool
	if !hasDeferredOrNamespace {
		filtered := make([]interface{}, 0, len(finalTools))
		for _, t := range finalTools {
			if tm, ok := t.(map[string]interface{}); ok {
				if tm["type"] == "tool_search" {
					continue
				}
			}
			filtered = append(filtered, t)
		}
		finalTools = filtered
	}

	if len(finalTools) > 0 {
		payload["tools"] = finalTools
	} else {
		delete(payload, "tools")
		delete(payload, "tool_choice")
	}

	for _, t := range finalTools {
		if tm, ok := t.(map[string]interface{}); ok {
			ttype, _ := tm["type"].(string)
			tname, _ := tm["name"].(string)
			if ttype == "namespace" {
				subLen := 0
				if st, ok := tm["tools"].([]interface{}); ok {
					subLen = len(st)
				}
				toolSummary = append(toolSummary, fmt.Sprintf("namespace:%s(%d_tools)", tname, subLen))
			} else {
				toolSummary = append(toolSummary, fmt.Sprintf("%s:%s", ttype, tname))
			}
		}
	}

	newBody, err := json.Marshal(payload)
	if err != nil {
		return nil, originalToolCount, 0, nil, fmt.Errorf("marshal corrected body: %w", err)
	}
	return newBody, originalToolCount, len(finalTools), toolSummary, nil
}

// BuildStandardTool normalizes a client tool to flat standard form.
func BuildStandardTool(toolMap map[string]interface{}) map[string]interface{} {
	toolType, _ := toolMap["type"].(string)
	if toolType == "" {
		if name, ok := toolMap["name"].(string); ok && name != "" {
			toolType = "function"
		} else {
			return nil
		}
	}

	targetType := toolType
	if targetType == "" || targetType == "custom" {
		targetType = "function"
	}

	if targetType != "function" && targetType != "mcp" && targetType != "shell" {
		builtin := map[string]interface{}{"type": targetType}
		if allowed, ok := builtinToolStandardFields[targetType]; ok {
			for _, field := range allowed {
				if v, exists := toolMap[field]; exists {
					builtin[field] = v
				}
			}
		}
		return builtin
	}

	if innerObj, ok := toolMap[toolType].(map[string]interface{}); ok {
		flatTool := make(map[string]interface{})
		flatTool["type"] = toolType

		for k, v := range innerObj {
			flatTool[k] = v
		}

		for k, v := range toolMap {
			if k != "type" && k != toolType {
				if _, exists := flatTool[k]; !exists {
					flatTool[k] = v
				}
			}
		}

		if toolType == "function" || toolType == "mcp" || toolType == "shell" {
			if name, ok := flatTool["name"].(string); !ok || name == "" {
				return nil
			}
		}

		return flatTool
	}

	targetType = toolType
	if targetType == "" || targetType == "custom" {
		targetType = "function"
	}

	innerName, _ := toolMap["name"].(string)
	if innerName == "" {
		innerName = targetType
	}

	flatTool := make(map[string]interface{})
	for k, v := range toolMap {
		if k != "type" && k != "name" {
			flatTool[k] = v
		}
	}
	flatTool["type"] = targetType
	flatTool["name"] = innerName

	if targetType == "function" || targetType == "mcp" || targetType == "shell" {
		if innerName == "" {
			return nil
		}
	}

	if innerName == "apply_patch" {
		params, _ := flatTool["parameters"].(map[string]interface{})
		if params == nil {
			params = make(map[string]interface{})
			flatTool["parameters"] = params
		}
		props, _ := params["properties"].(map[string]interface{})
		if len(props) == 0 {
			params["type"] = "object"
			params["properties"] = map[string]interface{}{
				"patch": map[string]interface{}{
					"type":        "string",
					"description": "The full patch or diff content to apply to files.",
				},
			}
			params["required"] = []string{"patch"}
		}
	}

	return flatTool
}

// splitChatToolNamespace extracts the original Responses namespace from a
// Chat Completions function name. Empty means the default functions namespace.
func splitChatToolNamespace(name string) string {
	if idx := strings.LastIndex(name, "."); idx > 0 && idx < len(name)-1 {
		return name[:idx]
	}
	return ""
}

func chatToolLocalName(name string) string {
	if idx := strings.LastIndex(name, "."); idx > 0 && idx < len(name)-1 {
		return name[idx+1:]
	}
	return name
}

// WrapFlatToolToNestedOpenAI wraps a flat function tool as nested OpenAI Chat form.
// When the flat tool carries a namespace, the name is sanitized to a pattern-safe
// form via mapper (registering the mapping for later response-side restoration),
// instead of the raw "namespace.name" form that strict upstreams reject.
func WrapFlatToolToNestedOpenAI(flatTool map[string]interface{}, mapper *ToolNameMapper) map[string]interface{} {
	toolType, _ := flatTool["type"].(string)
	if toolType == "" {
		toolType = "function"
	}

	innerMap := make(map[string]interface{})
	for k, v := range flatTool {
		if k != "type" {
			innerMap[k] = v
		}
	}
	if ns, ok := innerMap["namespace"].(string); ok && ns != "" {
		if name, _ := innerMap["name"].(string); name != "" {
			innerMap["name"] = mapper.SanitizeAndRegister(ns, name)
		}
		delete(innerMap, "namespace")
	}

	return map[string]interface{}{
		"type":   toolType,
		toolType: innerMap,
	}
}

func correctInputForNativeResponses(payload map[string]interface{}) {
	inputVal, ok := payload["input"]
	if !ok || inputVal == nil {
		return
	}

	inputArr, ok := inputVal.([]interface{})
	if !ok {
		return
	}

	for _, item := range inputArr {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		if role, ok := itemMap["role"].(string); ok {
			if role == "developer" {
				itemMap["role"] = "system"
			}
		}

		// Keep Responses content part types as-is.
		// OpenAI Responses / Codex backend only accept:
		// input_text, input_image, output_text, refusal, input_file, ...
		// Rewriting input_text -> text breaks upstream with:
		// Invalid value: 'text'. Supported values are: 'input_text', ...
		if contentVal, ok := itemMap["content"]; ok && contentVal != nil {
			if contentArr, ok := contentVal.([]interface{}); ok {
				for _, c := range contentArr {
					cMap, ok := c.(map[string]interface{})
					if !ok {
						continue
					}
					if cType, ok := cMap["type"].(string); ok {
						// Normalize legacy/compat alias "text" to official input_text.
						if cType == "text" {
							cMap["type"] = "input_text"
						}
					}
				}
			}
		}

		// Drop item-level type when role/content message shape is used.
		// Keep typed items (e.g. function_call / reasoning) intact.
		if _, hasRole := itemMap["role"]; hasRole {
			if _, hasContent := itemMap["content"]; hasContent {
				delete(itemMap, "type")
			}
		}
	}
}

// extractReasoningSummary pulls the text of a Responses reasoning item. Codex
// sends reasoning back as either a `summary` array of {type:"summary_text",
// text:...} parts, a `content` array/string, or a plain string. Only the plaintext
// summary form is recoverable for re-injection as reasoning_content upstream;
// encrypted reasoning has no plaintext to pass back.
func extractReasoningSummary(itemMap map[string]interface{}) string {
	// 1. Try summary array: [{"type": "summary_text", "text": "..."}] or ["..."]
	if summary, ok := itemMap["summary"].([]interface{}); ok {
		var b strings.Builder
		for _, part := range summary {
			if partMap, ok := part.(map[string]interface{}); ok {
				if text, _ := partMap["text"].(string); text != "" {
					b.WriteString(text)
				}
			} else if str, ok := part.(string); ok && str != "" {
				b.WriteString(str)
			}
		}
		if b.Len() > 0 {
			return b.String()
		}
	}
	// 2. Try summary as string
	if summaryStr, ok := itemMap["summary"].(string); ok && summaryStr != "" {
		return summaryStr
	}
	// 3. Try reasoning_content or text as string
	if rc, ok := itemMap["reasoning_content"].(string); ok && rc != "" {
		return rc
	}
	if text, ok := itemMap["text"].(string); ok && text != "" {
		return text
	}
	// 4. Try content array
	if contentArr, ok := itemMap["content"].([]interface{}); ok {
		var b strings.Builder
		for _, part := range contentArr {
			if partMap, ok := part.(map[string]interface{}); ok {
				if text, _ := partMap["text"].(string); text != "" {
					b.WriteString(text)
				}
			} else if str, ok := part.(string); ok && str != "" {
				b.WriteString(str)
			}
		}
		if b.Len() > 0 {
			return b.String()
		}
	}
	return ""
}
