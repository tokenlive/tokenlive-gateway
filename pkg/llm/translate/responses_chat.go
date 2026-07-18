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
}

// ResponsesRequestToChat translates Responses request to Chat Completions.
func ResponsesRequestToChat(rawBody []byte) ([]byte, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("parse raw body: %w", err)
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
			for _, item := range inputArr {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				role, _ := itemMap["role"].(string)
				openAIRole := "user"
				if role == "developer" {
					openAIRole = "system"
				} else if role == "assistant" {
					openAIRole = "assistant"
				} else if role == "system" {
					openAIRole = "system"
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
								if text, ok := cMap["text"].(string); ok {
									textContent.WriteString(text)
								}
							}
						}
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

				openAIMessages = append(openAIMessages, msg)
			}
		}
	}
	payload["messages"] = openAIMessages
	delete(payload, "input")
	delete(payload, "instructions")

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
							finalTools = append(finalTools, WrapFlatToolToNestedOpenAI(stdTool))
						}
					}
				}
			} else {
				stdTool := BuildStandardTool(toolMap)
				if stdTool != nil && stdTool["type"] == "function" {
					finalTools = append(finalTools, WrapFlatToolToNestedOpenAI(stdTool))
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
		return nil, fmt.Errorf("marshal translated body: %w", err)
	}
	return newBody, nil
}

// ChatCompletionToResponsesResult is non-stream Chat→Responses result.
type ChatCompletionToResponsesResult struct {
	Body  []byte
	Usage TokenUsage
}

// ChatCompletionToResponses translates non-stream Chat response to Responses.
func ChatCompletionToResponses(chatBody []byte, model string) (ChatCompletionToResponsesResult, error) {
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
		if len(choice.Message.ToolCalls) > 0 {
			for _, tc := range choice.Message.ToolCalls {
				outputList = append(outputList, map[string]interface{}{
					"id":        tc.ID,
					"type":      "function_call",
					"status":    "completed",
					"name":      tc.Function.Name,
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
					subType, _ := subToolMap["type"].(string)
					if subType == "namespace" {
						continue
					}

					stdTool := BuildStandardTool(subToolMap)
					if stdTool != nil {
						finalTools = append(finalTools, stdTool)
					}
				}
			}
		} else {
			stdTool := BuildStandardTool(toolMap)
			if stdTool != nil {
				finalTools = append(finalTools, stdTool)
			}
		}
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
			toolSummary = append(toolSummary, fmt.Sprintf("%s:%s", ttype, tname))
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

	return flatTool
}

// WrapFlatToolToNestedOpenAI wraps a flat function tool as nested OpenAI Chat form.
func WrapFlatToolToNestedOpenAI(flatTool map[string]interface{}) map[string]interface{} {
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

		if contentVal, ok := itemMap["content"]; ok && contentVal != nil {
			if contentArr, ok := contentVal.([]interface{}); ok {
				for _, c := range contentArr {
					cMap, ok := c.(map[string]interface{})
					if !ok {
						continue
					}
					if cType, ok := cMap["type"].(string); ok {
						if cType == "input_text" {
							cMap["type"] = "text"
						}
					}
				}
			}
		}

		delete(itemMap, "type")
	}
}
