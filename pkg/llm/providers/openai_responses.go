package providers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"

	"go.uber.org/zap"
)

type openaiResponsesInvoker struct{}

func (i *openaiResponsesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	gctx.Logger(zap.L()).Info("openaiResponsesInvoker Invoke entry called", zap.String("model", gctx.Model), zap.String("req_type", string(gctx.RequestType)))
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}

	// 1. 分流判定：端点是否原生支持 responses
	hasResponseCapability := false
	if gctx.SelectedEndpoint != nil {
		for _, cap := range gctx.SelectedEndpoint.RequestTypes {
			if cap == core.RequestTypeResponses {
				hasResponseCapability = true
				break
			}
		}
	}

	// 分支 A：原生同名转发
	if hasResponseCapability {
		endpoint := op.baseURL + "/responses"
		return op.doRequest(gctx, endpoint)
	}

	// 分支 B：协议降级与翻译 (Responses -> Chat/Completions)
	newBody, err := translateResponsesToChatCompletion(gctx.RawBody)
	if err != nil {
		return err
	}
	gctx.RawBody = newBody

	// 同名重定向到上游 /chat/completions
	endpoint := op.baseURL + "/chat/completions"
	if err := op.doRequest(gctx, endpoint); err != nil {
		return err
	}

	// 翻译响应体 (OpenAI Chat -> Responses)
	if gctx.IsStream {
		return handleResponsesStream(gctx, gctx.UpstreamResponse)
	} else {
		if err := translateResponsesNonStreamResponse(gctx); err != nil {
			return fmt.Errorf("translate response: %w", err)
		}
	}

	return nil
}

func translateResponsesToChatCompletion(rawBody []byte) ([]byte, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return nil, fmt.Errorf("parse raw body: %w", err)
	}

	var openAIMessages []interface{}
	// 处理 instructions -> system message
	if instructions, ok := payload["instructions"].(string); ok && instructions != "" {
		openAIMessages = append(openAIMessages, map[string]interface{}{
			"role":    "system",
			"content": instructions,
		})
	}

	// 处理 input
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

				openAIMessages = append(openAIMessages, map[string]interface{}{
					"role":    openAIRole,
					"content": textContent.String(),
				})
			}
		}
	}
	payload["messages"] = openAIMessages
	delete(payload, "input")
	delete(payload, "instructions")

	// 映射 max_output_tokens -> max_completion_tokens
	if maxOutputTokens, ok := payload["max_output_tokens"]; ok {
		payload["max_completion_tokens"] = maxOutputTokens
		delete(payload, "max_output_tokens")
	}

	// 纠正 Codex 等客户端发送的非标准 tools 格式（平铺或自定义字段转换为嵌套的 function，且打平 namespace 级工具，过滤无名查询）
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
						subType, _ := subToolMap["type"].(string)
						if subType == "function" || subToolMap["name"] != nil {
							fn := buildStandardFunctionTool(subToolMap)
							if fn != nil {
								finalTools = append(finalTools, fn)
							}
						}
					}
				}
			} else {
				fn := buildStandardFunctionTool(toolMap)
				if fn != nil {
					finalTools = append(finalTools, fn)
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

func translateResponsesNonStreamResponse(gctx *core.GatewayContext) error {
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

	if err := json.Unmarshal(gctx.UpstreamBody, &oaiResp); err != nil {
		return err
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
		// 1. 如果有工具调用
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
			// 2. 纯文本消息回复
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

	responsesResp := map[string]interface{}{
		"id":           respID,
		"object":       "response",
		"created_at":   now,
		"status":       "completed",
		"completed_at": now + 1,
		"model":        gctx.Model,
		"output":       outputList,
		"usage": map[string]interface{}{
			"input_tokens":  oaiResp.Usage.PromptTokens,
			"output_tokens": oaiResp.Usage.CompletionTokens,
			"total_tokens":  oaiResp.Usage.TotalTokens,
		},
	}

	translatedBody, err := json.Marshal(responsesResp)
	if err != nil {
		return err
	}

	gctx.UpstreamBody = translatedBody

	var result map[string]interface{}
	if err := json.Unmarshal(translatedBody, &result); err != nil {
		return err
	}
	gctx.Response = result

	return nil
}

type responseCreatedEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
	} `json:"response"`
}

type responseInProgressEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
	} `json:"response"`
}

type responseOutputItemAddedEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID      string        `json:"id"`
		Type    string        `json:"type"`
		Status  string        `json:"status"`
		Role    string        `json:"role"`
		Content []interface{} `json:"content"`
	} `json:"item"`
}

type responseContentPartAddedEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Part         struct {
		Type        string        `json:"type"`
		Text        string        `json:"text"`
		Annotations []interface{} `json:"annotations"`
	} `json:"part"`
}

type responseOutputTextDeltaEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

type responseOutputTextDoneEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Text         string `json:"text"`
}

type responseContentPartDoneEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Part         struct {
		Type        string        `json:"type"`
		Text        string        `json:"text"`
		Annotations []interface{} `json:"annotations"`
	} `json:"part"`
}

type responseOutputItemDoneEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID      string        `json:"id"`
		Type    string        `json:"type"`
		Status  string        `json:"status"`
		Role    string        `json:"role"`
		Content []interface{} `json:"content"`
	} `json:"item"`
}

type responseDoneEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
		Usage     struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
	} `json:"response"`
}

type responseOutputItemAddedFunctionCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"item"`
}

type responseFunctionCallArgumentsDeltaEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	ItemID      string `json:"item_id"`
	OutputIndex int    `json:"output_index"`
	Delta       string `json:"delta"`
}

type responseFunctionCallArgumentsDoneEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	ItemID      string `json:"item_id"`
	OutputIndex int    `json:"output_index"`
	Arguments   string `json:"arguments"`
}

type responseOutputItemDoneFunctionCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"item"`
}

func writeResponseEvent(w io.Writer, eventType string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, string(jsonData))
	return err
}

type ParsedToolCall struct {
	Name      string
	Arguments string
}

func parseXMLToolCall(xmlStr string) (*ParsedToolCall, error) {
	// 1. 提取函数名
	funcRegex := regexp.MustCompile(`<function\s*=\s*([a-zA-Z0-9_\-]+)>`)
	funcMatches := funcRegex.FindStringSubmatch(xmlStr)
	if len(funcMatches) < 2 {
		funcRegexAlt := regexp.MustCompile(`<function\s+name\s*=\s*["']?([a-zA-Z0-9_\-]+)["']?>`)
		funcMatches = funcRegexAlt.FindStringSubmatch(xmlStr)
		if len(funcMatches) < 2 {
			return nil, fmt.Errorf("could not find function name")
		}
	}
	funcName := funcMatches[1]

	// 2. 提取所有 parameter 标签
	// 匹配不限制属性格式的 parameter：(?s)<parameter\s*([^>]*?)>(.*?)</parameter>
	paramRegex := regexp.MustCompile(`(?s)<parameter\s*([^>]*?)>(.*?)</parameter>`)
	matches := paramRegex.FindAllStringSubmatch(xmlStr, -1)

	argsMap := make(map[string]interface{})

	// 用于在无法提取属性键名时 fallback 默认字段名
	fallbackKey := ""
	if funcName == "exec_command" {
		fallbackKey = "cmd"
	} else if funcName == "apply_patch" {
		fallbackKey = "patch"
	} else if funcName == "update_plan" {
		fallbackKey = "steps"
	}

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		attrStr := strings.TrimSpace(match[1])
		content := strings.TrimSpace(match[2])

		// 清洗开头的 "=" 字符
		if strings.HasPrefix(attrStr, "=") {
			attrStr = strings.TrimSpace(attrStr[1:])
		}

		// 尝试从属性中提取键名
		// 模式：匹配 `name="cmd"` 或是 `=cmd` 或是 `="cmd"` 或者是 `'cmd'`
		keyName := ""
		keyRegex := regexp.MustCompile(`(?:name\s*=\s*|=(?:\s*))["']?([a-zA-Z0-9_\-]+)["']?`)
		keyMatches := keyRegex.FindStringSubmatch(attrStr)
		if len(keyMatches) >= 2 {
			keyName = keyMatches[1]
		} else {
			// 如果没有 `=` 或 `name=`，整个属性就是带引号或不带引号的单词 (例如 <parameter "cmd"> 或 <parameter cmd>)
			cleanAttrRegex := regexp.MustCompile(`^["']?([a-zA-Z0-9_\-]+)["']?$`)
			cleanMatches := cleanAttrRegex.FindStringSubmatch(attrStr)
			if len(cleanMatches) >= 2 {
				keyName = cleanMatches[1]
			}
		}

		// 如果完全提取不出键名
		if keyName == "" {
			// 如果 content 为空且 attrStr 类似 json
			if content == "" && (strings.HasPrefix(attrStr, "[") || strings.HasPrefix(attrStr, "{")) {
				var jsonVal interface{}
				if err := json.Unmarshal([]byte(attrStr), &jsonVal); err == nil {
					if funcName == "update_plan" {
						argsMap["steps"] = jsonVal
					} else {
						argsMap["arguments"] = jsonVal
					}
					continue
				}
			}

			// fallback 默认字段名
			if fallbackKey != "" {
				keyName = fallbackKey
			} else {
				keyName = "value" // 通用兜底
			}
		}

		// 移除键名中两端多余的单双引号
		if (strings.HasPrefix(keyName, "\"") && strings.HasSuffix(keyName, "\"")) ||
			(strings.HasPrefix(keyName, "'") && strings.HasSuffix(keyName, "'")) {
			keyName = keyName[1 : len(keyName)-1]
		}

		if funcName == "update_plan" {
			if keyName == "explanation" {
				argsMap["explanation"] = content
			} else if strings.HasPrefix(keyName, "[") || strings.HasPrefix(content, "[") {
				var jsonVal interface{}
				valStr := content
				if valStr == "" {
					valStr = attrStr
				}
				if err := json.Unmarshal([]byte(valStr), &jsonVal); err == nil {
					argsMap["steps"] = jsonVal
				} else {
					argsMap["steps"] = valStr
				}
			} else {
				if argsMap["explanation"] == nil {
					argsMap["explanation"] = content
				} else {
					argsMap["steps"] = content
				}
			}
		} else if funcName == "apply_patch" {
			if strings.Contains(content, "*** Begin Patch") {
				argsMap["patch"] = content
			} else if strings.Contains(attrStr, "*** Begin Patch") {
				argsMap["patch"] = attrStr
			} else if keyName == "explanation" {
				argsMap["explanation"] = content
			} else if keyName == "patch" {
				if content != "" {
					argsMap["patch"] = content
				} else {
					argsMap["patch"] = attrStr
				}
			} else {
				if argsMap["patch"] == nil {
					argsMap["patch"] = content
				} else {
					argsMap["explanation"] = content
				}
			}
		} else if funcName == "exec_command" {
			if keyName == "explanation" {
				argsMap["explanation"] = content
			} else if keyName == "cmd" {
				if content != "" {
					argsMap["cmd"] = content
				} else {
					argsMap["cmd"] = attrStr
				}
			} else {
				if argsMap["cmd"] == nil {
					if content != "" {
						argsMap["cmd"] = content
					} else {
						argsMap["cmd"] = attrStr
					}
				} else {
					argsMap["explanation"] = content
				}
			}
		} else {
			argsMap[keyName] = content
		}
	}

	// 针对核心参数缺失时的终极兜底逻辑，剥离所有 XML/HTML 标签提取纯文本
	if funcName == "exec_command" && (argsMap["cmd"] == nil || argsMap["cmd"] == "") {
		argsMap["cmd"] = stripXMLTags(xmlStr)
	} else if funcName == "apply_patch" && (argsMap["patch"] == nil || argsMap["patch"] == "") {
		argsMap["patch"] = stripXMLTags(xmlStr)
	} else if funcName == "update_plan" && (argsMap["steps"] == nil || argsMap["steps"] == "") {
		plainText := stripXMLTags(xmlStr)
		var jsonVal interface{}
		if err := json.Unmarshal([]byte(plainText), &jsonVal); err == nil {
			argsMap["steps"] = jsonVal
		} else {
			argsMap["steps"] = plainText
		}
	}

	argBytes, err := json.Marshal(argsMap)
	if err != nil {
		return nil, err
	}

	return &ParsedToolCall{
		Name:      funcName,
		Arguments: string(argBytes),
	}, nil
}

type ToolCallStreamParser struct {
	buffer       strings.Builder
	intercepting bool
}

func NewToolCallStreamParser() *ToolCallStreamParser {
	return &ToolCallStreamParser{}
}

func (p *ToolCallStreamParser) Feed(txt string) (string, *ParsedToolCall) {
	p.buffer.WriteString(txt)
	fullBuf := p.buffer.String()

	if !p.intercepting {
		idx := strings.Index(fullBuf, "<tool_call")
		if idx != -1 {
			p.intercepting = true
			plainText := fullBuf[:idx]
			p.buffer.Reset()
			p.buffer.WriteString(fullBuf[idx:])
			return plainText, nil
		} else {
			if len(fullBuf) > 15 {
				sendLen := len(fullBuf) - 15
				plainText := fullBuf[:sendLen]
				p.buffer.Reset()
				p.buffer.WriteString(fullBuf[sendLen:])
				return plainText, nil
			}
			return "", nil
		}
	} else {
		endIdx := strings.Index(fullBuf, "</tool_call>")
		if endIdx != -1 {
			tagEnd := endIdx + len("</tool_call>")
			xmlChunk := fullBuf[:tagEnd]

			p.intercepting = false
			p.buffer.Reset()
			p.buffer.WriteString(fullBuf[tagEnd:])

			parsed, err := parseXMLToolCall(xmlChunk)
			if err != nil {
				return xmlChunk, nil
			}
			return "", parsed
		}
		return "", nil
	}
}

func (p *ToolCallStreamParser) Flush() string {
	remaining := p.buffer.String()
	p.buffer.Reset()
	return remaining
}

func sendPlainResponsesText(gctx *core.GatewayContext, respID string, txt string, msgID string, messageAdded *bool, textOutputIndex *int, currentOutputIndex *int) error {
	if !*messageAdded {
		*messageAdded = true
		*textOutputIndex = *currentOutputIndex
		// 3. response.output_item.added (文本 message 类型)
		var evItemAdded responseOutputItemAddedEvent
		evItemAdded.Type = "response.output_item.added"
		evItemAdded.ResponseID = respID
		evItemAdded.OutputIndex = *textOutputIndex
		evItemAdded.Item.ID = msgID
		evItemAdded.Item.Type = "message"
		evItemAdded.Item.Status = "in_progress"
		evItemAdded.Item.Role = "assistant"
		evItemAdded.Item.Content = []interface{}{}
		if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.added", evItemAdded); err != nil {
			return err
		}

		// 4. response.content_part.added (output_text 类型)
		var evPartAdded responseContentPartAddedEvent
		evPartAdded.Type = "response.content_part.added"
		evPartAdded.ResponseID = respID
		evPartAdded.ItemID = msgID
		evPartAdded.OutputIndex = *textOutputIndex
		evPartAdded.ContentIndex = 0
		evPartAdded.Part.Type = "output_text"
		evPartAdded.Part.Text = ""
		evPartAdded.Part.Annotations = []interface{}{}
		if err := writeResponseEvent(gctx.ResponseWriter, "response.content_part.added", evPartAdded); err != nil {
			return err
		}

		*currentOutputIndex++
	}

	var evDelta responseOutputTextDeltaEvent
	evDelta.Type = "response.output_text.delta"
	evDelta.ResponseID = respID
	evDelta.ItemID = msgID
	evDelta.OutputIndex = *textOutputIndex
	evDelta.ContentIndex = 0
	evDelta.Delta = txt

	return writeResponseEvent(gctx.ResponseWriter, "response.output_text.delta", evDelta)
}

func handleResponsesStream(gctx *core.GatewayContext, resp *http.Response) error {
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
	xmlParser := NewToolCallStreamParser()
	buf := make([]byte, 4096)
	started := false

	var fullText strings.Builder
	var lastResponseID string
	var lastModelName string

	messageAdded := false
	textOutputIndex := -1
	currentOutputIndex := 0

	type localToolCall struct {
		ID          string
		Name        string
		Arguments   strings.Builder
		OutputIndex int
		Added       bool
	}
	var localToolCalls = make(map[int]*localToolCall)

	headersSent := false

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			// 1. 嗅探所有帧中的 SSE 错误事件
			events := parser.Feed(buf[:n])
			for _, ev := range events {
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
						return fmt.Errorf("upstream stream returned error event: %s", errMsg)
					}
				}
			}

			if !headersSent {
				// 2. 嗅探第一帧原始数据是不是普通 JSON 错误
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

				// 如果还没发送头部，现在是发送头部并触发首包计时的最佳时机
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
					hasDone = true
					break
				}

				if ev.InputTokens > 0 {
					gctx.InputTokens = ev.InputTokens
				}
				if ev.OutputTokens > 0 {
					gctx.OutputTokens = ev.OutputTokens
				}
				if ev.CachedTokens > 0 {
					gctx.CachedTokens = ev.CachedTokens
				}
				if ev.CacheCreationTokens > 0 {
					gctx.CacheCreationTokens = ev.CacheCreationTokens
				}

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
					gctx.Logger(zap.L()).Warn("[DEBUG-responses-stream] json.Unmarshal failed", zap.String("raw_data", ev.Data), zap.Error(err))
					continue
				}

				if chunk.ID != "" {
					lastResponseID = chunk.ID
					if gctx.Tags == nil {
						gctx.Tags = make(map[string]string)
					}
					respID := chunk.ID
					if strings.HasPrefix(respID, "chatcmpl-") {
						respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
					} else if !strings.HasPrefix(respID, "resp_") {
						respID = "resp_" + respID
					}
					gctx.Tags["response_id"] = respID
				}
				if chunk.Model != "" {
					lastModelName = chunk.Model
					if gctx.Tags == nil {
						gctx.Tags = make(map[string]string)
					}
					gctx.Tags["response_model"] = chunk.Model
				}

				respID := lastResponseID
				if strings.HasPrefix(respID, "chatcmpl-") {
					respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
				} else if respID == "" {
					respID = "resp_mock"
				} else if !strings.HasPrefix(respID, "resp_") {
					respID = "resp_" + respID
				}

				msgID := lastResponseID
				if strings.HasPrefix(msgID, "chatcmpl-") {
					msgID = strings.Replace(msgID, "chatcmpl-", "msg_", 1)
				} else if msgID == "" {
					msgID = "msg_mock"
				} else if !strings.HasPrefix(msgID, "msg_") {
					msgID = "msg_" + msgID
				}

				modelName := lastModelName
				if modelName == "" {
					modelName = gctx.Model
				}

				if !started {
					started = true
					now := time.Now().Unix()

					// 1. response.created
					var evCreated responseCreatedEvent
					evCreated.Type = "response.created"
					evCreated.Response.ID = respID
					evCreated.Response.Object = "response"
					evCreated.Response.CreatedAt = now
					evCreated.Response.Status = "in_progress"
					evCreated.Response.Model = modelName
					evCreated.Response.Output = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.created", evCreated); err != nil {
						return err
					}

					// 2. response.in_progress
					var evInProgress responseInProgressEvent
					evInProgress.Type = "response.in_progress"
					evInProgress.Response.ID = respID
					evInProgress.Response.Object = "response"
					evInProgress.Response.CreatedAt = now
					evInProgress.Response.Status = "in_progress"
					evInProgress.Response.Model = modelName
					evInProgress.Response.Output = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.in_progress", evInProgress); err != nil {
						return err
					}

					// 2.1 提前无条件发送 response.output_item.added (message) 以便客户端渲染层完成视图元素 ID 的注册
					messageAdded = true
					textOutputIndex = currentOutputIndex
					currentOutputIndex++

					var evItemAdded responseOutputItemAddedEvent
					evItemAdded.Type = "response.output_item.added"
					evItemAdded.ResponseID = respID
					evItemAdded.OutputIndex = textOutputIndex
					evItemAdded.Item.ID = msgID
					evItemAdded.Item.Type = "message"
					evItemAdded.Item.Status = "in_progress"
					evItemAdded.Item.Role = "assistant"
					evItemAdded.Item.Content = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.added", evItemAdded); err != nil {
						return err
					}

					// 2.2 提前发送 response.content_part.added (output_text)
					var evPartAdded responseContentPartAddedEvent
					evPartAdded.Type = "response.content_part.added"
					evPartAdded.ResponseID = respID
					evPartAdded.ItemID = msgID
					evPartAdded.OutputIndex = textOutputIndex
					evPartAdded.ContentIndex = 0
					evPartAdded.Part.Type = "output_text"
					evPartAdded.Part.Text = ""
					evPartAdded.Part.Annotations = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.content_part.added", evPartAdded); err != nil {
						return err
					}

					if hasFlusher {
						flusher.Flush()
					}
				}

				if len(chunk.Choices) > 0 {
					choice := chunk.Choices[0]

					// 处理文本
					txt := choice.Delta.Content
					if txt != "" {
						plainText, parsedTool := xmlParser.Feed(txt)
						if plainText != "" {
							if err := sendPlainResponsesText(gctx, respID, plainText, msgID, &messageAdded, &textOutputIndex, &currentOutputIndex); err != nil {
								return err
							}
							fullText.WriteString(plainText)
							gctx.TransmittedChars += len(plainText)
						}

						if parsedTool != nil {
							tcIdx := len(localToolCalls)
							localTC := &localToolCall{}
							localTC.ID = fmt.Sprintf("call_%s_xml_%d", msgID, tcIdx)
							localTC.Name = parsedTool.Name
							localTC.OutputIndex = currentOutputIndex
							currentOutputIndex++
							localTC.Added = true
							localTC.Arguments.WriteString(parsedTool.Arguments)
							localToolCalls[tcIdx] = localTC

							// 发送 function_call added
							var evTCAdded responseOutputItemAddedFunctionCallEvent
							evTCAdded.Type = "response.output_item.added"
							evTCAdded.ResponseID = respID
							evTCAdded.OutputIndex = localTC.OutputIndex
							evTCAdded.Item.ID = localTC.ID
							evTCAdded.Item.Type = "function_call"
							evTCAdded.Item.Status = "in_progress"
							evTCAdded.Item.Name = localTC.Name
							evTCAdded.Item.Arguments = ""
							if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.added", evTCAdded); err != nil {
								return err
							}

							// 发送 function_call arguments.delta
							var evTCDelta responseFunctionCallArgumentsDeltaEvent
							evTCDelta.Type = "response.function_call.arguments.delta"
							evTCDelta.ResponseID = respID
							evTCDelta.ItemID = localTC.ID
							evTCDelta.OutputIndex = localTC.OutputIndex
							evTCDelta.Delta = parsedTool.Arguments
							if err := writeResponseEvent(gctx.ResponseWriter, "response.function_call.arguments.delta", evTCDelta); err != nil {
								return err
							}
						}
					}

					// 处理工具调用
					if len(choice.Delta.ToolCalls) > 0 {
						for _, tc := range choice.Delta.ToolCalls {
							localTC, exist := localToolCalls[tc.Index]
							if !exist {
								localTC = &localToolCall{}
								if tc.ID != "" {
									localTC.ID = tc.ID
								} else {
									localTC.ID = fmt.Sprintf("call_%s_%d", msgID, tc.Index)
								}
								localTC.Name = tc.Function.Name
								localTC.OutputIndex = currentOutputIndex
								currentOutputIndex++
								localToolCalls[tc.Index] = localTC
							}
							if tc.Function.Name != "" && localTC.Name == "" {
								localTC.Name = tc.Function.Name
							}

							// 只要有了工具名，且还未发送过 added，就立刻发送 added 事件
							if localTC.Name != "" && !localTC.Added {
								localTC.Added = true
								var evTCAdded responseOutputItemAddedFunctionCallEvent
								evTCAdded.Type = "response.output_item.added"
								evTCAdded.ResponseID = respID
								evTCAdded.OutputIndex = localTC.OutputIndex
								evTCAdded.Item.ID = localTC.ID
								evTCAdded.Item.Type = "function_call"
								evTCAdded.Item.Status = "in_progress"
								evTCAdded.Item.Name = localTC.Name
								evTCAdded.Item.Arguments = ""
								if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.added", evTCAdded); err != nil {
									return err
								}
							}

							argDelta := tc.Function.Arguments
							if argDelta != "" {
								localTC.Arguments.WriteString(argDelta)

								// 发送 arguments delta
								var evTCDelta responseFunctionCallArgumentsDeltaEvent
								evTCDelta.Type = "response.function_call.arguments.delta"
								evTCDelta.ResponseID = respID
								evTCDelta.ItemID = localTC.ID
								evTCDelta.OutputIndex = localTC.OutputIndex
								evTCDelta.Delta = argDelta
								if err := writeResponseEvent(gctx.ResponseWriter, "response.function_call.arguments.delta", evTCDelta); err != nil {
									return err
								}
							}
						}
					}

					if hasFlusher {
						flusher.Flush()
					}
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

	if started {
		respID := lastResponseID
		if strings.HasPrefix(respID, "chatcmpl-") {
			respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
		} else if respID == "" {
			respID = "resp_mock"
		} else if !strings.HasPrefix(respID, "resp_") {
			respID = "resp_" + respID
		}

		msgID := lastResponseID
		if strings.HasPrefix(msgID, "chatcmpl-") {
			msgID = strings.Replace(msgID, "chatcmpl-", "msg_", 1)
		} else if msgID == "" {
			msgID = "msg_mock"
		} else if !strings.HasPrefix(msgID, "msg_") {
			msgID = "msg_" + msgID
		}

		remainingText := xmlParser.Flush()
		if remainingText != "" {
			if err := sendPlainResponsesText(gctx, respID, remainingText, msgID, &messageAdded, &textOutputIndex, &currentOutputIndex); err != nil {
				return err
			}
			fullText.WriteString(remainingText)
			gctx.TransmittedChars += len(remainingText)
		}

		modelName := lastModelName
		if modelName == "" {
			modelName = gctx.Model
		}

		now := time.Now().Unix()

		// 结束文本消息
		if messageAdded {
			finalText := fullText.String()

			// 5. response.output_text.done
			var evTextDone responseOutputTextDoneEvent
			evTextDone.Type = "response.output_text.done"
			evTextDone.ResponseID = respID
			evTextDone.ItemID = msgID
			evTextDone.OutputIndex = textOutputIndex
			evTextDone.ContentIndex = 0
			evTextDone.Text = finalText
			if err := writeResponseEvent(gctx.ResponseWriter, "response.output_text.done", evTextDone); err != nil {
				return err
			}

			// 6. response.content_part.done
			var evPartDone responseContentPartDoneEvent
			evPartDone.Type = "response.content_part.done"
			evPartDone.ResponseID = respID
			evPartDone.ItemID = msgID
			evPartDone.OutputIndex = textOutputIndex
			evPartDone.ContentIndex = 0
			evPartDone.Part.Type = "output_text"
			evPartDone.Part.Text = finalText
			evPartDone.Part.Annotations = []interface{}{}
			if err := writeResponseEvent(gctx.ResponseWriter, "response.content_part.done", evPartDone); err != nil {
				return err
			}

			// 7. response.output_item.done
			var evItemDone responseOutputItemDoneEvent
			evItemDone.Type = "response.output_item.done"
			evItemDone.ResponseID = respID
			evItemDone.OutputIndex = textOutputIndex
			evItemDone.Item.ID = msgID
			evItemDone.Item.Type = "message"
			evItemDone.Item.Status = "completed"
			evItemDone.Item.Role = "assistant"
			evItemDone.Item.Content = []interface{}{
				map[string]interface{}{
					"type":        "output_text",
					"text":        finalText,
					"annotations": []interface{}{},
				},
			}
			if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.done", evItemDone); err != nil {
				return err
			}
		}

		// 结束工具调用
		var outputs []interface{}
		outputs = append(outputs, map[string]interface{}{
			"id":     msgID,
			"type":   "message",
			"status": "completed",
			"role":   "assistant",
			"content": []interface{}{
				map[string]interface{}{
					"type":        "output_text",
					"text":        fullText.String(),
					"annotations": []interface{}{},
				},
			},
		})

		// 按照 index 顺序遍历工具调用完成事件
		var indices []int
		for idx := range localToolCalls {
			indices = append(indices, idx)
		}
		sort.Ints(indices)

		for _, idx := range indices {
			tc := localToolCalls[idx]
			finalArgs := tc.Arguments.String()

			// 发送 arguments done
			var evTCDone responseFunctionCallArgumentsDoneEvent
			evTCDone.Type = "response.function_call.arguments.done"
			evTCDone.ResponseID = respID
			evTCDone.ItemID = tc.ID
			evTCDone.OutputIndex = tc.OutputIndex
			evTCDone.Arguments = finalArgs
			if err := writeResponseEvent(gctx.ResponseWriter, "response.function_call.arguments.done", evTCDone); err != nil {
				return err
			}

			// 发送 output_item.done
			var evTCItemDone responseOutputItemDoneFunctionCallEvent
			evTCItemDone.Type = "response.output_item.done"
			evTCItemDone.ResponseID = respID
			evTCItemDone.OutputIndex = tc.OutputIndex
			evTCItemDone.Item.ID = tc.ID
			evTCItemDone.Item.Type = "function_call"
			evTCItemDone.Item.Status = "completed"
			evTCItemDone.Item.Name = tc.Name
			evTCItemDone.Item.Arguments = finalArgs
			if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.done", evTCItemDone); err != nil {
				return err
			}

			outputs = append(outputs, map[string]interface{}{
				"id":        tc.ID,
				"type":      "function_call",
				"status":    "completed",
				"name":      tc.Name,
				"arguments": finalArgs,
			})
		}

		// 8. response.done
		var evCompleted responseDoneEvent
		evCompleted.Type = "response.done"
		evCompleted.Response.ID = respID
		evCompleted.Response.Object = "response"
		evCompleted.Response.CreatedAt = now - 1
		evCompleted.Response.Status = "completed"
		evCompleted.Response.Model = modelName
		evCompleted.Response.Output = outputs
		evCompleted.Response.Usage.InputTokens = gctx.InputTokens
		evCompleted.Response.Usage.OutputTokens = gctx.OutputTokens
		evCompleted.Response.Usage.TotalTokens = gctx.InputTokens + gctx.OutputTokens
		if err := writeResponseEvent(gctx.ResponseWriter, "response.done", evCompleted); err != nil {
			return err
		}

		// 同时发送 response.completed 兼容旧客户端，避免死等超时
		evCompleted.Type = "response.completed"
		if err := writeResponseEvent(gctx.ResponseWriter, "response.completed", evCompleted); err != nil {
			return err
		}

		if gctx.Tags == nil {
			gctx.Tags = make(map[string]string)
		}
		gctx.Tags["response_completed_sent"] = "true"

		// 发送 data: [DONE] 以显式结束客户端的 SSE 监听
		_, _ = fmt.Fprintf(gctx.ResponseWriter, "data: [DONE]\n\n")

		if hasFlusher {
			flusher.Flush()
		}
	}

	return nil
}

func buildStandardFunctionTool(toolMap map[string]interface{}) map[string]interface{} {
	if fnObj, ok := toolMap["function"].(map[string]interface{}); ok {
		return map[string]interface{}{
			"type":     "function",
			"function": fnObj,
		}
	}

	name, _ := toolMap["name"].(string)
	if name == "" {
		return nil
	}

	fnMap := make(map[string]interface{})
	fnMap["name"] = name
	if desc, ok := toolMap["description"].(string); ok {
		fnMap["description"] = desc
	}
	if params, ok := toolMap["parameters"]; ok {
		fnMap["parameters"] = params
	}
	if strict, ok := toolMap["strict"]; ok {
		fnMap["strict"] = strict
	}

	return map[string]interface{}{
		"type":     "function",
		"function": fnMap,
	}
}

func stripXMLTags(xmlStr string) string {
	tagRegex := regexp.MustCompile(`<[^>]*>`)
	cleaned := tagRegex.ReplaceAllString(xmlStr, "")
	return strings.TrimSpace(cleaned)
}
