package providers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
)

func init() {
	core.RegisterProviderFactory(core.ProviderJoyCode, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewJoyCodeProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeChatCompletion, &joycodeChatInvoker{})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeResponses, &joycodeResponsesInvoker{})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeMessages, &joycodeMessagesInvoker{})
}

// JoyCodeProvider 适配京东 JoyCode API。
type JoyCodeProvider struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
	models  []string
}

// NewJoyCodeProvider 创建 JoyCode provider 实例。
func NewJoyCodeProvider(name, baseURL, apiKey string, models []string) *JoyCodeProvider {
	if baseURL == "" {
		baseURL = "http://joycode-api-saas.jd.com"
	}
	return &JoyCodeProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{},
		models:  models,
	}
}

func (p *JoyCodeProvider) Name() string            { return p.name }
func (p *JoyCodeProvider) Type() core.ProviderType { return core.ProviderJoyCode }
func (p *JoyCodeProvider) ValidateConfig() error   { return nil }

func (p *JoyCodeProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{
		core.RequestTypeChatCompletion,
		core.RequestTypeResponses,
		core.RequestTypeMessages,
	}
}

// HealthCheck 探测上游服务是否可达。
func (p *JoyCodeProvider) HealthCheck(ctx context.Context) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "modelList")
	} else {
		endpoint = p.baseURL + "/api/saas/models/v2/modelList"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader("{}"))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("ptKey", p.apiKey)
	req.Header.Set("loginType", getLoginTypeForPtKey(p.apiKey))
	req.Header.Set("x-ms-client-request-id", uuid.NewString())
	req.Header.Set("client", "JoyCodeIDE")
	req.Header.Set("clientVersion", "3.8.61")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return fmt.Errorf("health check failed: status %d", resp.StatusCode)
	}
	return nil
}

func (p *JoyCodeProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

// isAnthropicModel 判断当前模型是否为 Anthropic (Claude) 分支模型
func isAnthropicModel(model string, gctx *core.GatewayContext) bool {
	lower := strings.ToLower(model)
	if strings.Contains(lower, "claude") {
		return true
	}
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.Metadata != nil {
		if val, exists := gctx.SelectedEndpoint.Metadata["adapter"]; exists && val == "anthropic" {
			return true
		}
	}
	return false
}

// ==========================================
// 1. ChatCompletion Invoker
// ==========================================
type joycodeChatInvoker struct{}

func (i *joycodeChatInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("expected *JoyCodeProvider, got %T", p)
	}

	model := gctx.Model
	if model == "" {
		model = "Kimi-K2.6"
	}

	if isAnthropicModel(model, gctx) {
		// 走 Anthropic messages 分支
		return jp.invokeAnthropic(gctx, model)
	}

	// 走普通 OpenAI completions 分支
	return jp.invokeOpenAI(gctx, model)
}

// ==========================================
// 2. Responses Invoker
// ==========================================
type joycodeResponsesInvoker struct{}

func (i *joycodeResponsesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("expected *JoyCodeProvider, got %T", p)
	}

	// 协议降级：把 responses 转换为 chatCompletions 格式
	newBody, err := translateResponsesToChatCompletion(gctx.RawBody)
	if err != nil {
		return fmt.Errorf("translate responses to chat completion: %w", err)
	}
	gctx.RawBody = newBody

	model := gctx.Model
	if model == "" {
		model = "Kimi-K2.6"
	}

	if isAnthropicModel(model, gctx) {
		if err := jp.invokeAnthropic(gctx, model); err != nil {
			return err
		}
	} else {
		if err := jp.invokeOpenAI(gctx, model); err != nil {
			return err
		}
	}

	// 逆向翻译响应体 (OpenAI Chat -> Responses)
	if gctx.IsStream {
		return handleResponsesStream(gctx, gctx.UpstreamResponse)
	} else {
		if err := translateResponsesNonStreamResponse(gctx); err != nil {
			return fmt.Errorf("translate response: %w", err)
		}
	}
	return nil
}

// ==========================================
// 3. Messages Invoker (原生 Anthropic Messages 请求)
// ==========================================
type joycodeMessagesInvoker struct{}

func (i *joycodeMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("expected *JoyCodeProvider, got %T", p)
	}

	mocked, err := llm.TryMockMessagesProbe(gctx)
	if err != nil {
		return err
	}
	if mocked {
		return nil
	}

	model := gctx.Model
	if model == "" {
		model = "claude-3-5-sonnet-v2"
	}

	// 原生 Anthropic 消息透传
	return jp.doAnthropicMessagesDirect(gctx, model)
}

// ==========================================
// JoyCode 核心请求处理逻辑
// ==========================================

// invokeOpenAI 向普通 OpenAI /v2 接口发起请求并处理响应
func (p *JoyCodeProvider) invokeOpenAI(gctx *core.GatewayContext, model string) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "chat_completions")
	} else {
		endpoint = p.baseURL + "/api/saas/openai/v2/chat/completions"
	}

	singleCtx, singleCancel := context.WithCancelCause(gctx.Ctx)
	defer singleCancel(nil)

	reqBody := injectJoyCodePayload(gctx.RawBody)
	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// 鉴权与配置头
	req.Header.Set("ptKey", p.apiKey)
	req.Header.Set("loginType", getLoginTypeForPtKey(p.apiKey))
	req.Header.Set("x-ms-client-request-id", uuid.NewString())
	req.Header.Set("client", "JoyCodeIDE")
	req.Header.Set("clientVersion", "3.8.61")
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	injectHeaders(req, gctx, false)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("upstream request: %w", err)
	}
	gctx.UpstreamResponse = resp

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		gctx.UpstreamBody = body
		resp.Body.Close()
		return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
	}

	if gctx.IsStream {
		// 普通模型直接使用标准的 OpenAI 流解析
		return handleOpenAIStream(gctx, resp)
	}

	// 非流式响应处理
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	gctx.UpstreamBody = body
	gctx.TriggerFirstByte()

	// 统计 Token
	var oaiResp struct {
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &oaiResp); err == nil {
		gctx.InputTokens = oaiResp.Usage.PromptTokens
		gctx.OutputTokens = oaiResp.Usage.CompletionTokens
	}
	return nil
}

// invokeAnthropic 将 OpenAI 请求格式转为 Anthropic messages 格式发往上游，并将响应还原为 OpenAI 格式
func (p *JoyCodeProvider) invokeAnthropic(gctx *core.GatewayContext, model string) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "anthropic_completions")
	} else {
		endpoint = p.baseURL + "/api/saas/anthropic/v1/messages"
	}

	// 1. OpenAI -> Anthropic 请求体转换
	anthropicBody, err := translateOpenAIToAnthropic(gctx.RawBody, model)
	if err != nil {
		return fmt.Errorf("translate openAI to anthropic request: %w", err)
	}
	anthropicBody = adaptThinkingBehavior(anthropicBody)
	anthropicBody = injectJoyCodePayload(anthropicBody)

	singleCtx, singleCancel := context.WithCancelCause(gctx.Ctx)
	defer singleCancel(nil)

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, endpoint, bytes.NewReader(anthropicBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// 鉴权与配置头
	req.Header.Set("ptKey", p.apiKey)
	req.Header.Set("loginType", getLoginTypeForPtKey(p.apiKey))
	req.Header.Set("x-ms-client-request-id", uuid.NewString())
	req.Header.Set("client", "JoyCodeIDE")
	req.Header.Set("clientVersion", "3.8.61")
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	injectHeaders(req, gctx, true)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("upstream request: %w", err)
	}
	gctx.UpstreamResponse = resp

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		gctx.UpstreamBody = body
		resp.Body.Close()
		return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
	}

	if gctx.IsStream {
		// 拦截并做流式翻译 (Anthropic SSE -> OpenAI SSE)
		return p.handleAnthropicStreamToOpenAI(gctx, resp)
	}

	// 非流式响应翻译
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	// 2. Anthropic -> OpenAI 响应转换
	openaiResp, err := translateAnthropicToOpenAINonStream(body, model)
	if err != nil {
		return fmt.Errorf("translate anthropic response: %w", err)
	}

	gctx.UpstreamBody = openaiResp
	gctx.TriggerFirstByte()

	// 统计 Token
	var tokenResp struct {
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(openaiResp, &tokenResp); err == nil {
		gctx.InputTokens = tokenResp.Usage.PromptTokens
		gctx.OutputTokens = tokenResp.Usage.CompletionTokens
	}
	return nil
}

// doAnthropicMessagesDirect 处理原生 Anthropic /messages 请求的转发，不进行格式转换，只注入 Header
func (p *JoyCodeProvider) doAnthropicMessagesDirect(gctx *core.GatewayContext, model string) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "anthropic_completions")
	} else {
		endpoint = p.baseURL + "/api/saas/anthropic/v1/messages"
	}

	singleCtx, singleCancel := context.WithCancelCause(gctx.Ctx)
	defer singleCancel(nil)

	adaptedBody := injectJoyCodePayload(adaptThinkingBehavior(gctx.RawBody))
	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, endpoint, bytes.NewReader(adaptedBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// 鉴权与配置头
	req.Header.Set("ptKey", p.apiKey)
	req.Header.Set("loginType", getLoginTypeForPtKey(p.apiKey))
	req.Header.Set("x-ms-client-request-id", uuid.NewString())
	req.Header.Set("client", "JoyCodeIDE")
	req.Header.Set("clientVersion", "3.8.61")
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	injectHeaders(req, gctx, true)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("upstream request: %w", err)
	}
	gctx.UpstreamResponse = resp

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		gctx.UpstreamBody = body
		resp.Body.Close()
		return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
	}

	if gctx.IsStream {
		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(strings.ToLower(contentType), "text/event-stream") {
			body, _ := io.ReadAll(resp.Body)
			gctx.UpstreamBody = body
			resp.Body.Close()
			return fmt.Errorf("upstream stream request returned non-stream content-type: %s, body: %s", contentType, string(body))
		}
		// 直接使用 Anthropic SSE 透传处理器
		return p.handleMessagesStream(gctx, resp)
	}

	// 非流式直接读取
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	gctx.UpstreamBody = body
	gctx.TriggerFirstByte()

	// 统计 Token
	var anthropicResp struct {
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &anthropicResp); err == nil {
		gctx.InputTokens = anthropicResp.Usage.InputTokens
		gctx.OutputTokens = anthropicResp.Usage.OutputTokens
	}
	return nil
}

// handleMessagesStream 原生透传 Anthropic SSE 格式流
func (p *JoyCodeProvider) handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
	writer := llm.NewSSEInterceptWriter(gctx, llm.WithTokenExtractor(llm.AnthropicTokenExtractor))
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := writer.Write(buf[:n]); werr != nil {
				resp.Body.Close()
				return werr
			}
			writer.Flush()
		}
		if err != nil {
			resp.Body.Close()
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read upstream stream: %w", err)
		}
	}
	return nil
}

// ==========================================
// 4. OpenAI <-> Anthropic 协议翻译函数
// ==========================================

// translateOpenAIToAnthropic 将 OpenAI 的 chat.completions 请求格式转换为 Anthropic messages 格式
func translateOpenAIToAnthropic(rawBody []byte, model string) ([]byte, error) {
	var oaiReq map[string]interface{}
	if err := json.Unmarshal(rawBody, &oaiReq); err != nil {
		return nil, err
	}

	anthropicReq := make(map[string]interface{})
	anthropicReq["model"] = model

	// 1. 提取 System Prompt 并拼接
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
				// 普通 user/assistant/tool 消息处理
				newMsg := make(map[string]interface{})
				newMsg["role"] = role

				if role == "tool" {
					// Convert OpenAI tool -> Anthropic tool_result (role 对应 user)
					newMsg["role"] = "user"
					toolCallID, _ := mMap["tool_call_id"].(string)
					contentStr, _ := content.(string)

					toolResultBlock := map[string]interface{}{
						"type":        "tool_result",
						"tool_use_id": toolCallID,
						"content":     contentStr,
					}

					// 尝试与前一条进行合并以规避连续 user 消息限制
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
					// 转换 tool_calls
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
					// 转换 image_url 格式
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

	// 2. 转换 Tools
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

	// 3. 其他控制参数
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

// translateAnthropicToOpenAINonStream 将 Anthropic 的非流式响应转换为 OpenAI 格式的 JSON
func translateAnthropicToOpenAINonStream(anthropicBody []byte, model string) ([]byte, error) {
	var aResp map[string]interface{}
	if err := json.Unmarshal(anthropicBody, &aResp); err != nil {
		return nil, err
	}

	// 错误处理
	if errMap, exists := aResp["error"].(map[string]interface{}); exists {
		msg, _ := errMap["message"].(string)
		t, _ := errMap["type"].(string)
		return json.Marshal(map[string]interface{}{
			"error": map[string]interface{}{
				"message": msg,
				"type":    t,
				"code":    errMap["code"],
			},
		})
	}

	id, _ := aResp["id"].(string)
	modelName, _ := aResp["model"].(string)
	if modelName == "" {
		modelName = model
	}

	// 提取 text content
	var textContent strings.Builder
	if contentArr, exists := aResp["content"].([]interface{}); exists {
		for _, block := range contentArr {
			blockMap, ok := block.(map[string]interface{})
			if !ok {
				continue
			}
			t, _ := blockMap["type"].(string)
			if t == "text" {
				txt, _ := blockMap["text"].(string)
				textContent.WriteString(txt)
			}
		}
	}

	var inputTokens, outputTokens int
	if usage, exists := aResp["usage"].(map[string]interface{}); exists {
		it, _ := usage["input_tokens"].(float64)
		ot, _ := usage["output_tokens"].(float64)
		inputTokens = int(it)
		outputTokens = int(ot)
	}

	openaiResp := map[string]interface{}{
		"id":      id,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   modelName,
		"choices": []interface{}{
			map[string]interface{}{
				"index": 0,
				"message": map[string]interface{}{
					"role":    "assistant",
					"content": textContent.String(),
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]interface{}{
			"prompt_tokens":     inputTokens,
			"completion_tokens": outputTokens,
			"total_tokens":      inputTokens + outputTokens,
		},
	}

	return json.Marshal(openaiResp)
}

// handleAnthropicStreamToOpenAI 拦截 Anthropic 流式 SSE，实时翻译重组为 OpenAI SSE chunk
func (p *JoyCodeProvider) handleAnthropicStreamToOpenAI(gctx *core.GatewayContext, resp *http.Response) error {
	gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
	gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
	gctx.ResponseWriter.WriteHeader(http.StatusOK)

	flusher, hasFlusher := gctx.ResponseWriter.(http.Flusher)
	reader := bufio.NewReader(resp.Body)
	defer resp.Body.Close()

	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "data:") {
				dataStr := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if dataStr == "[DONE]" {
					_, _ = fmt.Fprint(gctx.ResponseWriter, "data: [DONE]\n\n")
					if hasFlusher {
						flusher.Flush()
					}
					break
				}

				var anthropicVal map[string]interface{}
				if err := json.Unmarshal([]byte(dataStr), &anthropicVal); err == nil {
					// 触发首字节
					gctx.TriggerFirstByte()

					// 统计 Token 追踪
					if usage, exists := anthropicVal["usage"].(map[string]interface{}); exists {
						if ot, ok := usage["output_tokens"].(float64); ok {
							gctx.OutputTokens = int(ot)
						}
						if it, ok := usage["input_tokens"].(float64); ok {
							gctx.InputTokens = int(it)
						}
					}
					if message, exists := anthropicVal["message"].(map[string]interface{}); exists {
						if usage, ok := message["usage"].(map[string]interface{}); ok {
							if it, ok := usage["input_tokens"].(float64); ok {
								gctx.InputTokens = int(it)
							}
						}
					}

					// 提取 Delta 转换
					eventType, _ := anthropicVal["type"].(string)
					switch eventType {
					case "content_block_delta":
						delta, _ := anthropicVal["delta"].(map[string]interface{})
						dt, _ := delta["type"].(string)

						if dt == "thinking_delta" {
							thinking, _ := delta["thinking"].(string)
							openaiChunk := map[string]interface{}{
								"id":      "chatcmpl-joycode-anthropic",
								"object":  "chat.completion.chunk",
								"created": time.Now().Unix(),
								"model":   gctx.Model,
								"choices": []interface{}{
									map[string]interface{}{
										"index": 0,
										"delta": map[string]interface{}{
											"reasoning_content": thinking,
										},
									},
								},
							}
							chunkBytes, _ := json.Marshal(openaiChunk)
							_, _ = fmt.Fprintf(gctx.ResponseWriter, "data: %s\n\n", string(chunkBytes))
						} else if dt == "text_delta" {
							text, _ := delta["text"].(string)
							openaiChunk := map[string]interface{}{
								"id":      "chatcmpl-joycode-anthropic",
								"object":  "chat.completion.chunk",
								"created": time.Now().Unix(),
								"model":   gctx.Model,
								"choices": []interface{}{
									map[string]interface{}{
										"index": 0,
										"delta": map[string]interface{}{
											"content": text,
										},
									},
								},
							}
							chunkBytes, _ := json.Marshal(openaiChunk)
							_, _ = fmt.Fprintf(gctx.ResponseWriter, "data: %s\n\n", string(chunkBytes))
						}
					case "message_delta":
						openaiChunk := map[string]interface{}{
							"id":      "chatcmpl-joycode-anthropic",
							"object":  "chat.completion.chunk",
							"created": time.Now().Unix(),
							"model":   gctx.Model,
							"choices": []interface{}{
								map[string]interface{}{
									"index":         0,
									"delta":         map[string]interface{}{},
									"finish_reason": "stop",
								},
							},
						}
						chunkBytes, _ := json.Marshal(openaiChunk)
						_, _ = fmt.Fprintf(gctx.ResponseWriter, "data: %s\n\n", string(chunkBytes))
					case "message_stop":
						_, _ = fmt.Fprint(gctx.ResponseWriter, "data: [DONE]\n\n")
					case "error":
						errMap, _ := anthropicVal["error"].(map[string]interface{})
						errMsg, _ := errMap["message"].(string)
						openaiErr := map[string]interface{}{
							"error": map[string]interface{}{
								"message": errMsg,
								"type":    "anthropic_error",
							},
						}
						chunkBytes, _ := json.Marshal(openaiErr)
						_, _ = fmt.Fprintf(gctx.ResponseWriter, "data: %s\n\n", string(chunkBytes))
					}
					if hasFlusher {
						flusher.Flush()
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
	return nil
}

func injectHeaders(req *http.Request, gctx *core.GatewayContext, isAnthropic bool) {
	if isAnthropic {
		if gctx.Request != nil {
			if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
				req.Header.Set("anthropic-version", ver)
			} else {
				req.Header.Set("anthropic-version", "2023-06-01")
			}
			if beta := gctx.Request.Header.Get("anthropic-beta"); beta != "" {
				req.Header.Set("anthropic-beta", beta)
			}
		} else {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
	}

	if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
		for k, v := range gctx.SelectedEndpoint.Headers {
			req.Header.Set(k, v)
		}
	}
	if len(gctx.InjectedHeaders) > 0 {
		for k, v := range gctx.InjectedHeaders {
			req.Header.Set(k, v)
		}
	}
}

func adaptThinkingBehavior(body []byte) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}

	delete(m, "thinking")
	delete(m, "output_config")

	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// 编译期接口断言
var _ core.Provider = (*JoyCodeProvider)(nil)

func signJoyCodeGatewayURL(baseURL, functionID string) string {
	baseURL = strings.TrimSuffix(strings.TrimSpace(baseURL), "/")
	t := time.Now().UnixNano() / int64(time.Millisecond)
	stringToSign := fmt.Sprintf("joycode_ide&%s&%d", functionID, t)
	key := []byte("0691a3f0b37b4a85aeb63ad0fc7db3ed")

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(stringToSign))
	sign := hex.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("%s/api?appid=joycode_ide&functionId=%s&t=%d&sign=%s", baseURL, functionID, t, sign)
}

func getLoginTypeForPtKey(ptKey string) string {
	if strings.HasPrefix(ptKey, "BJ.") {
		return "ERP"
	}
	return "N_PIN_PC"
}

func injectJoyCodePayload(rawBody []byte) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(rawBody, &m); err != nil {
		return rawBody
	}
	m["client"] = "JoyCodeIDE"
	m["clientVersion"] = "3.8.61"

	if out, err := json.Marshal(m); err == nil {
		return out
	}
	return rawBody
}


