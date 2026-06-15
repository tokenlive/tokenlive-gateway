package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"

	"go.uber.org/zap"
)

func init() {
	core.RegisterProviderFactory(core.ProviderOpenAI, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewOpenAIProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeChatCompletion, &openaiChatInvoker{})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeEmbedding, &openaiEmbeddingInvoker{})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeResponses, &openaiResponsesInvoker{})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeMessages, &openaiMessagesInvoker{})
}

const defaultTimeout = 60 * time.Second

// OpenAIProvider 实现 core.Provider 接口，适配 OpenAI 兼容 API。
type OpenAIProvider struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
	models  []string
}

// NewOpenAIProvider 创建 OpenAI provider 实例。
func NewOpenAIProvider(name, baseURL, apiKey string, models []string) *OpenAIProvider {
	return &OpenAIProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{},
		models:  models,
	}
}

func (p *OpenAIProvider) Name() string            { return p.name }
func (p *OpenAIProvider) Type() core.ProviderType { return core.ProviderOpenAI }
func (p *OpenAIProvider) ValidateConfig() error   { return nil }

// RequestTypes 返回该 provider 支持的请求类型列表。
func (p *OpenAIProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{
		core.RequestTypeChatCompletion,
		core.RequestTypeEmbedding,
		core.RequestTypeResponses,
		core.RequestTypeMessages,
	}
}

// HealthCheck 通过 GET /models 探测上游服务是否可达。
func (p *OpenAIProvider) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

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

// Invoke 根据请求类型分发到对应的 RequestInvoker 处理器。
func (p *OpenAIProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

// doRequest 统一处理 POST 请求（chat completion、embedding 等）。
func (p *OpenAIProvider) doRequest(gctx *core.GatewayContext, endpoint string) error {
	// 动态解析超时，如果没有配置具体首字超时，则使用最大超时时间进行等待
	totalTimeout := 60000
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout > 0 {
		totalTimeout = gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout
	} else if gctx.IsStream {
		totalTimeout = 600000
	}

	firstByteTimeout := totalTimeout
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		p := gctx.Policy.InvocationPolicy.RetryPolicy
		if p.ConnectTimeout > 0 || p.TtftTimeout > 0 {
			firstByteTimeout = p.ConnectTimeout + p.TtftTimeout
		}
	}

	singleCtx, singleCancel := context.WithCancel(gctx.Ctx)
	shouldCancel := true
	defer func() {
		if shouldCancel {
			singleCancel()
		}
	}()

	// 注册首包前定时器
	timer := time.AfterFunc(time.Duration(firstByteTimeout)*time.Millisecond, func() {
		if gctx.TTFT == 0 {
			singleCancel()
		}
	})
	defer timer.Stop()

	gctx.RegisterTTFTimer(func() {
		timer.Stop()
	})

	// 流式请求注入 stream_options 以确保上游在最后一个 SSE chunk 返回 usage 统计
	body := gctx.RawBody
	if gctx.IsStream {
		body = ensureStreamUsage(body)
	}

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	var ua string
	if gctx.Request != nil {
		ua = gctx.Request.Header.Get("User-Agent")
	}
	if ua == "" {
		ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	}
	req.Header.Set("User-Agent", ua)

	if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
		for k, v := range gctx.SelectedEndpoint.Headers {
			req.Header.Set(k, v)
		}
	}

	var endpointID string
	if gctx.SelectedEndpoint != nil {
		endpointID = gctx.SelectedEndpoint.ID
	}
	gctx.Logger(zap.L()).Debug("sending request to upstream with headers",
		zap.String("endpoint_id", endpointID),
		zap.String("url", req.URL.String()),
		zap.Any("headers", req.Header),
	)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("upstream request: %w", err)
	}

	shouldCloseBody := true
	defer func() {
		if shouldCloseBody {
			resp.Body.Close()
		}
	}()

	gctx.UpstreamResponse = resp

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		gctx.UpstreamBody = body
		return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
	}

	if gctx.IsStream {
		idleTimeout := 0
		if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
			idleTimeout = gctx.Policy.InvocationPolicy.RetryPolicy.IdleTimeout
		}
		if idleTimeout > 0 {
			resp.Body = llm.WrapIdleTimeoutReader(resp.Body, time.Duration(idleTimeout)*time.Millisecond, singleCancel)
		}

		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "text/event-stream") {
			body, _ := io.ReadAll(resp.Body)
			gctx.UpstreamBody = body
			return fmt.Errorf("upstream stream request returned non-stream content-type: %s, body: %s", contentType, string(body))
		}
		if gctx.RequestType == core.RequestTypeMessages || (gctx.RequestType == core.RequestTypeResponses && strings.HasSuffix(endpoint, "/chat/completions")) {
			shouldCloseBody = false
			shouldCancel = false
			resp.Body = &cancelReadCloser{
				ReadCloser: resp.Body,
				cancel:     singleCancel,
			}
			return nil
		}
		return handleOpenAIStream(gctx, resp)
	}
	return handleOpenAINonStream(gctx, resp)
}

// handleOpenAIStream 处理 SSE 流式响应，通过 SSEInterceptWriter 拦截字节流并提取 token 统计。
func handleOpenAIStream(gctx *core.GatewayContext, resp *http.Response) error {
	defer func() {
		if r := recover(); r != nil {
			gctx.Logger(zap.L()).Error("[DEBUG-openai-stream] panic captured in handleOpenAIStream",
				zap.Any("panic_info", r),
				zap.String("stack", string(debug.Stack())),
			)
			panic(r)
		}
	}()

	writer := llm.NewSSEInterceptWriter(gctx)

	buf := make([]byte, 4096)
	headersSent := false
	parser := llm.NewSSEParser()
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			// 1. 嗅探所有帧中的 SSE 错误事件
			events := parser.Feed(buf[:n])
			for _, ev := range events {
				if gctx.RequestType == core.RequestTypeResponses && gctx.GetTagValue("response_id") == "" {
					var respChunk struct {
						ResponseID string `json:"response_id"`
						Response   *struct {
							ID    string `json:"id"`
							Model string `json:"model"`
						} `json:"response"`
						Model string `json:"model"`
					}
					cleanData := strings.TrimSpace(ev.Data)
					if strings.HasPrefix(cleanData, "data:") {
						cleanData = strings.TrimSpace(strings.TrimPrefix(cleanData, "data:"))
					}
					if json.Unmarshal([]byte(cleanData), &respChunk) == nil {
						respID := respChunk.ResponseID
						if respID == "" && respChunk.Response != nil {
							respID = respChunk.Response.ID
						}
						if respID != "" {
							if strings.HasPrefix(respID, "chatcmpl-") {
								respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
							} else if !strings.HasPrefix(respID, "resp_") {
								respID = "resp_" + respID
							}
							if gctx.Tags == nil {
								gctx.Tags = make(map[string]string)
							}
							gctx.Tags["response_id"] = respID
						}

						modelName := respChunk.Model
						if modelName == "" && respChunk.Response != nil {
							modelName = respChunk.Response.Model
						}
						if modelName != "" {
							if gctx.Tags == nil {
								gctx.Tags = make(map[string]string)
							}
							gctx.Tags["response_model"] = modelName
						}
					}
				}

				if gctx.RequestType == core.RequestTypeResponses {
					cleanData := strings.TrimSpace(ev.Data)
					if strings.HasPrefix(cleanData, "data:") {
						cleanData = strings.TrimSpace(strings.TrimPrefix(cleanData, "data:"))
					}
					var eventTypeCheck struct {
						Type string `json:"type"`
					}
					if json.Unmarshal([]byte(cleanData), &eventTypeCheck) == nil {
						if eventTypeCheck.Type == "response.done" || eventTypeCheck.Type == "response.completed" {
							if gctx.Tags == nil {
								gctx.Tags = make(map[string]string)
							}
							gctx.Tags["response_completed_sent"] = "true"
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

				// 正常数据，发送头部并开始写入
				writer.Header().Set("Content-Type", "text/event-stream")
				writer.Header().Set("Cache-Control", "no-cache")
				writer.Header().Set("Connection", "keep-alive")
				writer.WriteHeader(http.StatusOK)
				headersSent = true
			}

			if _, werr := writer.Write(buf[:n]); werr != nil {
				return werr
			}
			writer.Flush()
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
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.Header().Set("Cache-Control", "no-cache")
		writer.Header().Set("Connection", "keep-alive")
		writer.WriteHeader(http.StatusOK)
	}

	return nil
}

// handleOpenAINonStream 处理非流式响应，解析 JSON 并提取 usage 信息。
func handleOpenAINonStream(gctx *core.GatewayContext, resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	gctx.TriggerFirstByte()
	gctx.UpstreamBody = body

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	gctx.Response = result

	if usage, ok := result["usage"].(map[string]interface{}); ok {
		if pt, ok := usage["prompt_tokens"].(float64); ok {
			gctx.InputTokens = int(pt)
		}
		if ct, ok := usage["completion_tokens"].(float64); ok {
			gctx.OutputTokens = int(ct)
		}
		if details, ok := usage["prompt_tokens_details"].(map[string]interface{}); ok {
			if cached, ok := details["cached_tokens"].(float64); ok {
				gctx.CachedTokens = int(cached)
			}
		}
	}
	return nil
}

// ensureStreamUsage 在流式请求体中注入 stream_options.include_usage = true，
// 使上游 OpenAI 兼容 API 在最后一个 SSE chunk 中返回 usage 统计。
func ensureStreamUsage(body []byte) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}

	var opts struct {
		IncludeUsage *bool `json:"include_usage"`
	}
	if raw, ok := m["stream_options"]; ok {
		if err := json.Unmarshal(raw, &opts); err == nil && opts.IncludeUsage != nil && *opts.IncludeUsage {
			return body // 已有 include_usage: true，无需修改
		}
	}

	m["stream_options"] = json.RawMessage(`{"include_usage":true}`)
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// 编译期接口断言
var _ core.Provider = (*OpenAIProvider)(nil)

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
