package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
)

func init() {
	core.RegisterProviderFactory(core.ProviderAnthropic, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewAnthropicProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderAnthropic, core.RequestTypeMessages, &anthropicMessagesInvoker{})
}

// AnthropicProvider 实现 core.Provider 接口，适配 Anthropic Messages API。
// 在 OpenAI 兼容格式和 Anthropic 原生格式之间进行转换。
type AnthropicProvider struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
}

// NewAnthropicProvider 创建 Anthropic provider 实例。
func NewAnthropicProvider(name, baseURL, apiKey string, _ []string) *AnthropicProvider {
	return &AnthropicProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{},
	}
}

func (p *AnthropicProvider) Name() string            { return p.name }
func (p *AnthropicProvider) Type() core.ProviderType { return core.ProviderAnthropic }
func (p *AnthropicProvider) ValidateConfig() error   { return nil }

// RequestTypes 返回该 provider 支持的请求类型列表。
func (p *AnthropicProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{core.RequestTypeMessages}
}

// HealthCheck 通过 POST /v1/messages 探测上游服务是否可达。
func (p *AnthropicProvider) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/messages",
		strings.NewReader(`{"model":"claude-sonnet-4-20250514","max_tokens":1,"messages":[{"role":"user","content":"ping"}]}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", p.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	// 如果是非官方 Anthropic 域名且 apiKey 不为空，则自动补充 Authorization: Bearer <key>
	// 以兼容类似于商汤(Sensenova)等使用 Anthropic 协议但采用 OpenAI 鉴权机制的第三方提供商
	if p.apiKey != "" && !strings.Contains(p.baseURL, "anthropic.com") {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}

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

// Invoke 处理 Anthropic Messages API 请求，委托给对应的 RequestInvoker 处理器。
func (p *AnthropicProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

// handleMessagesNonStream 原生透传非流式响应,只提取 token 用于计费。
// 不填 gctx.Response:仅填充 gctx.UpstreamBody,Engine 主流程兜底逻辑自动写出。
func (p *AnthropicProvider) handleMessagesNonStream(gctx *core.GatewayContext, resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	gctx.TriggerFirstByte()
	gctx.UpstreamBody = body

	var anthropicResp struct {
		Usage struct {
			InputTokens              int `json:"input_tokens"`
			OutputTokens             int `json:"output_tokens"`
			CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
			CacheReadInputTokens     int `json:"cache_read_input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &anthropicResp); err == nil {
		u := anthropicResp.Usage
		cached := u.CacheReadInputTokens
		cacheCreated := u.CacheCreationInputTokens
		// Anthropic 的 input_tokens 仅含「未命中缓存的输入」，缓存读取/写入是额外单独计量。
		// 归一化为「总输入」（含缓存读取与缓存写入），对齐 OpenAI 的 prompt_tokens 语义，
		// 使下游计费公式 nonCached = InputTokens - Cached - CacheCreation 对所有 provider 通用。
		gctx.InputTokens = u.InputTokens + cached + cacheCreated
		gctx.OutputTokens = u.OutputTokens
		gctx.CachedTokens = cached
		gctx.CacheCreationTokens = cacheCreated
	}
	return nil
}

// handleMessagesStream 原生透传 SSE 流,InterceptWriter 用 AnthropicTokenExtractor 提取 token。
func (p *AnthropicProvider) handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
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
				return werr
			}
			writer.Flush()
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

// 编译期接口断言
var _ core.Provider = (*AnthropicProvider)(nil)
