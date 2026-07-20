package providers

import (
	"context"
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
	core.RegisterRequestInvoker(core.ProviderAnthropic, core.RequestTypeResponses, &anthropicResponsesInvoker{})
}

// AnthropicProvider implements core.Provider, adapting the Anthropic Messages API.
// Translates between OpenAI-compatible format and Anthropic native format.
type AnthropicProvider struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
}

// NewAnthropicProvider creates an Anthropic provider instance.
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

// RequestTypes returns the request types supported by this provider.
// Responses is served via protocol translation (Responses <-> Messages).
func (p *AnthropicProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{core.RequestTypeMessages, core.RequestTypeResponses}
}

// HealthCheck probes upstream reachability via POST /v1/messages.
func (p *AnthropicProvider) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/messages",
		strings.NewReader(`{"model":"claude-sonnet-4-20250514","max_tokens":1,"messages":[{"role":"user","content":"ping"}]}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", p.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	// For non-official Anthropic domains with a non-empty apiKey, automatically add Authorization: Bearer <key>
	// to support third-party providers (e.g. SenseTime/Sensenova) that use the Anthropic protocol but OpenAI auth.
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

// Invoke handles Anthropic Messages API requests, delegating to the corresponding RequestInvoker handler.
func (p *AnthropicProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

// handleMessagesNonStream transparently passes through non-streaming responses, extracting only tokens for billing.
// Does not fill gctx.Response; only fills gctx.UpstreamBody, and the Engine fallback logic writes it out.
func (p *AnthropicProvider) handleMessagesNonStream(gctx *core.GatewayContext, resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	gctx.TriggerFirstByte()
	gctx.UpstreamBody = body

	in, out, cached, cacheCreated := llm.AnthropicTokenExtractor(string(body))
	llm.ApplyUsage(gctx, in, out, cached, cacheCreated)
	return nil
}

// handleMessagesStream transparently passes through SSE streams; InterceptWriter uses AnthropicTokenExtractor for token extraction.
func (p *AnthropicProvider) handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
	return llm.PassthroughStream(gctx, resp, llm.AnthropicTokenExtractor)
}

// Compile-time interface assertion
var _ core.Provider = (*AnthropicProvider)(nil)
