package providers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/upstream"
)

func init() {
	core.RegisterProviderFactory(core.ProviderGemini, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewGeminiProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderGemini, core.RequestTypeGeminiGenerateContent, &geminiGenerateContentInvoker{})
}

// GeminiProvider adapts the Google Gemini native generateContent protocol.
type GeminiProvider struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
	models  []string
}

func NewGeminiProvider(name, baseURL, apiKey string, models []string) *GeminiProvider {
	return &GeminiProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{},
		models:  models,
	}
}

func (p *GeminiProvider) Name() string            { return p.name }
func (p *GeminiProvider) Type() core.ProviderType { return core.ProviderGemini }
func (p *GeminiProvider) ValidateConfig() error   { return nil }

func (p *GeminiProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{core.RequestTypeGeminiGenerateContent}
}

func (p *GeminiProvider) HealthCheck(ctx context.Context) error {
	model := "gemini-2.5-flash"
	if len(p.models) > 0 && p.models[0] != "" {
		model = p.models[0]
	}
	body := []byte(`{"contents":[{"parts":[{"text":"ping"}]}],"generationConfig":{"maxOutputTokens":1}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.generateContentURL(model, false), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", p.apiKey)

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

func (p *GeminiProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

func (p *GeminiProvider) generateContentURL(model string, stream bool) string {
	method := "generateContent"
	if stream {
		method = "streamGenerateContent"
	}
	u := fmt.Sprintf("%s/models/%s:%s", p.baseURL, url.PathEscape(model), method)
	if stream {
		u += "?alt=sse"
	}
	return u
}

type geminiGenerateContentInvoker struct{}

func (i *geminiGenerateContentInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	gp, ok := p.(*GeminiProvider)
	if !ok {
		return fmt.Errorf("expected *GeminiProvider, got %T", p)
	}
	return gp.doGenerateContent(gctx)
}

func (p *GeminiProvider) doGenerateContent(gctx *core.GatewayContext) error {
	model := gctx.Model
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.EffectiveModel() != "" {
		model = gctx.SelectedEndpoint.EffectiveModel()
	}

	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	h.Set("x-goog-api-key", p.apiKey)

	resp, err := upstream.Call(gctx, upstream.Request{
		Client: p.client,
		URL:    p.generateContentURL(model, gctx.IsStream),
		Body:   gctx.RawBody,
		Header: h,
		Stream: upstream.Consume,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if gctx.IsStream {
		return p.handleGenerateContentStream(gctx, resp)
	}
	return p.handleGenerateContentNonStream(gctx, resp)
}

func (p *GeminiProvider) handleGenerateContentNonStream(gctx *core.GatewayContext, resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	gctx.TriggerFirstByte()
	gctx.UpstreamBody = body

	in, out, cached, cacheCreated := llm.GeminiTokenExtractor(string(body))
	llm.ApplyUsage(gctx, in, out, cached, cacheCreated)
	return nil
}

func (p *GeminiProvider) handleGenerateContentStream(gctx *core.GatewayContext, resp *http.Response) error {
	return llm.PassthroughStream(gctx, resp, llm.GeminiTokenExtractor)
}

var _ core.Provider = (*GeminiProvider)(nil)
