package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"

	"go.uber.org/zap"
)

func init() {
	core.RegisterProviderFactory(core.ProviderGemini, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewGeminiProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderGemini, core.RequestTypeGeminiGenerateContent, &geminiGenerateContentInvoker{})
}

// GeminiProvider 适配 Google Gemini 原生 generateContent 协议。
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
	totalTimeout := 60000
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout > 0 {
		totalTimeout = gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout
	} else if gctx.IsStream {
		totalTimeout = 600000
	}

	firstByteTimeout := totalTimeout
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		policy := gctx.Policy.InvocationPolicy.RetryPolicy
		if policy.ConnectTimeout > 0 || policy.TtftTimeout > 0 {
			firstByteTimeout = policy.ConnectTimeout + policy.TtftTimeout
		}
	}

	singleCtx, singleCancel := context.WithCancelCause(gctx.Ctx)
	defer singleCancel(nil)

	timer := time.AfterFunc(time.Duration(firstByteTimeout)*time.Millisecond, func() {
		if gctx.TTFT == 0 {
			singleCancel(core.ErrGatewayFirstByteTimeout)
		}
	})
	defer timer.Stop()
	gctx.RegisterTTFTimer(func() {
		timer.Stop()
	})

	model := gctx.Model
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.EffectiveModel() != "" {
		model = gctx.SelectedEndpoint.EffectiveModel()
	}
	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, p.generateContentURL(model, gctx.IsStream), bytes.NewReader(gctx.RawBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

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
	if len(gctx.InjectedHeaders) > 0 {
		for k, v := range gctx.InjectedHeaders {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("x-goog-api-key", p.apiKey)

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
		if context.Cause(singleCtx) == core.ErrGatewayFirstByteTimeout {
			return fmt.Errorf("upstream request timeout (gateway policy active disconnect, first byte timeout): %w", err)
		}
		return fmt.Errorf("upstream request: %w", err)
	}
	defer resp.Body.Close()
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
			resp.Body = llm.WrapIdleTimeoutReader(resp.Body, time.Duration(idleTimeout)*time.Millisecond, func() { singleCancel(nil) })
		}

		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "text/event-stream") {
			body, _ := io.ReadAll(resp.Body)
			gctx.UpstreamBody = body
			return fmt.Errorf("upstream stream request returned non-stream content-type: %s, body: %s", contentType, string(body))
		}
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

	var geminiResp struct {
		UsageMetadata *struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal(body, &geminiResp); err == nil && geminiResp.UsageMetadata != nil {
		gctx.InputTokens = geminiResp.UsageMetadata.PromptTokenCount
		gctx.OutputTokens = geminiResp.UsageMetadata.CandidatesTokenCount
	}
	return nil
}

func (p *GeminiProvider) handleGenerateContentStream(gctx *core.GatewayContext, resp *http.Response) error {
	writer := llm.NewSSEInterceptWriter(gctx, llm.WithTokenExtractor(llm.GeminiTokenExtractor))
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

var _ core.Provider = (*GeminiProvider)(nil)
