package invoker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// mockProvider 实现 Provider 接口，用于测试
type mockProvider struct {
	name         string
	providerType core.ProviderType
	apis         []core.RequestType
	invokeErr    error
	healthErr    error
	validateErr  error
}

func (m *mockProvider) Name() string                           { return m.name }
func (m *mockProvider) Type() core.ProviderType                { return m.providerType }
func (m *mockProvider) RequestTypes() []core.RequestType       { return m.apis }
func (m *mockProvider) Invoke(gctx *core.GatewayContext) error { return m.invokeErr }
func (m *mockProvider) HealthCheck(ctx context.Context) error  { return m.healthErr }
func (m *mockProvider) ValidateConfig() error                  { return m.validateErr }

// 编译时检查：mockProvider 实现 Provider 接口
var _ core.Provider = (*mockProvider)(nil)

func TestProviderInvoker_Invoke_SetsFields(t *testing.T) {
	provider := &mockProvider{
		name:         "test-openai",
		providerType: core.ProviderOpenAI,
		apis:         []core.RequestType{core.RequestTypeChatCompletion},
	}
	ep := &core.Endpoint{ID: "ep-1", Provider: "openai"}
	pi := NewProviderInvoker(provider, ep)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	before := time.Now()
	err := pi.Invoke(gctx)
	after := time.Now()

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.SelectedInvoker != pi {
		t.Error("expected SelectedInvoker to be set to pi")
	}
	if gctx.SelectedEndpoint != ep {
		t.Errorf("expected SelectedEndpoint to be ep-1, got %v", gctx.SelectedEndpoint)
	}
	if gctx.UpstreamConnect.Before(before) || gctx.UpstreamConnect.After(after) {
		t.Errorf("UpstreamConnect %v not in range [%v, %v]", gctx.UpstreamConnect, before, after)
	}
}

func TestProviderInvoker_Invoke_NilProvider(t *testing.T) {
	ep := &core.Endpoint{ID: "ep-nil", Provider: "unknown"}
	pi := NewProviderInvoker(nil, ep)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	before := time.Now()
	err := pi.Invoke(gctx)
	after := time.Now()

	if err != nil {
		t.Fatalf("expected no error for nil provider, got %v", err)
	}
	if gctx.SelectedInvoker != pi {
		t.Error("expected SelectedInvoker to be set to pi")
	}
	if gctx.SelectedEndpoint != ep {
		t.Errorf("expected SelectedEndpoint to be ep-nil, got %v", gctx.SelectedEndpoint)
	}
	if gctx.UpstreamConnect.Before(before) || gctx.UpstreamConnect.After(after) {
		t.Errorf("UpstreamConnect %v not in range [%v, %v]", gctx.UpstreamConnect, before, after)
	}
}

func TestProviderInvoker_Invoke_ProviderError(t *testing.T) {
	expectedErr := errors.New("upstream unavailable")
	provider := &mockProvider{
		name:      "failing-provider",
		invokeErr: expectedErr,
	}
	ep := &core.Endpoint{ID: "ep-err", Provider: "test"}
	pi := NewProviderInvoker(provider, ep)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	err := pi.Invoke(gctx)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
	if gctx.SelectedInvoker != pi {
		t.Error("expected SelectedInvoker to be set even on error")
	}
}

func TestProviderInterface_Constants(t *testing.T) {
	if core.ProviderOpenAI != "openai" {
		t.Errorf("expected ProviderOpenAI='openai', got %s", core.ProviderOpenAI)
	}
	if core.ProviderAnthropic != "anthropic" {
		t.Errorf("expected ProviderAnthropic='anthropic', got %s", core.ProviderAnthropic)
	}
}

func TestProviderInvoker_CreatedByLB_WithoutProvider(t *testing.T) {
	// 模拟 LB 的典型用法：只设置 Endpoint，不设置 Provider
	ep := &core.Endpoint{ID: "lb-ep", Provider: "openai"}
	pi := NewProviderInvoker(nil, ep)

	if pi.Provider != nil {
		t.Error("expected Provider to be nil when created by LB without setting Provider")
	}

	// Invoke 应该是 no-op，不 panic
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	err := pi.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error for LB-created invoker, got %v", err)
	}
}
