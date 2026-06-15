package inbound

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

// ---------- 辅助函数 ----------

// newTestGatewayContext 创建用于测试的 GatewayContext
func newTestGatewayContext(method, path string, headers map[string]string) *core.GatewayContext {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return &core.GatewayContext{
		Ctx:     context.Background(),
		Request: req,
	}
}

// getHTTPErrorCode 从 error 中提取 HTTPError 的状态码
func getHTTPErrorCode(err error) int {
	if httpErr, ok := err.(*HTTPError); ok {
		return httpErr.Code
	}
	if limitErr, ok := err.(*limiter.HTTPError); ok {
		return limitErr.Code
	}
	return 0
}

// ---------- AuthFilter 测试 ----------

func TestAuthFilter_Authorized(t *testing.T) {
	f := NewAuthFilter()

	gctx := &core.GatewayContext{
		UserID: "user-001",
		Model:  "gpt-4",
		Policy: &policy.Policy{
			Permissions: []string{"gpt-*"},
		},
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestAuthFilter_Unauthorized(t *testing.T) {
	f := NewAuthFilter()

	gctx := &core.GatewayContext{
		UserID: "user-001",
		Model:  "claude-3-opus",
		Policy: &policy.Policy{
			Permissions: []string{"gpt-*"},
		},
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for unauthorized model")
	}
	if getHTTPErrorCode(err) != http.StatusForbidden {
		t.Errorf("expected status 403 Forbidden, got %d", getHTTPErrorCode(err))
	}
}

func TestAuthFilter_MissingUser(t *testing.T) {
	f := NewAuthFilter()

	gctx := &core.GatewayContext{
		UserID: "",
		Model:  "gpt-4",
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for missing user")
	}
	if getHTTPErrorCode(err) != http.StatusUnauthorized {
		t.Errorf("expected status 401 Unauthorized, got %d", getHTTPErrorCode(err))
	}
}

func TestAuthFilter_MissingPolicy(t *testing.T) {
	f := NewAuthFilter()

	gctx := &core.GatewayContext{
		UserID: "user-001",
		Model:  "gpt-4",
		Policy: nil,
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for missing policy")
	}
	if getHTTPErrorCode(err) != http.StatusForbidden {
		t.Errorf("expected status 403 Forbidden, got %d", getHTTPErrorCode(err))
	}
}

func TestAuthFilter_NameAndOrder(t *testing.T) {
	f := NewAuthFilter()
	if f.Name() != "auth" {
		t.Errorf("expected name 'auth', got '%s'", f.Name())
	}
	if f.Order() != 10 {
		t.Errorf("expected order 10, got %d", f.Order())
	}
}

// ---------- ValidateFilter 测试 ----------

func TestValidateFilter_ValidModel(t *testing.T) {
	knownModels := map[string]bool{"gpt-4": true, "claude-3-opus": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{Model: "gpt-4"}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error for valid model, got: %v", err)
	}
}

func TestValidateFilter_UnknownModel(t *testing.T) {
	knownModels := map[string]bool{"gpt-4": true, "claude-3-opus": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{Model: "unknown-model"}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for unknown model")
	}
	if getHTTPErrorCode(err) != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, getHTTPErrorCode(err))
	}
	if !strings.Contains(err.Error(), "unknown model: unknown-model") {
		t.Errorf("expected 'unknown model: unknown-model' in error, got '%s'", err.Error())
	}
}

func TestValidateFilter_EmptyModel(t *testing.T) {
	knownModels := map[string]bool{"gpt-4": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{Model: ""}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for empty model")
	}
	if getHTTPErrorCode(err) != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, getHTTPErrorCode(err))
	}
	if !strings.Contains(err.Error(), "model is required") {
		t.Errorf("expected 'model is required' in error, got '%s'", err.Error())
	}
}

func TestValidateFilter_NameAndOrder(t *testing.T) {
	f := NewValidateFilter(nil)
	if f.Name() != "validate" {
		t.Errorf("expected name 'validate', got '%s'", f.Name())
	}
	if f.Order() != 30 {
		t.Errorf("expected order 30, got %d", f.Order())
	}
}

// ---------- RateLimitFilter 测试 ----------

func TestRateLimitFilter_NoPolicyMatch(t *testing.T) {
	ss := store.NewMemoryStateStore()
	f := NewRateLimitFilter(ss)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-4"}`))
	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		Request: req,
		Model:   "gpt-4",
		APIKey:  "sk-test",
		RawBody: []byte(`{"model":"gpt-4"}`),
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error when no policy matches, got: %v", err)
	}
}

func TestRateLimitFilter_WithinLimit(t *testing.T) {
	policyVal := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "qps-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 10000, TimeWindowInMs: 1000},
				},
			},
		},
	}
	ss := store.NewMemoryStateStore()
	f := NewRateLimitFilter(ss)

	body := []byte(`{"model":"gpt-4","messages":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		Request: req,
		Model:   "gpt-4",
		RawBody: body,
		Policy:  policyVal,
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error within rate limit, got: %v", err)
	}
}

type mockExceededStateStore struct {
	core.StateStore
}

func (m *mockExceededStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	return 1, nil
}

func (m *mockExceededStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	return nil
}

func TestRateLimitFilter_Exceeded(t *testing.T) {
	ss := &mockExceededStateStore{}
	f := NewRateLimitFilter(ss)

	policyVal := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "qps-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 0, TimeWindowInMs: 1000},
				},
			},
		},
	}

	body := []byte(`{"model":"gpt-4"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		Request: req,
		Model:   "gpt-4",
		RawBody: body,
		Policy:  policyVal,
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected rate limit exceeded error")
	}
	if getHTTPErrorCode(err) != http.StatusTooManyRequests {
		t.Errorf("expected status 429, got %d", getHTTPErrorCode(err))
	}
}

func TestRateLimitFilter_NameAndOrder(t *testing.T) {
	f := NewRateLimitFilter(nil)
	if f.Name() != "rate_limit" {
		t.Errorf("expected name 'rate_limit', got '%s'", f.Name())
	}
	if f.Order() != 20 {
		t.Errorf("expected order 20, got %d", f.Order())
	}
}

// ---------- HTTPError 测试 ----------

func TestHTTPError_ErrorString(t *testing.T) {
	err := &HTTPError{Code: 401, Message: "unauthorized"}
	if err.Error() != "unauthorized" {
		t.Errorf("expected 'unauthorized', got '%s'", err.Error())
	}
}

// ---------- 接口断言 ----------

func TestInboundFilterInterface(t *testing.T) {
	// 编译期接口断言
	var _ core.InboundFilter = (*AuthFilter)(nil)
	var _ core.InboundFilter = (*RateLimitFilter)(nil)
	var _ core.InboundFilter = (*ValidateFilter)(nil)
}
