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
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// mockRateLimitStore 实现 core.StateStore 接口，记录 RateLimitIncr 的 window 参数
type mockRateLimitStore struct {
	core.StateStore // 嵌入接口以满足编译检查

	recordedWindow time.Duration
}

func (m *mockRateLimitStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	m.recordedWindow = window
	return 0, nil
}

func (m *mockRateLimitStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	return nil
}

func (m *mockRateLimitStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0.0001, nil
}

func (m *mockRateLimitStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func TestRateLimitFilter_UsesPolicyWindow(t *testing.T) {
	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "qps-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 100, TimeWindowInMs: 300000}, // 5m = 300,000ms
				},
			},
		},
	}
	mock := &mockRateLimitStore{}
	f := NewRateLimitFilter(mock)

	body := []byte(`{"model":"gpt-4","messages":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		Request: req,
		Model:   "gpt-4",
		RawBody: body,
		Policy:  p,
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if mock.recordedWindow != 5*time.Minute {
		t.Errorf("expected window 5m, got %v", mock.recordedWindow)
	}
}

func TestRateLimitFilter_DefaultWindow(t *testing.T) {
	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "qps-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 100, TimeWindowInMs: 0}, // 使用默认值
				},
			},
		},
	}
	mock := &mockRateLimitStore{}
	f := NewRateLimitFilter(mock)

	body := []byte(`{"model":"gpt-4","messages":[]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		Request: req,
		Model:   "gpt-4",
		RawBody: body,
		Policy:  p,
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if mock.recordedWindow != time.Minute {
		t.Errorf("expected default window 1m, got %v", mock.recordedWindow)
	}
}

func TestRateLimitFilter_ConditionsAutonomyMatch(t *testing.T) {
	mockStore := &mockRateLimitStore{}
	f := NewRateLimitFilter(mockStore)

	t.Run("Match: conditions are satisfied", func(t *testing.T) {
		p := &policy.Policy{
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "conditional-limit",
					Type: "request",
					Conditions: []*matcher.TagCondition{
						{Type: "system", OpType: "EQUAL", Key: "user", Values: []string{"u1"}},
					},
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 100, TimeWindowInMs: 60000},
					},
				},
			},
		}
		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			UserID:  "u1",
			Model:   "gpt-4",
			Policy:  p,
			RawBody: []byte(`{"model":"gpt-4"}`),
		}

		mockStore.recordedWindow = 0
		err := f.OnRequest(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if mockStore.recordedWindow != time.Minute {
			t.Errorf("expected limit policy to match and execute RateLimitIncr, but got recordedWindow = %v", mockStore.recordedWindow)
		}
	})

	t.Run("Not Match: conditions are not satisfied", func(t *testing.T) {
		p := &policy.Policy{
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "conditional-limit",
					Type: "request",
					Conditions: []*matcher.TagCondition{
						{Type: "system", OpType: "EQUAL", Key: "user", Values: []string{"u2"}}, // 不匹配 u1
					},
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 100, TimeWindowInMs: 60000},
					},
				},
			},
		}
		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			UserID:  "u1",
			Model:   "gpt-4",
			Policy:  p,
			RawBody: []byte(`{"model":"gpt-4"}`),
		}

		mockStore.recordedWindow = 0
		err := f.OnRequest(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if mockStore.recordedWindow != 0 {
			t.Errorf("expected limit policy to be skipped, but RateLimitIncr was executed (recordedWindow = %v)", mockStore.recordedWindow)
		}
	})

	t.Run("Wildcard Match: conditions are empty", func(t *testing.T) {
		p := &policy.Policy{
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name:       "wildcard-limit",
					Type:       "request",
					Conditions: nil, // 空条件
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 100, TimeWindowInMs: 60000},
					},
				},
			},
		}
		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			UserID:  "u1",
			Model:   "gpt-4",
			Policy:  p,
			RawBody: []byte(`{"model":"gpt-4"}`),
		}

		mockStore.recordedWindow = 0
		err := f.OnRequest(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if mockStore.recordedWindow != time.Minute {
			t.Errorf("expected empty conditions to match by default, but recordedWindow = %v", mockStore.recordedWindow)
		}
	})

	t.Run("Wildcard Match: conditions contain empty object", func(t *testing.T) {
		p := &policy.Policy{
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "wildcard-limit-empty-obj",
					Type: "request",
					Conditions: []*matcher.TagCondition{
						{Type: "", Key: "", Values: []string{}}, // 实质为空的条件
					},
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 100, TimeWindowInMs: 60000},
					},
				},
			},
		}
		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			UserID:  "u1",
			Model:   "gpt-4",
			Policy:  p,
			RawBody: []byte(`{"model":"gpt-4"}`),
		}

		mockStore.recordedWindow = 0
		err := f.OnRequest(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if mockStore.recordedWindow != time.Minute {
			t.Errorf("expected conditions containing empty object to be ignored and match by default, but recordedWindow = %v", mockStore.recordedWindow)
		}
	})
}

func TestRateLimitFilter_CostLimiter(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:  0.004,
			OutputPrice: 0.008,
		},
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "cost-daily-limit",
				Type: "cost",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 50, TimeWindowInMs: 86400000}, // 50 厘
				},
			},
		},
	}

	mockStore := &mockRateLimitStore{}
	f := NewRateLimitFilter(mockStore)

	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		UserID:  "u1",
		Model:   "gpt-4",
		Policy:  p,
		RawBody: []byte("1234567890"), // 10 bytes -> estimate = 2 tokens -> 2 * 0.004 * 1000 = 8 厘
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if mockStore.recordedWindow != 24*time.Hour {
		t.Errorf("expected 24h window, got %v", mockStore.recordedWindow)
	}

	if gctx.InputTokens != 2 {
		t.Errorf("expected InputTokens 2, got %d", gctx.InputTokens)
	}
}

type mockCascadeStore struct {
	core.StateStore

	incrCalls   map[string]int64
	refundCalls map[string]int64
}

func (m *mockCascadeStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	if m.incrCalls == nil {
		m.incrCalls = make(map[string]int64)
	}
	m.incrCalls[key] += tokens

	if strings.Contains(key, "1h") {
		return 1001, nil // Threshold: 1000 => 超限
	}
	return 1, nil // Threshold: 10 => 不超限
}

func (m *mockCascadeStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	if m.refundCalls == nil {
		m.refundCalls = make(map[string]int64)
	}
	m.refundCalls[key] += tokens
	return nil
}

func TestRateLimitFilter_CascadeRollback(t *testing.T) {
	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "multi-window-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 10, TimeWindowInMs: 60000},     // 1m
					{Threshold: 1000, TimeWindowInMs: 3600000}, // 1h
				},
			},
		},
	}

	mockStore := &mockCascadeStore{}
	f := NewRateLimitFilter(mockStore)

	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		UserID:  "u-cascade",
		Model:   "gpt-4",
		Policy:  p,
		RawBody: []byte(`{"model":"gpt-4"}`),
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected rate limit exceeded error due to 1h window")
	}

	// 验证 1m 窗口虽然自增成功，但由于 1h 超限，最终应该都被 Refund
	if mockStore.refundCalls["u-cascade:gpt-4:multi-window-limit:1m0s"] != 1 {
		t.Errorf("expected 1m window to be refunded once, got %d", mockStore.refundCalls["u-cascade:gpt-4:multi-window-limit:1m0s"])
	}
	if mockStore.refundCalls["u-cascade:gpt-4:multi-window-limit:1h0m0s"] != 1 {
		t.Errorf("expected 1h window to be refunded once, got %d", mockStore.refundCalls["u-cascade:gpt-4:multi-window-limit:1h0m0s"])
	}
}

type mockExecutor struct {
	executeFunc func() error
	callCount   int
}

func (m *mockExecutor) Execute(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	m.callCount++
	return m.executeFunc()
}

func (m *mockExecutor) Refund(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	return nil
}

func TestRateLimitFilter_QueueAndRetry_Success(t *testing.T) {
	mockExec := &mockExecutor{}
	core.DefaultLimitExecutorFactory.Register("test_queue_success", mockExec)

	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name:      "test-queue-policy",
				Type:      "test_queue_success",
				MaxWaitMs: 100,
			},
		},
	}

	// 第一次返回 429 错误，第二次返回 nil
	mockExec.executeFunc = func() error {
		if mockExec.callCount == 1 {
			return &limiter.HTTPError{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"}
		}
		return nil
	}

	f := NewRateLimitFilter(nil)
	gctx := &core.GatewayContext{
		Ctx:    context.Background(),
		Policy: p,
	}

	start := time.Now()
	err := f.OnRequest(gctx)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if mockExec.callCount != 2 {
		t.Errorf("expected executor to be called 2 times, got %d", mockExec.callCount)
	}
	if duration < 20*time.Millisecond {
		t.Errorf("expected queueing duration to be at least 20ms, got %v", duration)
	}
}

func TestRateLimitFilter_QueueAndRetry_Timeout(t *testing.T) {
	mockExec := &mockExecutor{}
	core.DefaultLimitExecutorFactory.Register("test_queue_timeout", mockExec)

	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name:      "test-queue-policy",
				Type:      "test_queue_timeout",
				MaxWaitMs: 50,
			},
		},
	}

	// 总是返回 429 错误
	mockExec.executeFunc = func() error {
		return &limiter.HTTPError{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"}
	}

	f := NewRateLimitFilter(nil)
	gctx := &core.GatewayContext{
		Ctx:    context.Background(),
		Policy: p,
	}

	start := time.Now()
	err := f.OnRequest(gctx)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	httpErr, ok := err.(*limiter.HTTPError)
	if !ok || httpErr.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 error, got %v", err)
	}
	if duration < 40*time.Millisecond || duration > 100*time.Millisecond {
		t.Logf("wait duration is %v", duration)
	}
}

func TestRateLimitFilter_QueueAndRetry_ContextCancel(t *testing.T) {
	mockExec := &mockExecutor{}
	core.DefaultLimitExecutorFactory.Register("test_queue_cancel", mockExec)

	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name:      "test-queue-policy",
				Type:      "test_queue_cancel",
				MaxWaitMs: 200,
			},
		},
	}

	// 总是返回 429 错误
	mockExec.executeFunc = func() error {
		return &limiter.HTTPError{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	f := NewRateLimitFilter(nil)
	gctx := &core.GatewayContext{
		Ctx:    ctx,
		Policy: p,
	}

	// 10ms 后取消 context
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected context cancel error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}
