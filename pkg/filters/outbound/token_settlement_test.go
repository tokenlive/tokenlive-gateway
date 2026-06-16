package outbound

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type mockSettlementStore struct {
	core.StateStore

	incrCalls   map[string]int64
	refundCalls map[string]int64
}

func (m *mockSettlementStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	if m.incrCalls == nil {
		m.incrCalls = make(map[string]int64)
	}
	m.incrCalls[key] += tokens
	return 10000, nil
}

func (m *mockSettlementStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	if m.refundCalls == nil {
		m.refundCalls = make(map[string]int64)
	}
	m.refundCalls[key] += tokens
	return nil
}

func (m *mockSettlementStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0.0001, nil
}

func (m *mockSettlementStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func TestTokenSettlementFilter_OnResponse(t *testing.T) {
	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-token-ratio",
				Type: "token",
				Estimator: &policy.EstimatorConfig{
					Type:  "length_ratio",
					Ratio: 0.5,
				},
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	t.Run("Actual less than estimated (Refund)", func(t *testing.T) {
		mock := &mockSettlementStore{}
		f := NewTokenSettlementFilter(mock, nil, nil)

		body := []byte("12345678901234567890") // length = 20. ratio = 0.5. estimated = 10.
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
		gctx := &core.GatewayContext{
			Ctx:          context.Background(),
			Request:      req,
			UserID:       "user-123",
			Model:        "gpt-4",
			RawBody:      body,
			Policy:       p,
			InputTokens:  3, // actual total = 3 + 4 = 7
			OutputTokens: 4,
		}

		// Actual = 7, Estimated = 10, diff = 7 - 10 = -3. We expect a Refund of 3 tokens.
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expectedKey := "user-123:gpt-4:policy-token-ratio:1m0s"
		refunded := mock.refundCalls[expectedKey]
		if refunded != 3 {
			t.Errorf("expected 3 refunded tokens, got %d", refunded)
		}
	})

	t.Run("Actual greater than estimated (Incr)", func(t *testing.T) {
		mock := &mockSettlementStore{}
		f := NewTokenSettlementFilter(mock, nil, nil)

		body := []byte("12345678901234567890") // length = 20. ratio = 0.5. estimated = 10.
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
		gctx := &core.GatewayContext{
			Ctx:          context.Background(),
			Request:      req,
			UserID:       "user-123",
			Model:        "gpt-4",
			RawBody:      body,
			Policy:       p,
			InputTokens:  10, // actual total = 10 + 5 = 15
			OutputTokens: 5,
		}

		// Actual = 15, Estimated = 10, diff = 15 - 10 = 5. We expect an Incr of 5 tokens.
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expectedKey := "user-123:gpt-4:policy-token-ratio:1m0s"
		incred := mock.incrCalls[expectedKey]
		if incred != 5 {
			t.Errorf("expected 5 incread tokens, got %d", incred)
		}
	})
}

func TestTokenSettlementFilter_CompletionEstimation(t *testing.T) {
	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-token-ratio",
				Type: "token",
				Estimator: &policy.EstimatorConfig{
					Type:  "length_ratio",
					Ratio: 0.5,
				},
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	t.Run("Stream Interrupted (No usage) - Fallback to Estimation", func(t *testing.T) {
		mock := &mockSettlementStore{}
		f := NewTokenSettlementFilter(mock, nil, nil)

		body := []byte("12345678901234567890") // length = 20. ratio = 0.5. estimated = 10.
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
		gctx := &core.GatewayContext{
			Ctx:              context.Background(),
			Request:          req,
			UserID:           "user-123",
			Model:            "gpt-4",
			RawBody:          body,
			Policy:           p,
			IsStream:         true,
			InputTokens:      10,
			OutputTokens:     0,  // 没有返回官方 completion tokens
			TransmittedChars: 10, // 但发送了 10 个字符
		}

		// 我们预期 gctx.OutputTokens 会被估算为：TransmittedChars * 0.5 (对应 LimitPolicies 里的 Estimator.Ratio)
		// 也就是说 CompletionTokens = 10 * 0.5 = 5.
		// 最终结算：Actual = PromptTokens(10) + CompletionTokens(5) = 15.
		// Estimated = 10. diff = 15 - 10 = 5. We expect an Incr of 5 tokens.
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if gctx.OutputTokens != 5 {
			t.Errorf("expected estimated completion tokens to be 5, got %d", gctx.OutputTokens)
		}
		if gctx.Tags["completion_token_estimated"] != "true" {
			t.Errorf("expected tag completion_token_estimated=true, got %s", gctx.Tags["completion_token_estimated"])
		}

		expectedKey := "user-123:gpt-4:policy-token-ratio:1m0s"
		incred := mock.incrCalls[expectedKey]
		if incred != 5 {
			t.Errorf("expected 5 incread tokens, got %d", incred)
		}
	})
}

func TestTokenSettlementFilter_OnResponse_WithCachedTokens(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:         2.0,   // 元/百万 Tokens
			OutputPrice:        4.0,
			CachedPrice:        0.2,   // 90% off
			CacheCreationPrice: 2.5,
		},
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-cost-limit",
				Type: "cost",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	// PromptTokens = 100, CompletionTokens = 50.
	// CachedTokens = 80, CacheCreationTokens = 10.
	// Non-cached prompt tokens = 100 - 80 - 10 = 10.
	// gctx.Cost = (10 * 2.0 + 80 * 0.2 + 10 * 2.5 + 50 * 4.0) / 1_000_000
	//           = (20 + 16 + 25 + 200) / 1_000_000
	//           = 261 / 1_000_000 = 0.000261
	gctx := &core.GatewayContext{
		Ctx:                 context.Background(),
		Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		UserID:              "user-caching",
		Model:               "gpt-4",
		Policy:              p,
		InputTokens:         100,
		OutputTokens:        50,
		CachedTokens:        80,
		CacheCreationTokens: 10,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// expectedCost = 0.000261 元
	if !almostEqual(gctx.Cost, 0.000261) {
		t.Errorf("expected gctx.Cost to be 0.000261, got %f", gctx.Cost)
	}

	// 小 token 量下 actualCost(厘) = int64(261/1000) = 0, estimatedCost = 0, diff = 0
	// 因此不触发滑动窗口扣减（这是正确的行为，小量请求不产生厘级费用）
	expectedKey := "user-caching:gpt-4:policy-cost-limit:1m0s"
	incred := mock.incrCalls[expectedKey]
	if incred != 0 {
		t.Errorf("expected 0 incred cost (diff rounds to 0 for small token counts), got %d", incred)
	}
}

func TestTokenSettlementFilter_OnResponse_WithExcessiveCachedTokens(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:         2.0,  // 元/百万 Tokens
			OutputPrice:        4.0,
			CachedPrice:        0.2,
			CacheCreationPrice: 2.5,
		},
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-cost-limit",
				Type: "cost",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	// CachedTokens (200) + CacheCreationTokens (50) > InputTokens (100)
	// 预期 OnResponse 执行后，gctx.CachedTokens 被修正回写为 100，gctx.CacheCreationTokens 修正为 0
	gctx := &core.GatewayContext{
		Ctx:                 context.Background(),
		Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		UserID:              "user-caching-excessive",
		Model:               "gpt-4",
		Policy:              p,
		InputTokens:         100,
		OutputTokens:        50,
		CachedTokens:        200,
		CacheCreationTokens: 50,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if gctx.CachedTokens != 100 {
		t.Errorf("expected gctx.CachedTokens to be corrected to 100, got %d", gctx.CachedTokens)
	}

	if gctx.CacheCreationTokens != 0 {
		t.Errorf("expected gctx.CacheCreationTokens to be corrected to 0, got %d", gctx.CacheCreationTokens)
	}
}

func almostEqual(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 1e-9
}

func TestTokenSettlementFilter_OnResponse_PriceInheritanceAndOverride(t *testing.T) {
	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	t.Run("Inherit from model level billing policy", func(t *testing.T) {
		p := &policy.Policy{
			Billing: &policy.BillingPolicy{
				InputPrice:         5.0,  // 元/百万 Tokens
				OutputPrice:        8.0,
				CachedPrice:        1.0,
				CacheCreationPrice: 6.0,
			},
		}
		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-1",
			Model:               "gpt-4",
			Policy:              p,
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		// cost = (nonCachedPromptTokens * inputPrice + cachedTokens * cachedPrice + cacheCreationTokens * cacheCreationPrice + outputTokens * outputPrice) / 1_000_000.0
		// nonCachedPromptTokens = 100 - 20 - 10 = 70.
		// cost = (70 * 5.0 + 20 * 1.0 + 10 * 6.0 + 50 * 8.0) / 1_000_000.0
		//      = (350 + 20 + 60 + 400) / 1_000_000.0
		//      = 830 / 1_000_000.0 = 0.00083
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.00083) {
			t.Errorf("expected cost to be 0.00083, got %f", gctx.Cost)
		}
	})

	t.Run("Override by Endpoint level configuration (Full)", func(t *testing.T) {
		p := &policy.Policy{
			Billing: &policy.BillingPolicy{
				InputPrice:         5.0,  // 元/百万 Tokens
				OutputPrice:        8.0,
				CachedPrice:        1.0,
				CacheCreationPrice: 6.0,
			},
		}
		inputVal := 10.0
		outputVal := 20.0
		cachedVal := 3.0
		creationVal := 15.0

		endpoint := &core.Endpoint{
			InputPrice:         &inputVal,
			OutputPrice:        &outputVal,
			CachedPrice:        &cachedVal,
			CacheCreationPrice: &creationVal,
		}

		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-2",
			Model:               "gpt-4",
			Policy:              p,
			SelectedEndpoint:    endpoint,
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		// cost = (70 * 10.0 + 20 * 3.0 + 10 * 15.0 + 50 * 20.0) / 1_000_000.0
		//      = (700 + 60 + 150 + 1000) / 1_000_000.0 = 1910 / 1_000_000.0 = 0.00191
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.00191) {
			t.Errorf("expected cost to be 0.00191, got %f", gctx.Cost)
		}
	})

	t.Run("Override by Endpoint level configuration (Partial/Inheritance)", func(t *testing.T) {
		p := &policy.Policy{
			Billing: &policy.BillingPolicy{
				InputPrice:         5.0,  // 元/百万 Tokens
				OutputPrice:        8.0,
				CachedPrice:        1.0,
				CacheCreationPrice: 6.0,
			},
		}
		inputVal := 10.0
		outputVal := 20.0

		endpoint := &core.Endpoint{
			InputPrice:         &inputVal,
			OutputPrice:        &outputVal,
			CachedPrice:        nil,
			CacheCreationPrice: nil,
		}

		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-3",
			Model:               "gpt-4",
			Policy:              p,
			SelectedEndpoint:    endpoint,
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.00178) {
			t.Errorf("expected cost to be 0.00178, got %f", gctx.Cost)
		}
	})

	t.Run("Fallback to default 2.0 when policy and endpoint are nil", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-4",
			Model:               "gpt-4",
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.0003) {
			t.Errorf("expected cost to be 0.0003, got %f", gctx.Cost)
		}
	})
}
