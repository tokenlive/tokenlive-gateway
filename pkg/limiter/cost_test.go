package limiter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type mockCostStore struct {
	core.StateStore

	incrKey    string
	incrTokens int64
	incrWindow time.Duration
	incrVal    int64
	incrErr    error

	refundKey    string
	refundTokens int64
}

func (m *mockCostStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	m.incrKey = key
	m.incrTokens = tokens
	m.incrWindow = window
	return m.incrVal, m.incrErr
}

func (m *mockCostStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	m.refundKey = key
	m.refundTokens = tokens
	return nil
}

func (m *mockCostStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0, nil
}

func (m *mockCostStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func TestCostLimitExecutor_Execute(t *testing.T) {
	lp := &policy.LimitPolicy{
		Name: "cost-limit-policy",
		Type: "cost",
		SlidingWindows: []*policy.SlidingWindow{
			{Threshold: 100, TimeWindowInMs: 60000}, // Threshold: 100厘
		},
	}

	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:  0.005, // 0.005厘 / token
			OutputPrice: 0.010,
		},
	}

	gctx := &core.GatewayContext{
		UserID:  "user-123",
		Model:   "gpt-4",
		RawBody: []byte("12345678901234567890"), // 20 bytes -> default estimate = 5 tokens
		Policy:  p,
	}

	// (5 + 200) tokens * 0.005 * 1000 = 1025 厘
	expectedCost := int64(1025)

	t.Run("Within limit", func(t *testing.T) {
		mock := &mockCostStore{
			incrVal: 50, // 新逻辑下，incrVal 直接是已消耗值 current = 50 <= 100。OK
		}

		executor := NewCostLimitExecutor(mock)
		err := executor.Execute(context.Background(), gctx, lp)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if mock.incrKey != "user-123:gpt-4:cost-limit-policy:1m0s" {
			t.Errorf("unexpected incr key: %s", mock.incrKey)
		}
		if mock.incrTokens != expectedCost {
			t.Errorf("expected incr %d tokens (cost), got %d", expectedCost, mock.incrTokens)
		}
	})

	t.Run("Exceeded limit", func(t *testing.T) {
		mock := &mockCostStore{
			incrVal: 150, // 新逻辑下，incrVal 直接是已消耗值 current = 150 > 100。Over limit!
		}

		executor := NewCostLimitExecutor(mock)
		err := executor.Execute(context.Background(), gctx, lp)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		httpErr, ok := err.(*HTTPError)
		if !ok || httpErr.Code != http.StatusTooManyRequests {
			t.Errorf("expected TooManyRequests HTTPError, got %+v", err)
		}

		// Verify Refund was triggered
		if mock.refundTokens != expectedCost {
			t.Errorf("expected refund %d tokens, got %d", expectedCost, mock.refundTokens)
		}
	})
}
