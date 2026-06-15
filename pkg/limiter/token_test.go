package limiter

import (
	"context"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func TestEstimateInputTokens(t *testing.T) {
	t.Run("length_ratio estimator", func(t *testing.T) {
		lp := &policy.LimitPolicy{
			Estimator: &policy.EstimatorConfig{
				Type:  "length_ratio",
				Ratio: 0.5,
			},
		}
		gctx := &core.GatewayContext{
			RawBody: []byte("12345678901234567890"), // 20 bytes
		}
		got := EstimateInputTokens(gctx, lp)
		if got != 10 {
			t.Errorf("expected 10 tokens, got %d", got)
		}
	})

	t.Run("fallback estimator when estimator nil", func(t *testing.T) {
		gctx := &core.GatewayContext{
			RawBody: []byte("12345678901234567890"), // 20 bytes
		}
		got := EstimateInputTokens(gctx, nil)
		if got != 5 {
			t.Errorf("expected 5 tokens, got %d", got)
		}
	})
}

func TestTokenLimitExecutor_Execute(t *testing.T) {
	gctx := &core.GatewayContext{
		UserID:  "user-123",
		Model:   "gpt-4",
		RawBody: []byte("12345678901234567890"), // 20 bytes -> default estimate = 5 tokens
	}

	t.Run("Standard Token Limit - within", func(t *testing.T) {
		lp := &policy.LimitPolicy{
			Name: "token-limit-policy",
			Type: "token",
			SlidingWindows: []*policy.SlidingWindow{
				{Threshold: 100, TimeWindowInMs: 60000},
			},
		}
		mock := &mockRequestStore{
			incrVal: 50,
		}
		exec := NewTokenLimitExecutor(mock)
		err := exec.Execute(context.Background(), gctx, lp)
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if mock.incrKey != "user-123:gpt-4:token-limit-policy:1m0s" {
			t.Errorf("unexpected key: %s", mock.incrKey)
		}
		if mock.incrTokens != 205 {
			t.Errorf("expected 205 tokens, got %d", mock.incrTokens)
		}
	})

	t.Run("Token Bucket Burst Token Limit - within", func(t *testing.T) {
		ratio := 2.0
		lp := &policy.LimitPolicy{
			Name: "token-limit-policy",
			Type: "token",
			SlidingWindows: []*policy.SlidingWindow{
				{Threshold: 100, TimeWindowInMs: 60000, BurstRatio: &ratio},
			},
		}
		mock := &mockRequestStore{
			takeAllowed:   true,
			takeRemaining: 150,
		}
		exec := NewTokenLimitExecutor(mock)
		err := exec.Execute(context.Background(), gctx, lp)
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if mock.takeKey != "user-123:gpt-4:token-limit-policy:1m0s" {
			t.Errorf("unexpected key: %s", mock.takeKey)
		}
		if mock.takeTokens != 205 {
			t.Errorf("expected 205 tokens, got %d", mock.takeTokens)
		}
		if mock.takeCapacity != 200 { // 100 * 2.0 = 200
			t.Errorf("expected capacity 200, got %d", mock.takeCapacity)
		}
	})
}
