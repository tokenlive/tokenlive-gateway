package limiter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type mockRequestStore struct {
	core.StateStore

	incrKey    string
	incrTokens int64
	incrWindow time.Duration
	incrVal    int64
	incrErr    error

	takeKey       string
	takeTokens    int64
	takeLimit     int64
	takeCapacity  int64
	takeWindow    time.Duration
	takeAllowed   bool
	takeRemaining int64
	takeErr       error
}

func (m *mockRequestStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	m.incrKey = key
	m.incrTokens = tokens
	m.incrWindow = window
	return m.incrVal, m.incrErr
}

func (m *mockRequestStore) RateLimitTake(ctx context.Context, key string, tokens int64, limit int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	m.takeKey = key
	m.takeTokens = tokens
	m.takeLimit = limit
	m.takeCapacity = capacity
	m.takeWindow = window
	return m.takeAllowed, m.takeRemaining, m.takeErr
}

func (m *mockRequestStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	return nil
}

func (m *mockRequestStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0, nil
}

func (m *mockRequestStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func TestRequestLimitExecutor_Execute(t *testing.T) {
	gctx := &core.GatewayContext{
		UserID: "user-123",
		Model:  "gpt-4",
	}

	t.Run("Standard counter limit - within", func(t *testing.T) {
		lp := &policy.LimitPolicy{
			Name: "req-limit-policy",
			Type: "request",
			SlidingWindows: []*policy.SlidingWindow{
				{Threshold: 10, TimeWindowInMs: 60000},
			},
		}
		mock := &mockRequestStore{
			incrVal: 5,
		}
		exec := NewRequestLimitExecutor(mock)
		err := exec.Execute(context.Background(), gctx, lp)
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if mock.incrKey != "user-123:gpt-4:req-limit-policy:1m0s" {
			t.Errorf("unexpected key: %s", mock.incrKey)
		}
	})

	t.Run("Standard counter limit - exceeded", func(t *testing.T) {
		lp := &policy.LimitPolicy{
			Name: "req-limit-policy",
			Type: "request",
			SlidingWindows: []*policy.SlidingWindow{
				{Threshold: 10, TimeWindowInMs: 60000},
			},
		}
		mock := &mockRequestStore{
			incrVal: 11,
		}
		exec := NewRequestLimitExecutor(mock)
		err := exec.Execute(context.Background(), gctx, lp)
		if err == nil {
			t.Fatal("expected 429 error, got nil")
		}
		httpErr, ok := err.(*HTTPError)
		if !ok || httpErr.Code != http.StatusTooManyRequests {
			t.Errorf("expected 429 too many requests, got %v", err)
		}
	})

	t.Run("Token Bucket Burst limit - within", func(t *testing.T) {
		ratio := 2.5
		lp := &policy.LimitPolicy{
			Name: "req-limit-policy",
			Type: "request",
			SlidingWindows: []*policy.SlidingWindow{
				{Threshold: 10, TimeWindowInMs: 60000, BurstRatio: &ratio},
			},
		}
		mock := &mockRequestStore{
			takeAllowed:   true,
			takeRemaining: 20,
		}
		exec := NewRequestLimitExecutor(mock)
		err := exec.Execute(context.Background(), gctx, lp)
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if mock.takeKey != "user-123:gpt-4:req-limit-policy:1m0s" {
			t.Errorf("unexpected key: %s", mock.takeKey)
		}
		if mock.takeCapacity != 25 { // 10 * 2.5 = 25
			t.Errorf("expected capacity 25, got %d", mock.takeCapacity)
		}
	})

	t.Run("Token Bucket Burst limit - exceeded", func(t *testing.T) {
		ratio := 2.5
		lp := &policy.LimitPolicy{
			Name: "req-limit-policy",
			Type: "request",
			SlidingWindows: []*policy.SlidingWindow{
				{Threshold: 10, TimeWindowInMs: 60000, BurstRatio: &ratio},
			},
		}
		mock := &mockRequestStore{
			takeAllowed:   false,
			takeRemaining: 0,
		}
		exec := NewRequestLimitExecutor(mock)
		err := exec.Execute(context.Background(), gctx, lp)
		if err == nil {
			t.Fatal("expected 429 error, got nil")
		}
		httpErr, ok := err.(*HTTPError)
		if !ok || httpErr.Code != http.StatusTooManyRequests {
			t.Errorf("expected 429 too many requests, got %v", err)
		}
	})
}
