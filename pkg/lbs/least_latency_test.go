package lbs

import (
	"context"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeastLatencyLoadBalancer_SelectLowest(t *testing.T) {
	ss := &mockLatencyStateStore{
		latencies: map[string]time.Duration{
			"ep1": 200 * time.Millisecond,
			"ep2": 50 * time.Millisecond,
			"ep3": 150 * time.Millisecond,
		},
	}
	lb := NewLeastLatencyLoadBalancer(ss)

	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	ep3 := newEndpoint("ep3", 0.03)
	endpoints := []*core.Endpoint{ep1, ep2, ep3}
	gctx := newGatewayContext()

	// 应该选择 ep2（延迟最低 50ms）
	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestLeastLatencyLoadBalancer_ZeroLatency(t *testing.T) {
	ss := &mockLatencyStateStore{
		latencies: map[string]time.Duration{
			"ep1": 0, // 无采样数据
			"ep2": 200 * time.Millisecond,
		},
	}
	lb := NewLeastLatencyLoadBalancer(ss)

	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 0 延迟可选，应选择 ep1
	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)
}

// ===== mock StateStore for latency tests =====

// mockLatencyStateStore 实现 core.StateStore 接口，仅 GetAvgLatency/GetAvgTTFT 有真实行为
type mockLatencyStateStore struct {
	latencies map[string]time.Duration
	ttfts     map[string]time.Duration
}

func (m *mockLatencyStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	return 0, nil
}
func (m *mockLatencyStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	return nil
}
func (m *mockLatencyStateStore) RateLimitTake(ctx context.Context, key string, tokens int64, limit int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	return true, capacity - tokens, nil
}
func (m *mockLatencyStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
	return "", nil
}
func (m *mockLatencyStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	return nil
}
func (m *mockLatencyStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	return nil
}
func (m *mockLatencyStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	if lat, ok := m.latencies[endpointID]; ok {
		return lat, nil
	}
	return 0, nil
}
func (m *mockLatencyStateStore) RecordTTFT(ctx context.Context, endpointID string, ttft time.Duration) error {
	return nil
}
func (m *mockLatencyStateStore) GetAvgTTFT(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	if lat, ok := m.ttfts[endpointID]; ok {
		return lat, nil
	}
	return 0, nil
}
func (m *mockLatencyStateStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0, nil
}
func (m *mockLatencyStateStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}
func (m *mockLatencyStateStore) Close() error { return nil }

func TestLeastLatencyLoadBalancer_MetricTTFT(t *testing.T) {
	ss := &mockLatencyStateStore{
		latencies: map[string]time.Duration{
			"ep1": 50 * time.Millisecond, // ep1 整单最快
			"ep2": 200 * time.Millisecond,
		},
		ttfts: map[string]time.Duration{
			"ep1": 300 * time.Millisecond,
			"ep2": 10 * time.Millisecond, // ep2 TTFT 最快
		},
	}
	lb := NewLeastLatencyLoadBalancer(ss)

	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	endpoints := []*core.Endpoint{ep1, ep2}

	// 无 Policy 时默认走 total:选 ep1
	gctx := newGatewayContext()
	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)

	// 配 metric=ttft:选 ep2
	gctx.Policy = &policy.Policy{
		LoadBalancePolicy: &policy.LoadBalancePolicy{
			Type: "least_latency",
			Params: map[string]interface{}{
				"latency_metric": "ttft",
			},
		},
	}
	invoker = lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestLeastLatencyLoadBalancer_LatencyWindowParam(t *testing.T) {
	ss := &mockLatencyStateStore{
		latencies: map[string]time.Duration{"ep1": 100 * time.Millisecond},
	}
	lb := NewLeastLatencyLoadBalancer(ss)

	ep1 := newEndpoint("ep1", 0.01)
	endpoints := []*core.Endpoint{ep1}

	gctx := newGatewayContext()
	gctx.Policy = &policy.Policy{
		LoadBalancePolicy: &policy.LoadBalancePolicy{
			Type: "least_latency",
			Params: map[string]interface{}{
				"latency_window": 120.0, // float64 数字
			},
		},
	}

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)

	// 字符串形式 window
	gctx.Policy.LoadBalancePolicy.Params = map[string]interface{}{
		"latency_window": "3m",
	}
	invoker = lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)
}

