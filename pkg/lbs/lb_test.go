package lbs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===== 测试辅助函数 =====

func newEndpoint(id string, costPerToken float64) *core.Endpoint {
	return &core.Endpoint{
		ID:       id,
		URL:      "http://example.com/" + id,
		Provider: "test",
		Model:    "test-model",
		Metadata: map[string]string{
			"cost_per_token": formatFloat(costPerToken),
		},
	}
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%f", f)
}

func newGatewayContext() *core.GatewayContext {
	return &core.GatewayContext{
		Ctx:       context.Background(),
		SessionID: "test-session",
	}
}

// ===== RoundRobin 测试 =====

func TestRoundRobin_EvenDistribution(t *testing.T) {
	lb := NewRoundRobin()
	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	ep3 := newEndpoint("ep3", 0.03)
	endpoints := []*core.Endpoint{ep1, ep2, ep3}

	counts := map[string]int{}
	gctx := newGatewayContext()

	// 选择 300 次，应该均匀分布
	for i := 0; i < 300; i++ {
		invoker := lb.Select(gctx, endpoints)
		require.NotNil(t, invoker)
		counts[invoker.Endpoint().ID]++
	}

	assert.Equal(t, 100, counts["ep1"])
	assert.Equal(t, 100, counts["ep2"])
	assert.Equal(t, 100, counts["ep3"])
}

func TestRoundRobin_CounterIsolatedByModel(t *testing.T) {
	lb := NewRoundRobin()

	modelAEndpoints := []*core.Endpoint{
		{ID: "a1", Model: "model-a"},
		{ID: "a2", Model: "model-a"},
	}
	modelBEndpoints := []*core.Endpoint{
		{ID: "b1", Model: "model-b"},
		{ID: "b2", Model: "model-b"},
	}

	gctxA := newGatewayContext()
	gctxA.Model = "model-a"
	invokerA := lb.Select(gctxA, modelAEndpoints)
	require.NotNil(t, invokerA)
	assert.Equal(t, "a1", invokerA.Endpoint().ID)

	gctxB := newGatewayContext()
	gctxB.Model = "model-b"
	invokerB := lb.Select(gctxB, modelBEndpoints)
	require.NotNil(t, invokerB)
	assert.Equal(t, "b1", invokerB.Endpoint().ID)

	invokerA = lb.Select(gctxA, modelAEndpoints)
	require.NotNil(t, invokerA)
	assert.Equal(t, "a2", invokerA.Endpoint().ID)
}

func TestRoundRobin_CounterIsolatedByCandidatePool(t *testing.T) {
	lb := NewRoundRobin()

	fullPool := []*core.Endpoint{
		{ID: "ep1", Model: "model-a"},
		{ID: "ep2", Model: "model-a"},
		{ID: "ep3", Model: "model-a"},
		{ID: "ep4", Model: "model-a"},
	}
	filteredPool := fullPool[:3]

	gctx := newGatewayContext()
	gctx.Model = "model-a"

	for i := 0; i < 3; i++ {
		invoker := lb.Select(gctx, filteredPool)
		require.NotNil(t, invoker)
	}

	invoker := lb.Select(gctx, fullPool)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)

	for _, want := range []string{"ep2", "ep3", "ep4"} {
		invoker = lb.Select(gctx, fullPool)
		require.NotNil(t, invoker)
		assert.Equal(t, want, invoker.Endpoint().ID)
	}
}

func TestRoundRobin_EmptyEndpoints(t *testing.T) {
	lb := NewRoundRobin()
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, []*core.Endpoint{})
	assert.Nil(t, invoker)
}

func TestRoundRobin_SingleEndpoint(t *testing.T) {
	lb := NewRoundRobin()
	ep1 := newEndpoint("ep1", 0.01)
	endpoints := []*core.Endpoint{ep1}
	gctx := newGatewayContext()

	for i := 0; i < 10; i++ {
		invoker := lb.Select(gctx, endpoints)
		require.NotNil(t, invoker)
		assert.Equal(t, "ep1", invoker.Endpoint().ID)
	}
}

// ===== Sticky 测试 =====

func TestSticky_FallbackOnMiss(t *testing.T) {
	ss := store.NewMemoryStateStore()
	fallback := NewRoundRobin()
	lb := NewStickyLoadBalancer(ss, fallback, func(gctx *core.GatewayContext) string {
		return gctx.SessionID
	}, 5*time.Minute)

	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 没有粘滞记录，应该回退到 RoundRobin
	invoker := lb.Select(gctx, endpoints)
	assert.NotNil(t, invoker)
}

func TestSticky_StickOnHit(t *testing.T) {
	ss := store.NewMemoryStateStore()
	fallback := NewRoundRobin()
	lb := NewStickyLoadBalancer(ss, fallback, func(gctx *core.GatewayContext) string {
		return gctx.SessionID
	}, 5*time.Minute)

	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 预先设置粘滞记录
	err := ss.StickySet(context.Background(), "test-session", "ep2", 5*time.Minute)
	require.NoError(t, err)

	// 应该选择 ep2
	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestSticky_FallbackWhenEndpointMissing(t *testing.T) {
	ss := store.NewMemoryStateStore()
	fallback := NewRoundRobin()
	lb := NewStickyLoadBalancer(ss, fallback, func(gctx *core.GatewayContext) string {
		return gctx.SessionID
	}, 5*time.Minute)

	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 设置一个不存在的端点 ID
	err := ss.StickySet(context.Background(), "test-session", "ep-nonexist", 5*time.Minute)
	require.NoError(t, err)

	// 应该回退到 RoundRobin
	invoker := lb.Select(gctx, endpoints)
	assert.NotNil(t, invoker)
}

func TestSticky_EmptyKey(t *testing.T) {
	ss := store.NewMemoryStateStore()
	fallback := NewRoundRobin()
	lb := NewStickyLoadBalancer(ss, fallback, func(gctx *core.GatewayContext) string {
		return "" // 空 key
	}, 5*time.Minute)

	ep1 := newEndpoint("ep1", 0.01)
	endpoints := []*core.Endpoint{ep1}
	gctx := newGatewayContext()

	// 空 key 应该直接回退
	invoker := lb.Select(gctx, endpoints)
	assert.NotNil(t, invoker)
}

// ===== Cost 测试 =====

func TestCost_SelectsCheapest(t *testing.T) {
	lb := NewCostLoadBalancer()
	ep1 := newEndpoint("ep1", 0.03)
	ep2 := newEndpoint("ep2", 0.01)
	ep3 := newEndpoint("ep3", 0.02)
	endpoints := []*core.Endpoint{ep1, ep2, ep3}
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestCost_EmptyEndpoints(t *testing.T) {
	lb := NewCostLoadBalancer()
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, []*core.Endpoint{})
	assert.Nil(t, invoker)
}

func TestCost_SameCost(t *testing.T) {
	lb := NewCostLoadBalancer()
	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.01)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, endpoints)
	assert.NotNil(t, invoker)
	// 两个成本相同，选择第一个
	assert.Equal(t, "ep1", invoker.Endpoint().ID)
}

func TestCost_ZeroCost(t *testing.T) {
	lb := NewCostLoadBalancer()
	ep1 := newEndpoint("ep1", 0.0) // 成本为 0
	ep2 := newEndpoint("ep2", 0.05)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)
}

// ===== Composite 测试 =====

func TestComposite_WeightedScoring(t *testing.T) {
	ss := store.NewMemoryStateStore()
	// 成本权重更高
	lb := NewCompositeLoadBalancer(ss, 0.7, 0.3)

	ep1 := newEndpoint("ep1", 0.03) // 高成本
	ep2 := newEndpoint("ep2", 0.01) // 低成本
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 没有延迟数据，只看成本
	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestComposite_WithLatencyData(t *testing.T) {
	ss := store.NewMemoryStateStore()
	// 延迟权重更高
	lb := NewCompositeLoadBalancer(ss, 0.3, 0.7)

	ep1 := newEndpoint("ep1", 0.01) // 低成本
	ep2 := newEndpoint("ep2", 0.02) // 高成本
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 设置延迟数据：ep1 延迟高，ep2 延迟低
	err := ss.RecordLatency(context.Background(), "ep1", 500*time.Millisecond)
	require.NoError(t, err)
	err = ss.RecordLatency(context.Background(), "ep2", 100*time.Millisecond)
	require.NoError(t, err)

	// 延迟权重高，应该选择 ep2（延迟低）
	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestComposite_EmptyEndpoints(t *testing.T) {
	ss := store.NewMemoryStateStore()
	lb := NewCompositeLoadBalancer(ss, 0.5, 0.5)
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, []*core.Endpoint{})
	assert.Nil(t, invoker)
}

func TestComposite_SingleEndpoint(t *testing.T) {
	ss := store.NewMemoryStateStore()
	lb := NewCompositeLoadBalancer(ss, 0.5, 0.5)

	ep1 := newEndpoint("ep1", 0.01)
	endpoints := []*core.Endpoint{ep1}
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)
}

func TestComposite_DynamicWeightsFromPolicy(t *testing.T) {
	ss := store.NewMemoryStateStore()
	// 默认权重 0.5, 0.5
	lb := NewCompositeLoadBalancer(ss, 0.5, 0.5)

	ep1 := newEndpoint("ep1", 0.01) // 低成本
	ep2 := newEndpoint("ep2", 0.05) // 高成本
	endpoints := []*core.Endpoint{ep1, ep2}

	// 记录延迟：ep1（低成本）延迟高，ep2（高成本）延迟低
	err := ss.RecordLatency(context.Background(), "ep1", 1000*time.Millisecond)
	require.NoError(t, err)
	err = ss.RecordLatency(context.Background(), "ep2", 100*time.Millisecond)
	require.NoError(t, err)

	// 测试场景 1：偏向成本 (cost_weight=0.9, latency_weight=0.1)
	// 即使 ep1 延迟高达 1000ms，因为成本特别低且成本权重高达 90%，应该选择 ep1
	gctx1 := newGatewayContext()
	gctx1.Policy = &policy.Policy{
		LoadBalancePolicy: &policy.LoadBalancePolicy{
			Type: "composite",
			Params: map[string]interface{}{
				"cost_weight":    0.9,
				"latency_weight": 0.1,
			},
		},
	}
	invoker1 := lb.Select(gctx1, endpoints)
	require.NotNil(t, invoker1)
	assert.Equal(t, "ep1", invoker1.Endpoint().ID)

	// 测试场景 2：偏向延迟 (cost_weight=0.1, latency_weight=0.9)
	// 即使 ep2 成本高达 0.05，因为延迟低且延迟权重高达 90%，应该选择 ep2
	gctx2 := newGatewayContext()
	gctx2.Policy = &policy.Policy{
		LoadBalancePolicy: &policy.LoadBalancePolicy{
			Type: "composite",
			Params: map[string]interface{}{
				"cost_weight":    0.1,
				"latency_weight": 0.9,
			},
		},
	}
	invoker2 := lb.Select(gctx2, endpoints)
	require.NotNil(t, invoker2)
	assert.Equal(t, "ep2", invoker2.Endpoint().ID)
}

// ===== 接口一致性测试 =====

func TestLoadBalancer_Interface(t *testing.T) {
	// 确保所有实现都满足接口
	var _ core.LoadBalancer = (*RoundRobin)(nil)
	var _ core.LoadBalancer = (*StickyLoadBalancer)(nil)
	var _ core.LoadBalancer = (*CostLoadBalancer)(nil)
	var _ core.LoadBalancer = (*CompositeLoadBalancer)(nil)
}
