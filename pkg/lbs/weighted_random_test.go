package lbs

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWeightedRandomLoadBalancer_WeightDistribution(t *testing.T) {
	lb := NewWeightedRandomLoadBalancer()
	ep1 := newEndpointWithWeight("ep1", 0.01, 3)
	ep2 := newEndpointWithWeight("ep2", 0.02, 1)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 权重比 3:1，10000 次抽样下 ep1 占比应接近 0.75
	const N = 10000
	counts := map[string]int{}
	for i := 0; i < N; i++ {
		inv := lb.Select(gctx, endpoints)
		require.NotNil(t, inv)
		counts[inv.Endpoint().ID]++
	}

	ratio1 := float64(counts["ep1"]) / float64(N)
	// 容忍 ±0.05 抽样误差
	assert.InDelta(t, 0.75, ratio1, 0.05, "ep1 ratio should be ~0.75, got %f", ratio1)
	assert.Equal(t, N, counts["ep1"]+counts["ep2"])
}

func TestWeightedRandomLoadBalancer_ZeroWeightTreatedAsOne(t *testing.T) {
	lb := NewWeightedRandomLoadBalancer()
	ep1 := newEndpointWithWeight("ep1", 0.01, 0) // 视为 1
	ep2 := newEndpointWithWeight("ep2", 0.02, 0) // 视为 1
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	const N = 2000
	counts := map[string]int{}
	for i := 0; i < N; i++ {
		inv := lb.Select(gctx, endpoints)
		require.NotNil(t, inv)
		counts[inv.Endpoint().ID]++
	}

	// 双方权重相等，比例应接近 0.5
	ratio1 := float64(counts["ep1"]) / float64(N)
	assert.InDelta(t, 0.5, ratio1, 0.08)
}

func TestWeightedRandomLoadBalancer_EmptyEndpoints(t *testing.T) {
	lb := NewWeightedRandomLoadBalancer()
	gctx := newGatewayContext()

	inv := lb.Select(gctx, []*core.Endpoint{})
	assert.Nil(t, inv)
}

func TestWeightedRandomLoadBalancer_SingleEndpoint(t *testing.T) {
	lb := NewWeightedRandomLoadBalancer()
	ep1 := newEndpointWithWeight("ep1", 0.01, 5)
	endpoints := []*core.Endpoint{ep1}
	gctx := newGatewayContext()

	for i := 0; i < 10; i++ {
		inv := lb.Select(gctx, endpoints)
		require.NotNil(t, inv)
		assert.Equal(t, "ep1", inv.Endpoint().ID)
	}
}

func TestWeightedRandomLoadBalancer_DominantWeight(t *testing.T) {
	lb := NewWeightedRandomLoadBalancer()
	ep1 := newEndpointWithWeight("ep1", 0.01, 99)
	ep2 := newEndpointWithWeight("ep2", 0.02, 1)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	const N = 5000
	counts := map[string]int{}
	for i := 0; i < N; i++ {
		inv := lb.Select(gctx, endpoints)
		require.NotNil(t, inv)
		counts[inv.Endpoint().ID]++
	}

	// ep1 应主导，ep2 命中次数极少
	assert.Greater(t, counts["ep1"], counts["ep2"]*10)
}
