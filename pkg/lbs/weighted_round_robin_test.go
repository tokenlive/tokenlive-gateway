package lbs

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEndpointWithWeight(id string, costPerToken float64, weight int) *core.Endpoint {
	ep := newEndpoint(id, costPerToken)
	ep.Weight = weight
	return ep
}

func TestWeightedRoundRobinLoadBalancer_WeightDistribution(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()
	ep1 := newEndpointWithWeight("ep1", 0.01, 3)
	ep2 := newEndpointWithWeight("ep2", 0.02, 1)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// ep1(weight=3) + ep2(weight=1) = 总权重 4
	// 4 次选择应该 ep1=3, ep2=1
	counts := map[string]int{}
	for i := 0; i < 4; i++ {
		invoker := lb.Select(gctx, endpoints)
		require.NotNil(t, invoker)
		counts[invoker.Endpoint().ID]++
	}

	assert.Equal(t, 3, counts["ep1"])
	assert.Equal(t, 1, counts["ep2"])
}

func TestWeightedRoundRobinLoadBalancer_ZeroWeight(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()
	ep1 := newEndpointWithWeight("ep1", 0.01, 0) // weight=0 → treated as 1
	ep2 := newEndpointWithWeight("ep2", 0.02, 1)
	endpoints := []*core.Endpoint{ep1, ep2}
	gctx := newGatewayContext()

	// 两者权重均为 1，应该均匀分布
	counts := map[string]int{}
	for i := 0; i < 4; i++ {
		invoker := lb.Select(gctx, endpoints)
		require.NotNil(t, invoker)
		counts[invoker.Endpoint().ID]++
	}

	assert.Equal(t, 2, counts["ep1"])
	assert.Equal(t, 2, counts["ep2"])
}

func TestWeightedRoundRobinLoadBalancer_EmptyEndpoints(t *testing.T) {
	lb := NewWeightedRoundRobinLoadBalancer()
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, []*core.Endpoint{})
	assert.Nil(t, invoker)
}
