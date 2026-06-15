package lbs

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandomLoadBalancer_Select(t *testing.T) {
	lb := NewRandomLoadBalancer()
	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	ep3 := newEndpoint("ep3", 0.03)
	endpoints := []*core.Endpoint{ep1, ep2, ep3}
	gctx := newGatewayContext()

	counts := map[string]int{}
	for i := 0; i < 100; i++ {
		invoker := lb.Select(gctx, endpoints)
		require.NotNil(t, invoker)
		counts[invoker.Endpoint().ID]++
	}

	// 100 次随机选择，至少命中 2 个不同端点
	assert.GreaterOrEqual(t, len(counts), 2, "expected at least 2 distinct endpoints in 100 random selections")
}

func TestRandomLoadBalancer_EmptyEndpoints(t *testing.T) {
	lb := NewRandomLoadBalancer()
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, []*core.Endpoint{})
	assert.Nil(t, invoker)
}

func TestRandomLoadBalancer_SingleEndpoint(t *testing.T) {
	lb := NewRandomLoadBalancer()
	ep1 := newEndpoint("ep1", 0.01)
	endpoints := []*core.Endpoint{ep1}
	gctx := newGatewayContext()

	for i := 0; i < 10; i++ {
		invoker := lb.Select(gctx, endpoints)
		require.NotNil(t, invoker)
		assert.Equal(t, "ep1", invoker.Endpoint().ID)
	}
}
