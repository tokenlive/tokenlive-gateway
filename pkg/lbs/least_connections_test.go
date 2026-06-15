package lbs

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeastConnectionsLoadBalancer_SelectLeast(t *testing.T) {
	lb := NewLeastConnectionsLoadBalancer()
	ep1 := newEndpoint("ep1", 0.01)
	ep2 := newEndpoint("ep2", 0.02)
	ep3 := newEndpoint("ep3", 0.03)
	endpoints := []*core.Endpoint{ep1, ep2, ep3}
	gctx := newGatewayContext()

	// 手动设置连接数：ep1=5, ep2=2, ep3=8
	for i := 0; i < 5; i++ {
		lb.IncrConnections("ep1")
	}
	for i := 0; i < 2; i++ {
		lb.IncrConnections("ep2")
	}
	for i := 0; i < 8; i++ {
		lb.IncrConnections("ep3")
	}

	// 应该选择 ep2（连接数最少）
	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)

	// 选择后 ep2 的连接数变为 3，仍然最少
	invoker = lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestLeastConnectionsLoadBalancer_EmptyEndpoints(t *testing.T) {
	lb := NewLeastConnectionsLoadBalancer()
	gctx := newGatewayContext()

	invoker := lb.Select(gctx, []*core.Endpoint{})
	assert.Nil(t, invoker)
}

func TestLeastConnectionsLoadBalancer_DoneTracking(t *testing.T) {
	lb := NewLeastConnectionsLoadBalancer()

	// 初始连接数为 0
	assert.Equal(t, int64(0), lb.ActiveConnections("ep1"))

	// 递增 3 次
	lb.IncrConnections("ep1")
	lb.IncrConnections("ep1")
	lb.IncrConnections("ep1")
	assert.Equal(t, int64(3), lb.ActiveConnections("ep1"))

	// 完成 2 次
	lb.Done("ep1")
	lb.Done("ep1")
	assert.Equal(t, int64(1), lb.ActiveConnections("ep1"))

	// Done 不会低于 0
	lb.Done("ep1")
	lb.Done("ep1")
	lb.Done("ep1")
	assert.Equal(t, int64(0), lb.ActiveConnections("ep1"))
}
