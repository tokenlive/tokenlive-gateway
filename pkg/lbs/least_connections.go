package lbs

import (
	"sync"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// LeastConnectionsLoadBalancer 最少连接负载均衡器
type LeastConnectionsLoadBalancer struct {
	mu          sync.Mutex
	connections map[string]*int64
}

// NewLeastConnectionsLoadBalancer 创建最少连接负载均衡器
func NewLeastConnectionsLoadBalancer() *LeastConnectionsLoadBalancer {
	return &LeastConnectionsLoadBalancer{
		connections: make(map[string]*int64),
	}
}

// Select 选择活跃连接最少的端点，并自动递增连接计数
func (lb *LeastConnectionsLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 确保每个端点都有计数器
	for _, ep := range endpoints {
		if _, ok := lb.connections[ep.ID]; !ok {
			var count int64
			lb.connections[ep.ID] = &count
		}
	}

	// 选择连接数最少的端点
	var selected *core.Endpoint
	var minConns int64 = -1
	for _, ep := range endpoints {
		conns := *lb.connections[ep.ID]
		if minConns < 0 || conns < minConns {
			minConns = conns
			selected = ep
		}
	}

	if selected == nil {
		return nil
	}

	// 自动递增
	*lb.connections[selected.ID]++

	return invoker.NewProviderInvoker(selected.ProviderImpl, selected)
}

// IncrConnections 手动递增指定端点的连接计数
func (lb *LeastConnectionsLoadBalancer) IncrConnections(endpointID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if count, ok := lb.connections[endpointID]; ok {
		*count++
	} else {
		var c int64 = 1
		lb.connections[endpointID] = &c
	}
}

// Done 请求完成后递减连接计数
func (lb *LeastConnectionsLoadBalancer) Done(endpointID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if count, ok := lb.connections[endpointID]; ok {
		if *count > 0 {
			*count--
		}
	}
}

// ActiveConnections 返回指定端点的当前活跃连接数
func (lb *LeastConnectionsLoadBalancer) ActiveConnections(endpointID string) int64 {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if count, ok := lb.connections[endpointID]; ok {
		return *count
	}
	return 0
}
