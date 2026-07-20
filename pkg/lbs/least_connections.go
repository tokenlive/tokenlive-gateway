package lbs

import (
	"sync"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// LeastConnectionsLoadBalancer picks the endpoint with fewest active connections.
type LeastConnectionsLoadBalancer struct {
	mu          sync.Mutex
	connections map[string]*int64
}

// NewLeastConnectionsLoadBalancer creates a least-connections LB.
func NewLeastConnectionsLoadBalancer() *LeastConnectionsLoadBalancer {
	return &LeastConnectionsLoadBalancer{
		connections: make(map[string]*int64),
	}
}

// Select picks fewest connections and increments the counter.
func (lb *LeastConnectionsLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Ensure counters exist.
	for _, ep := range endpoints {
		if _, ok := lb.connections[ep.ID]; !ok {
			var count int64
			lb.connections[ep.ID] = &count
		}
	}

	// Pick lowest connection count.
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

	// Auto-increment.
	*lb.connections[selected.ID]++

	return invoker.NewProviderInvoker(selected.ProviderImpl, selected)
}

// IncrConnections increments the connection counter.
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

// Done decrements the connection counter after the request.
func (lb *LeastConnectionsLoadBalancer) Done(endpointID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if count, ok := lb.connections[endpointID]; ok {
		if *count > 0 {
			*count--
		}
	}
}

// ActiveConnections returns the current active connection count.
func (lb *LeastConnectionsLoadBalancer) ActiveConnections(endpointID string) int64 {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if count, ok := lb.connections[endpointID]; ok {
		return *count
	}
	return 0
}
