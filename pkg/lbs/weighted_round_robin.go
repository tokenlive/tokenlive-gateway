package lbs

import (
	"sync"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// WeightedRoundRobinLoadBalancer uses smooth weighted round-robin.
// Smooth Weighted Round-Robin algorithm.
type WeightedRoundRobinLoadBalancer struct {
	mu      sync.Mutex
	weights map[string]int // Current effective weights
}

// NewWeightedRoundRobinLoadBalancer creates a weighted RR LB.
func NewWeightedRoundRobinLoadBalancer() *WeightedRoundRobinLoadBalancer {
	return &WeightedRoundRobinLoadBalancer{
		weights: make(map[string]int),
	}
}

// Select uses smooth weighted round-robin.
func (lb *WeightedRoundRobinLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Sum weights and init effective weights.
	totalWeight := 0
	for _, ep := range endpoints {
		w := ep.Weight
		if w <= 0 {
			w = 1
		}
		totalWeight += w

		if _, ok := lb.weights[ep.ID]; !ok {
			lb.weights[ep.ID] = 0
		}
	}

	// Smooth WRR: current += weight, pick max, then -= totalWeight.
	var selected *core.Endpoint
	maxWeight := -1

	for _, ep := range endpoints {
		w := ep.Weight
		if w <= 0 {
			w = 1
		}
		lb.weights[ep.ID] += w

		if maxWeight < 0 || lb.weights[ep.ID] > maxWeight {
			maxWeight = lb.weights[ep.ID]
			selected = ep
		}
	}

	if selected == nil {
		return nil
	}

	lb.weights[selected.ID] -= totalWeight

	return invoker.NewProviderInvoker(selected.ProviderImpl, selected)
}
