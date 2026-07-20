package lbs

import (
	"math"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// CostLoadBalancer prefers the lowest per-token cost.
// Picks the endpoint with lowest cost per token.
type CostLoadBalancer struct{}

// NewCostLoadBalancer creates a cost-based LB.
func NewCostLoadBalancer() *CostLoadBalancer {
	return &CostLoadBalancer{}
}

// Select picks the lowest-cost endpoint.
func (lb *CostLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	var best *core.Endpoint
	bestCost := math.MaxFloat64
	for _, ep := range endpoints {
		cost := ep.CostPerToken()
		if cost < bestCost {
			bestCost = cost
			best = ep
		}
	}
	return invoker.NewProviderInvoker(best.ProviderImpl, best)
}
