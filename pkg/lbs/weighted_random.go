package lbs

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// WeightedRandomLoadBalancer picks by weight proportion.
// Probability ∝ endpoint.Weight: cumulative ranges, rand in [0, totalWeight).
// weight<=0 treated as 1.
type WeightedRandomLoadBalancer struct{}

// NewWeightedRandomLoadBalancer creates a weighted-random LB.
func NewWeightedRandomLoadBalancer() *WeightedRandomLoadBalancer {
	return &WeightedRandomLoadBalancer{}
}

// Select picks one endpoint by weighted random.
func (lb *WeightedRandomLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	totalWeight := 0
	for _, ep := range endpoints {
		w := ep.Weight
		if w <= 0 {
			w = 1
		}
		totalWeight += w
	}

	r := randomInt(totalWeight)
	cum := 0
	for _, ep := range endpoints {
		w := ep.Weight
		if w <= 0 {
			w = 1
		}
		cum += w
		if r < cum {
			return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
		}
	}

	// Float/edge fallback; should not reach.
	last := endpoints[len(endpoints)-1]
	return invoker.NewProviderInvoker(last.ProviderImpl, last)
}
