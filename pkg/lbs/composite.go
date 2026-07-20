package lbs

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// CompositeLoadBalancer scores by cost and latency weights.
// Weighted score of cost + latency; pick best.
type CompositeLoadBalancer struct {
	stateStore    core.StateStore
	costWeight    float64
	latencyWeight float64
}

// NewCompositeLoadBalancer creates a composite LB.
func NewCompositeLoadBalancer(ss core.StateStore, costWeight, latencyWeight float64) *CompositeLoadBalancer {
	return &CompositeLoadBalancer{
		stateStore:    ss,
		costWeight:    costWeight,
		latencyWeight: latencyWeight,
	}
}

// Select picks the best-scoring endpoint.
func (lb *CompositeLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	costWeight := lb.costWeight
	latencyWeight := lb.latencyWeight

	// Resolve weight params from policy.
	if gctx != nil && gctx.Policy != nil && gctx.Policy.LoadBalancePolicy != nil && gctx.Policy.LoadBalancePolicy.Params != nil {
		params := gctx.Policy.LoadBalancePolicy.Params
		if cw, ok := parseWeight(params["cost_weight"]); ok {
			costWeight = cw
		}
		if lw, ok := parseWeight(params["latency_weight"]); ok {
			latencyWeight = lw
		}
	}

	type scored struct {
		ep      *core.Endpoint
		cost    float64
		latency time.Duration
		score   float64
	}

	scores := make([]scored, len(endpoints))
	maxCost := 0.0
	maxLatency := time.Duration(0)

	// Pass 1: collect data, max for normalization.
	for i, ep := range endpoints {
		cost := ep.CostPerToken()
		if cost > maxCost {
			maxCost = cost
		}
		latency, _ := lb.stateStore.GetAvgLatency(context.Background(), ep.ID, 5*time.Minute)
		if latency > maxLatency {
			maxLatency = latency
		}
		scores[i] = scored{ep: ep, cost: cost, latency: latency}
	}

	// Pass 2: normalize and weighted score.
	for i, s := range scores {
		costScore := 0.0
		if maxCost > 0 {
			costScore = s.cost / maxCost
		}
		latencyScore := 0.0
		if maxLatency > 0 {
			latencyScore = float64(s.latency) / float64(maxLatency)
		}
		scores[i].score = costScore*costWeight + latencyScore*latencyWeight
	}

	// Pick lowest score (lower cost/latency is better).
	best := scores[0]
	for _, s := range scores[1:] {
		if s.score < best.score {
			best = s
		}
	}
	return invoker.NewProviderInvoker(best.ep.ProviderImpl, best.ep)
}

func parseWeight(val interface{}) (float64, bool) {
	if val == nil {
		return 0, false
	}
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}
