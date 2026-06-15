package lbs

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// CompositeLoadBalancer 复合负载均衡器
// 综合成本和延迟进行加权评分，选择最优端点
type CompositeLoadBalancer struct {
	stateStore    core.StateStore
	costWeight    float64
	latencyWeight float64
}

// NewCompositeLoadBalancer 创建复合负载均衡器
func NewCompositeLoadBalancer(ss core.StateStore, costWeight, latencyWeight float64) *CompositeLoadBalancer {
	return &CompositeLoadBalancer{
		stateStore:    ss,
		costWeight:    costWeight,
		latencyWeight: latencyWeight,
	}
}

// Select 综合评分选择最优端点
func (lb *CompositeLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	costWeight := lb.costWeight
	latencyWeight := lb.latencyWeight

	// 动态解析策略中的权重参数
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

	// 第一遍：收集数据，计算最大值用于归一化
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

	// 第二遍：归一化并计算加权分数
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

	// 选择分数最低的（成本和延迟越低越好）
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
