package lbs

import (
	"math"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// CostLoadBalancer 成本优先负载均衡器
// 选择每 token 成本最低的端点
type CostLoadBalancer struct{}

// NewCostLoadBalancer 创建成本优先负载均衡器
func NewCostLoadBalancer() *CostLoadBalancer {
	return &CostLoadBalancer{}
}

// Select 选择成本最低的端点
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
