package lbs

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// WeightedRandomLoadBalancer 加权随机负载均衡器
// 按 endpoint.Weight 的比例分配选择概率：累加权重区间，在 [0, totalWeight) 取随机数，
// 落入哪个端点的区间就选哪个。weight<=0 视为 1。
type WeightedRandomLoadBalancer struct{}

// NewWeightedRandomLoadBalancer 创建加权随机负载均衡器
func NewWeightedRandomLoadBalancer() *WeightedRandomLoadBalancer {
	return &WeightedRandomLoadBalancer{}
}

// Select 按权重加权随机选择一个端点
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

	// 浮点/边界兜底，理论上不会到达
	last := endpoints[len(endpoints)-1]
	return invoker.NewProviderInvoker(last.ProviderImpl, last)
}
