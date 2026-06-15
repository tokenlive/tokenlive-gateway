package lbs

import (
	"sync"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// WeightedRoundRobinLoadBalancer 加权轮询负载均衡器
// 采用平滑加权轮询算法（Smooth Weighted Round-Robin）
type WeightedRoundRobinLoadBalancer struct {
	mu      sync.Mutex
	weights map[string]int // 当前有效权重
}

// NewWeightedRoundRobinLoadBalancer 创建加权轮询负载均衡器
func NewWeightedRoundRobinLoadBalancer() *WeightedRoundRobinLoadBalancer {
	return &WeightedRoundRobinLoadBalancer{
		weights: make(map[string]int),
	}
}

// Select 使用平滑加权轮询算法选择端点
func (lb *WeightedRoundRobinLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// 计算总权重并初始化有效权重
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

	// 平滑加权轮询：每个端点 current += weight，选最大的，再减 totalWeight
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
