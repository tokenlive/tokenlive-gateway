package lbs

import (
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// LeastLatencyLoadBalancer 最低延迟负载均衡器
type LeastLatencyLoadBalancer struct {
	stateStore core.StateStore
	window     time.Duration
}

// NewLeastLatencyLoadBalancer 创建最低延迟负载均衡器
func NewLeastLatencyLoadBalancer(ss core.StateStore) *LeastLatencyLoadBalancer {
	return &LeastLatencyLoadBalancer{
		stateStore: ss,
		window:     5 * time.Minute,
	}
}

// Select 选择平均延迟最低的端点
func (lb *LeastLatencyLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	var selected *core.Endpoint
	var minLatency time.Duration = -1

	for _, ep := range endpoints {
		avgLatency, err := lb.stateStore.GetAvgLatency(gctx.Ctx, ep.ID, lb.window)
		if err != nil {
			// 查询失败时视为无限延迟，跳过
			continue
		}

		// 0 延迟（无采样数据）视为可选
		if minLatency < 0 || avgLatency < minLatency {
			minLatency = avgLatency
			selected = ep
		}
	}

	// 如果所有端点查询都失败，回退选择第一个
	if selected == nil {
		selected = endpoints[0]
	}

	return invoker.NewProviderInvoker(selected.ProviderImpl, selected)
}
