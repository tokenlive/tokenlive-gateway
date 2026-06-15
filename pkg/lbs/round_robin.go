package lbs

import (
	"sync/atomic"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// RoundRobin 轮询负载均衡器
type RoundRobin struct {
	counter uint64
}

// NewRoundRobin 创建轮询负载均衡器
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

// Select 轮询选择一个端点
func (lb *RoundRobin) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	idx := atomic.AddUint64(&lb.counter, 1) - 1
	ep := endpoints[idx%uint64(len(endpoints))]
	return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
}
