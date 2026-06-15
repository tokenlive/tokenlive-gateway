package lbs

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// StickyLoadBalancer 会话粘滞负载均衡器
// 优先使用之前会话选择的端点，如果没有则回退到备用负载均衡器
type StickyLoadBalancer struct {
	stateStore core.StateStore
	fallback   core.LoadBalancer
	keyFunc    func(gctx *core.GatewayContext) string
	ttl        time.Duration
}

// NewStickyLoadBalancer 创建会话粘滞负载均衡器
func NewStickyLoadBalancer(ss core.StateStore, fallback core.LoadBalancer, keyFunc func(*core.GatewayContext) string, ttl time.Duration) *StickyLoadBalancer {
	return &StickyLoadBalancer{
		stateStore: ss,
		fallback:   fallback,
		keyFunc:    keyFunc,
		ttl:        ttl,
	}
}

// Select 选择端点，优先使用会话粘滞
func (lb *StickyLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	key := lb.keyFunc(gctx)
	if key != "" {
		epID, _ := lb.stateStore.StickyGet(context.Background(), key)
		if epID != "" {
			for _, ep := range endpoints {
				if ep.ID == epID {
					return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
				}
			}
		}
	}
	return lb.fallback.Select(gctx, endpoints)
}
