package lbs

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// StickyLoadBalancer prefers the session-bound endpoint.
// Prefer prior session endpoint; else fallback LB.
type StickyLoadBalancer struct {
	stateStore core.StateStore
	fallback   core.LoadBalancer
	keyFunc    func(gctx *core.GatewayContext) string
	ttl        time.Duration
}

// NewStickyLoadBalancer creates a sticky LB.
func NewStickyLoadBalancer(ss core.StateStore, fallback core.LoadBalancer, keyFunc func(*core.GatewayContext) string, ttl time.Duration) *StickyLoadBalancer {
	return &StickyLoadBalancer{
		stateStore: ss,
		fallback:   fallback,
		keyFunc:    keyFunc,
		ttl:        ttl,
	}
}

// Select prefers sticky session endpoint.
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
