package lbs

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// RoundRobin 轮询负载均衡器
type RoundRobin struct {
	mu       sync.Mutex
	counters map[string]*uint64
}

// NewRoundRobin 创建轮询负载均衡器
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{
		counters: make(map[string]*uint64),
	}
}

// Select 轮询选择一个端点
func (lb *RoundRobin) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	counter := lb.counterFor(routePoolKey(gctx, endpoints))
	idx := atomic.AddUint64(counter, 1) - 1
	ep := endpoints[idx%uint64(len(endpoints))]
	return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
}

func (lb *RoundRobin) counterFor(key string) *uint64 {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	counter, ok := lb.counters[key]
	if ok {
		return counter
	}
	counter = new(uint64)
	lb.counters[key] = counter
	return counter
}

func routePoolKey(gctx *core.GatewayContext, endpoints []*core.Endpoint) string {
	var b strings.Builder
	if gctx != nil && gctx.Model != "" {
		b.WriteString(gctx.Model)
	} else if len(endpoints) > 0 && endpoints[0].Model != "" {
		b.WriteString(endpoints[0].Model)
	} else {
		b.WriteString("__default__")
	}
	b.WriteByte('|')
	for i, ep := range endpoints {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(ep.ID)
	}
	return b.String()
}
