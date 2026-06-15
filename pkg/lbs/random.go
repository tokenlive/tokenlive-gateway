package lbs

import (
	"crypto/rand"
	"math/big"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// RandomLoadBalancer 随机负载均衡器
type RandomLoadBalancer struct{}

// NewRandomLoadBalancer 创建随机负载均衡器
func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{}
}

// Select 随机选择一个端点
func (lb *RandomLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	idx := randomInt(len(endpoints))
	ep := endpoints[idx]
	return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
}

// randomInt 使用 crypto/rand 生成 [0, n) 范围的随机整数
func randomInt(n int) int {
	bigN := big.NewInt(int64(n))
	val, err := rand.Int(rand.Reader, bigN)
	if err != nil {
		// crypto/rand 极少出错，回退到 0
		return 0
	}
	return int(val.Int64())
}
