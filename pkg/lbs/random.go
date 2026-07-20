package lbs

import (
	"crypto/rand"
	"math/big"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// RandomLoadBalancer picks a random endpoint.
type RandomLoadBalancer struct{}

// NewRandomLoadBalancer creates a random LB.
func NewRandomLoadBalancer() *RandomLoadBalancer {
	return &RandomLoadBalancer{}
}

// Select picks a random endpoint.
func (lb *RandomLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	idx := randomInt(len(endpoints))
	ep := endpoints[idx]
	return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
}

// randomInt returns a crypto/rand int in [0, n).
func randomInt(n int) int {
	bigN := big.NewInt(int64(n))
	val, err := rand.Int(rand.Reader, bigN)
	if err != nil {
		// On crypto/rand error, fall back to 0.
		return 0
	}
	return int(val.Int64())
}
