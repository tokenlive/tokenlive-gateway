package lbs

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// maxHashWeight caps virtual entries per endpoint so a huge configured
// weight cannot blow up the rendezvous scan.
const maxHashWeight = 128

// SessionHashLoadBalancer maps a session deterministically to an endpoint
// via weighted rendezvous hashing (HRW). It is stateless: no mapping is
// stored, so bindings survive idle gaps and gateway restarts, and only
// membership changes in the endpoint set can reassign a session.
type SessionHashLoadBalancer struct {
	fallback core.LoadBalancer
}

// NewSessionHashLoadBalancer creates a session-hash LB.
// fallback is used when the request carries no SessionID.
func NewSessionHashLoadBalancer(fallback core.LoadBalancer) *SessionHashLoadBalancer {
	return &SessionHashLoadBalancer{fallback: fallback}
}

// Select picks the endpoint with the highest rendezvous score for the session.
func (lb *SessionHashLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	sessionID := gctx.SessionID
	if sessionID == "" {
		if lb.fallback == nil {
			return nil
		}
		return lb.fallback.Select(gctx, endpoints)
	}

	var best *core.Endpoint
	var bestScore uint64
	for _, ep := range endpoints {
		weight := ep.Weight
		if weight < 1 {
			weight = 1
		}
		if weight > maxHashWeight {
			weight = maxHashWeight
		}
		for i := 0; i < weight; i++ {
			score := rendezvousScore(sessionID, ep.ID, i)
			if score > bestScore {
				bestScore = score
				best = ep
			}
		}
	}
	if best == nil {
		return nil
	}
	return invoker.NewProviderInvoker(best.ProviderImpl, best)
}

func rendezvousScore(sessionID, endpointID string, replica int) uint64 {
	h := fnv.New64a()
	h.Write([]byte(sessionID))
	h.Write([]byte{'|'})
	h.Write([]byte(endpointID))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(replica))
	h.Write(buf[:])
	return mix64(h.Sum64())
}

// mix64 applies a splitmix64-style finalizer to compensate for FNV-1a's poor
// avalanche: without it, inputs differing only in the last 8 bytes (the
// replica index) yield correlated scores and weights stop influencing the
// rendezvous outcome.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
