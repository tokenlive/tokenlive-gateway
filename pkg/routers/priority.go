package routers

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// PriorityRouter keeps only highest-priority (lowest Priority value) endpoints.
// Must run after CircuitBreakerRouter.
// Keeps endpoints with the minimum Priority value.
// Ties stay as candidates for the load balancer.
type PriorityRouter struct {
	logger *zap.Logger
}

// NewPriorityRouter creates a PriorityRouter.
func NewPriorityRouter(logger *zap.Logger) *PriorityRouter {
	return &PriorityRouter{logger: logger}
}

// Name returns the router name.
func (r *PriorityRouter) Name() string { return "priority" }

// Route filters to min-priority endpoints.
func (r *PriorityRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	if len(endpoints) == 0 {
		return endpoints
	}

	// Find minimum Priority (lower is higher priority).
	minPriority := endpoints[0].Priority
	for _, ep := range endpoints[1:] {
		if ep.Priority < minPriority {
			minPriority = ep.Priority
		}
	}

	// Keep endpoints with Priority == minPriority.
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if ep.Priority == minPriority {
			result = append(result, ep)
		} else {
			r.logger.Debug("priority router: skip endpoint due to lower priority",
				zap.String("endpoint_id", ep.ID),
				zap.Int("priority", ep.Priority),
				zap.Int("min_priority", minPriority))
		}
	}

	return result
}
