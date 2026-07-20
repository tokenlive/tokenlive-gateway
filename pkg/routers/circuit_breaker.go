package routers

import (
	"sort"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// CircuitBreakerRouter drops endpoints in Open circuit state.
// Uses CircuitBreakerManager for service- and instance-level breakers.
// Two layers:
//   - service (provider:model): whole service down
//   - instance (endpoint ID): single instance down
type CircuitBreakerRouter struct {
	cbManager    *core.CircuitBreakerManager
	enableActive bool
	logger       *zap.Logger
}

// NewCircuitBreakerRouter creates a CircuitBreakerRouter.
func NewCircuitBreakerRouter(cbManager *core.CircuitBreakerManager, enableActive bool, logger *zap.Logger) *CircuitBreakerRouter {
	return &CircuitBreakerRouter{cbManager: cbManager, enableActive: enableActive, logger: logger}
}

func (r *CircuitBreakerRouter) Name() string { return "circuit_breaker" }

func (r *CircuitBreakerRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	if len(endpoints) == 0 {
		return endpoints
	}

	// Pre-bind breaker keys to model for metrics labels.
	for _, ep := range endpoints {
		serviceKey := ep.Provider + ":" + ep.Model
		r.cbManager.GetEntryWithModel(serviceKey, ep.Model)
		r.cbManager.GetEntryWithModel(ep.ID, ep.Model)
	}

	if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
		for _, p := range gctx.Policy.CircuitBreakPolicies {
			if p == nil {
				continue
			}
			level := strings.ToUpper(p.Level)
			if level == "" || level == "SERVICE" {
				for _, ep := range endpoints {
					serviceKey := ep.Provider + ":" + ep.Model
					r.cbManager.CheckAndResetOnVersionChange(serviceKey, p.Version)
				}
			}
			if level == "" || level == "INSTANCE" || level == "ENDPOINT" {
				for _, ep := range endpoints {
					r.cbManager.CheckAndResetOnVersionChange(ep.ID, p.Version)
				}
			}
		}
	}

	// 1. Drop service-level Open endpoints.
	var servicePassed []*core.Endpoint
	for _, ep := range endpoints {
		serviceKey := ep.Provider + ":" + ep.Model
		if !r.cbManager.AllowRequest(serviceKey, r.enableActive) {
			r.logger.Warn("circuit breaker: service not allowed, skipping",
				zap.String("key", serviceKey))
			continue
		}
		servicePassed = append(servicePassed, ep)
	}

	if len(servicePassed) == 0 {
		return nil
	}

	// 2. Group by Provider:Model; cap instance Open by outlier_max_percent.
	groups := make(map[string][]*core.Endpoint)
	for _, ep := range servicePassed {
		key := ep.Provider + ":" + ep.Model
		groups[key] = append(groups[key], ep)
	}

	// Min non-zero OutlierMaxPercent from instance policies.
	var maxPercent int = 0
	hasInstancePolicy := false
	if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
		for _, p := range gctx.Policy.CircuitBreakPolicies {
			if p == nil {
				continue
			}
			level := strings.ToUpper(p.Level)
			if level == "" || level == "INSTANCE" || level == "ENDPOINT" {
				hasInstancePolicy = true
				if p.OutlierMaxPercent > 0 {
					if maxPercent == 0 || p.OutlierMaxPercent < maxPercent {
						maxPercent = p.OutlierMaxPercent
					}
				}
			}
		}
	}

	blockedIDs := make(map[string]bool)

	for _, groupEPs := range groups {
		var groupBlocked []*core.Endpoint
		for _, ep := range groupEPs {
			if !r.cbManager.AllowRequest(ep.ID, r.enableActive) {
				groupBlocked = append(groupBlocked, ep)
			}
		}

		if len(groupBlocked) == 0 {
			continue
		}

		// Instance policy with valid outlier_max_percent.
		if hasInstancePolicy && maxPercent > 0 {
			totalInGroup := len(groupEPs)
			maxAllowed := totalInGroup * maxPercent / 100
			if maxAllowed == 0 && totalInGroup > 1 {
				maxAllowed = 1
			}

			if len(groupBlocked) > maxAllowed {
				// Sort by open time asc (exclude earliest first); ID as tiebreak.
				sort.Slice(groupBlocked, func(i, j int) bool {
					ti := r.cbManager.GetOpenSince(groupBlocked[i].ID)
					tj := r.cbManager.GetOpenSince(groupBlocked[j].ID)
					if ti.Equal(tj) {
						return groupBlocked[i].ID < groupBlocked[j].ID
					}
					return ti.Before(tj)
				})

				// Keep first maxAllowed Open; release the rest.
				for i := 0; i < maxAllowed; i++ {
					blockedIDs[groupBlocked[i].ID] = true
				}
				r.logger.Warn("circuit breaker: instance percent limit hit",
					zap.Int("total", totalInGroup),
					zap.Int("blocked", len(groupBlocked)),
					zap.Int("maxAllowed", maxAllowed),
					zap.Int("finalBlocked", maxAllowed),
					zap.Strings("keptBlocked", func() []string {
						var ids []string
						for i := 0; i < maxAllowed; i++ {
							ids = append(ids, groupBlocked[i].ID)
						}
						return ids
					}()),
					zap.Strings("released", func() []string {
						var ids []string
						for i := maxAllowed; i < len(groupBlocked); i++ {
							ids = append(ids, groupBlocked[i].ID)
						}
						return ids
					}()))
			} else {
				// Under cap: keep Open.
				for _, ep := range groupBlocked {
					blockedIDs[ep.ID] = true
				}
			}
		} else {
			// No percent limit: open all matching instances.
			for _, ep := range groupBlocked {
				blockedIDs[ep.ID] = true
			}
		}
	}

	// 3. Assemble final filtered list.
	var result []*core.Endpoint
	for _, ep := range servicePassed {
		if !blockedIDs[ep.ID] {
			result = append(result, ep)
		} else {
			r.logger.Warn("circuit breaker: instance not allowed, skipping",
				zap.String("endpoint", ep.ID))
		}
	}
	return result
}
