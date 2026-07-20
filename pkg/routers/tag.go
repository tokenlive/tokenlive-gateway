package routers

import (
	"math/rand"
	"sort"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// TagRouter filters endpoints by tag/route policies.
type TagRouter struct {
	logger *zap.Logger
}

// NewTagRouter creates a TagRouter.
func NewTagRouter(logger *zap.Logger) *TagRouter {
	return &TagRouter{logger: logger}
}

func (r *TagRouter) Name() string { return "tag" }

// Route filters candidates by matching route policies.
func (r *TagRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	if gctx.Policy == nil || len(gctx.Policy.RoutePolicies) == 0 {
		return endpoints
	}

	// Copy and sort RoutePolicies by Order ascending.
	routePolicies := make([]*policy.RoutePolicy, len(gctx.Policy.RoutePolicies))
	copy(routePolicies, gctx.Policy.RoutePolicies)
	sort.Slice(routePolicies, func(i, j int) bool {
		return routePolicies[i].Order < routePolicies[j].Order
	})

	for _, rp := range routePolicies {
		// Sort TagRules.
		tagRules := rp.TagRules
		sort.Slice(tagRules, func(i, j int) bool {
			return tagRules[i].Order < tagRules[j].Order
		})

		for _, rule := range tagRules {
			if r.matchConditions(gctx, rule.Conditions, rule.RelationType) {
				// Rule hit: pick Destination by weight.
				dest := r.selectDestination(rule.Destinations)
				if dest == nil {
					continue
				}

				// Filter endpoints by selected Destination.
				filtered := r.filterEndpoints(endpoints, dest.Conditions, dest.RelationType)
				if len(filtered) > 0 {
					r.logger.Info("dynamic route matched successful subset",
						zap.String("policy", rp.Name),
						zap.Int("selected_subset_size", len(filtered)))
					return filtered
				}

				// Empty subset: escape, warn, try other rules, else default pool.
				r.logger.Warn("dynamic route matched subset is empty, triggering fallback to default pool",
					zap.String("policy", rp.Name))
			}
		}
	}

	return endpoints
}

func (r *TagRouter) matchConditions(gctx *core.GatewayContext, conds []*matcher.TagCondition, relation string) bool {
	var validConds []*matcher.TagCondition
	for _, cond := range conds {
		if cond.Type != "" {
			validConds = append(validConds, cond)
		}
	}
	if len(validConds) == 0 {
		return true
	}

	isAnd := relation != "OR"
	for _, cond := range validConds {
		m := matcher.DefaultTagMatcherFactory.Get(strings.ToLower(cond.Type))
		matched := m != nil && m.Match(gctx.Ctx, cond, gctx)
		if isAnd && !matched {
			return false
		}
		if !isAnd && matched {
			return true
		}
	}
	return isAnd
}

func (r *TagRouter) selectDestination(dests []*policy.Destination) *policy.Destination {
	if len(dests) == 0 {
		return nil
	}
	totalWeight := 0
	for _, d := range dests {
		totalWeight += d.Weight
	}
	if totalWeight <= 0 {
		return dests[0]
	}
	val := rand.Intn(totalWeight)
	curr := 0
	for _, d := range dests {
		curr += d.Weight
		if val < curr {
			return d
		}
	}
	return dests[0]
}

func (r *TagRouter) filterEndpoints(endpoints []*core.Endpoint, conds []*matcher.TagCondition, relation string) []*core.Endpoint {
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if r.matchEndpointMetadata(ep, conds, relation) {
			result = append(result, ep)
		}
	}
	return result
}

func (r *TagRouter) matchEndpointMetadata(ep *core.Endpoint, conds []*matcher.TagCondition, relation string) bool {
	if len(conds) == 0 {
		return true
	}
	isAnd := relation != "OR"
	for _, cond := range conds {
		val := ep.Metadata[cond.Key]
		var matched bool
		if val != "" {
			matched = cond.MatchValues([]string{val})
		} else {
			matched = cond.MatchValues(nil)
		}

		if isAnd && !matched {
			return false
		}
		if !isAnd && matched {
			return true
		}
	}
	return isAnd
}
