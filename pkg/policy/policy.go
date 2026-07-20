package policy

import (
	"context"
	"encoding/json"
	"errors"
)

// Policy is the core policy config, mapping docs/policy.json.
type Policy struct {
	LoadBalancePolicy      *LoadBalancePolicy    `yaml:"load_balance_policy" json:"load_balance_policy"`
	InvocationPolicy       *InvocationPolicy     `yaml:"invocation_policy" json:"invocation_policy"`
	LimitPolicies          []*LimitPolicy        `yaml:"limit_policies" json:"limit_policies"`
	RoutePolicies          []*RoutePolicy        `yaml:"route_policies" json:"route_policies"`
	CircuitBreakPolicies   []*CircuitBreakPolicy `yaml:"circuit_break_policies" json:"circuit_break_policies"`
	TaggingPolicies        []*TaggingPolicy      `yaml:"tagging_policies" json:"tagging_policies"`
	Permissions            []string              `yaml:"permissions" json:"permissions"`
	Billing                *BillingPolicy        `yaml:"billing" json:"billing"`
	EnableMetricsReporting bool                  `yaml:"enable_metrics_reporting" json:"enable_metrics_reporting"`
}

// BillingPolicy is pricing per million tokens (CNY).
type BillingPolicy struct {
	InputPrice         float64 `yaml:"input_price" json:"input_price"`
	OutputPrice        float64 `yaml:"output_price" json:"output_price"`
	CachedPrice        float64 `yaml:"cached_price" json:"cached_price"`
	CacheCreationPrice float64 `yaml:"cache_creation_price" json:"cache_creation_price"`
}

// UnmarshalJSON accepts camelCase policy fields from Redis/Admin.
func (p *Policy) UnmarshalJSON(data []byte) error {
	type Alias Policy
	aux := &struct {
		LoadBalancePolicyCamel      *LoadBalancePolicy    `json:"loadBalancePolicy"`
		InvocationPolicyCamel       *InvocationPolicy     `json:"invocationPolicy"`
		LimitPoliciesCamel          []*LimitPolicy        `json:"limitPolicies"`
		RoutePoliciesCamel          []*RoutePolicy        `json:"routePolicies"`
		CircuitBreakPoliciesCamel   []*CircuitBreakPolicy `json:"circuitBreakPolicies"`
		TaggingPoliciesCamel        []*TaggingPolicy      `json:"taggingPolicies"`
		EnableMetricsReportingCamel *bool                 `json:"enableMetricsReporting"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if aux.LoadBalancePolicyCamel != nil {
		p.LoadBalancePolicy = aux.LoadBalancePolicyCamel
	}
	if aux.InvocationPolicyCamel != nil {
		p.InvocationPolicy = aux.InvocationPolicyCamel
	}
	if len(aux.LimitPoliciesCamel) > 0 {
		p.LimitPolicies = aux.LimitPoliciesCamel
	}
	if len(aux.RoutePoliciesCamel) > 0 {
		p.RoutePolicies = aux.RoutePoliciesCamel
	}
	if len(aux.CircuitBreakPoliciesCamel) > 0 {
		p.CircuitBreakPolicies = aux.CircuitBreakPoliciesCamel
	}
	if len(aux.TaggingPoliciesCamel) > 0 {
		p.TaggingPolicies = aux.TaggingPoliciesCamel
	}
	if aux.EnableMetricsReportingCamel != nil {
		p.EnableMetricsReporting = *aux.EnableMetricsReportingCamel
	}
	return nil
}

// UnmarshalJSON accepts camelCase billing fields.
func (b *BillingPolicy) UnmarshalJSON(data []byte) error {
	type Alias BillingPolicy
	aux := &struct {
		InputPriceCamel         *float64 `json:"inputPrice"`
		OutputPriceCamel        *float64 `json:"outputPrice"`
		CachedPriceCamel        *float64 `json:"cachedPrice"`
		CacheCreationPriceCamel *float64 `json:"cacheCreationPrice"`
		*Alias
	}{
		Alias: (*Alias)(b),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if aux.InputPriceCamel != nil {
		b.InputPrice = *aux.InputPriceCamel
	}
	if aux.OutputPriceCamel != nil {
		b.OutputPrice = *aux.OutputPriceCamel
	}
	if aux.CachedPriceCamel != nil {
		b.CachedPrice = *aux.CachedPriceCamel
	}
	if aux.CacheCreationPriceCamel != nil {
		b.CacheCreationPrice = *aux.CacheCreationPriceCamel
	}
	return nil
}

// PolicyProvider supplies policies (inverts core ↔ I/O).
type PolicyProvider interface {
	GetPolicy(ctx context.Context, tenantCode, userID, model string) (*Policy, error)
}

// PolicyMatcher merges policies in memory (stateless).
type PolicyMatcher struct{}

// DefaultPolicyMatcher is a package-level singleton safe for concurrent use.
var DefaultPolicyMatcher = &PolicyMatcher{}

// NewPolicyMatcher creates a PolicyMatcher.
func NewPolicyMatcher() *PolicyMatcher {
	return &PolicyMatcher{}
}

// Match merges policies in priority order (later overrides earlier).
func (pm *PolicyMatcher) Match(tenantCode, userID, model string, policies []*Policy) (*Policy, error) {
	var valid []*Policy
	for _, p := range policies {
		if p != nil {
			valid = append(valid, p)
		}
	}
	if len(valid) == 0 {
		return nil, errors.New("no dynamic policy matched")
	}
	return MergePolicies(valid...), nil
}

// MergePolicies field-merges policies from low to high priority.
func MergePolicies(policies ...*Policy) *Policy {
	result := &Policy{}

	for _, p := range policies {
		if p == nil {
			continue
		}

		if p.LoadBalancePolicy != nil {
			result.LoadBalancePolicy = p.LoadBalancePolicy
		}

		if p.Billing != nil {
			result.Billing = p.Billing
		}

		if p.InvocationPolicy != nil {
			result.InvocationPolicy = p.InvocationPolicy
		}

		// Name-based merge for LimitPolicies
		if len(p.LimitPolicies) > 0 {
			limitMap := make(map[string]*LimitPolicy)
			for _, item := range result.LimitPolicies {
				limitMap[item.Name] = item
			}
			for _, item := range p.LimitPolicies {
				if old, ok := limitMap[item.Name]; ok {
					limitMap[item.Name] = mergeLimitPolicy(old, item)
				} else {
					limitMap[item.Name] = item
				}
			}
			var newLimits []*LimitPolicy
			seen := make(map[string]bool)
			for _, item := range result.LimitPolicies {
				newLimits = append(newLimits, limitMap[item.Name])
				seen[item.Name] = true
			}
			for _, item := range p.LimitPolicies {
				if !seen[item.Name] {
					newLimits = append(newLimits, limitMap[item.Name])
				}
			}
			result.LimitPolicies = newLimits
		}

		// Name-based merge for RoutePolicies
		if len(p.RoutePolicies) > 0 {
			routeMap := make(map[string]*RoutePolicy)
			for _, item := range result.RoutePolicies {
				routeMap[item.Name] = item
			}
			for _, item := range p.RoutePolicies {
				if old, ok := routeMap[item.Name]; ok {
					routeMap[item.Name] = mergeRoutePolicy(old, item)
				} else {
					routeMap[item.Name] = item
				}
			}
			var newRoutes []*RoutePolicy
			seen := make(map[string]bool)
			for _, item := range result.RoutePolicies {
				newRoutes = append(newRoutes, routeMap[item.Name])
				seen[item.Name] = true
			}
			for _, item := range p.RoutePolicies {
				if !seen[item.Name] {
					newRoutes = append(newRoutes, routeMap[item.Name])
				}
			}
			result.RoutePolicies = newRoutes
		}

		// Name-based merge for CircuitBreakPolicies
		if len(p.CircuitBreakPolicies) > 0 {
			cbMap := make(map[string]*CircuitBreakPolicy)
			for _, item := range result.CircuitBreakPolicies {
				cbMap[item.Name] = item
			}
			for _, item := range p.CircuitBreakPolicies {
				if old, ok := cbMap[item.Name]; ok {
					cbMap[item.Name] = mergeCircuitBreakPolicy(old, item)
				} else {
					cbMap[item.Name] = item
				}
			}
			var newCBs []*CircuitBreakPolicy
			seen := make(map[string]bool)
			for _, item := range result.CircuitBreakPolicies {
				newCBs = append(newCBs, cbMap[item.Name])
				seen[item.Name] = true
			}
			for _, item := range p.CircuitBreakPolicies {
				if !seen[item.Name] {
					newCBs = append(newCBs, cbMap[item.Name])
				}
			}
			result.CircuitBreakPolicies = newCBs
		}

		// Name-based merge for TaggingPolicies
		if len(p.TaggingPolicies) > 0 {
			taggingMap := make(map[string]*TaggingPolicy)
			for _, item := range result.TaggingPolicies {
				taggingMap[item.Name] = item
			}
			for _, item := range p.TaggingPolicies {
				if old, ok := taggingMap[item.Name]; ok {
					taggingMap[item.Name] = mergeTaggingPolicy(old, item)
				} else {
					taggingMap[item.Name] = item
				}
			}
			var newTags []*TaggingPolicy
			seen := make(map[string]bool)
			for _, item := range result.TaggingPolicies {
				newTags = append(newTags, taggingMap[item.Name])
				seen[item.Name] = true
			}
			for _, item := range p.TaggingPolicies {
				if !seen[item.Name] {
					newTags = append(newTags, taggingMap[item.Name])
				}
			}
			result.TaggingPolicies = newTags
		}

		if p.Permissions != nil {
			result.Permissions = p.Permissions
		}

		// OR-merge: any true enables metrics reporting
		if p.EnableMetricsReporting {
			result.EnableMetricsReporting = true
		}
	}

	return result
}

func mergeLimitPolicy(target, source *LimitPolicy) *LimitPolicy {
	if target == nil {
		return source
	}
	if source == nil {
		return target
	}
	res := &LimitPolicy{
		ID:           target.ID,
		Name:         target.Name,
		Version:      target.Version,
		Type:         target.Type,
		MaxWaitMs:    target.MaxWaitMs,
		RelationType: target.RelationType,
	}
	if source.ID != "" {
		res.ID = source.ID
	}
	if source.Version > 0 {
		res.Version = source.Version
	}
	if source.Type != "" {
		res.Type = source.Type
	}
	if source.MaxWaitMs > 0 {
		res.MaxWaitMs = source.MaxWaitMs
	}
	if source.RelationType != "" {
		res.RelationType = source.RelationType
	}
	if len(source.SlidingWindows) > 0 {
		res.SlidingWindows = source.SlidingWindows
	} else {
		res.SlidingWindows = target.SlidingWindows
	}
	if len(source.Conditions) > 0 {
		res.Conditions = source.Conditions
	} else {
		res.Conditions = target.Conditions
	}
	if len(source.LimitBy) > 0 {
		res.LimitBy = source.LimitBy
	} else {
		res.LimitBy = target.LimitBy
	}
	return res
}

func mergeRoutePolicy(target, source *RoutePolicy) *RoutePolicy {
	if target == nil {
		return source
	}
	if source == nil {
		return target
	}
	res := &RoutePolicy{
		ID:      target.ID,
		Name:    target.Name,
		Version: target.Version,
		Order:   target.Order,
	}
	if source.ID != "" {
		res.ID = source.ID
	}
	if source.Version > 0 {
		res.Version = source.Version
	}
	if source.Order > 0 {
		res.Order = source.Order
	}
	if len(source.TagRules) > 0 {
		res.TagRules = source.TagRules
	} else {
		res.TagRules = target.TagRules
	}
	return res
}

func mergeCircuitBreakPolicy(target, source *CircuitBreakPolicy) *CircuitBreakPolicy {
	if target == nil {
		return source
	}
	if source == nil {
		return target
	}
	res := *target
	if source.ID != "" {
		res.ID = source.ID
	}
	if source.Level != "" {
		res.Level = source.Level
	}
	if source.SlidingWindowType != "" {
		res.SlidingWindowType = source.SlidingWindowType
	}
	if source.SlidingWindowSize > 0 {
		res.SlidingWindowSize = source.SlidingWindowSize
	}
	if source.MinCallsThreshold > 0 {
		res.MinCallsThreshold = source.MinCallsThreshold
	}
	if source.CodePolicy != nil {
		res.CodePolicy = source.CodePolicy
	}
	if len(source.ErrorCodes) > 0 {
		res.ErrorCodes = source.ErrorCodes
	}
	if source.FailureRateThreshold > 0 {
		res.FailureRateThreshold = source.FailureRateThreshold
	}
	if source.SlowCallRateThreshold > 0 {
		res.SlowCallRateThreshold = source.SlowCallRateThreshold
	}
	if source.SlowCallDurationThreshold > 0 {
		res.SlowCallDurationThreshold = source.SlowCallDurationThreshold
	}
	if source.WaitDurationInOpenState > 0 {
		res.WaitDurationInOpenState = source.WaitDurationInOpenState
	}
	if source.AllowedCallsInHalfOpenState > 0 {
		res.AllowedCallsInHalfOpenState = source.AllowedCallsInHalfOpenState
	}
	if source.ForceOpen != 0 {
		res.ForceOpen = source.ForceOpen
	}
	if source.OutlierMaxPercent > 0 {
		res.OutlierMaxPercent = source.OutlierMaxPercent
	}
	if source.DegradeConfig != nil {
		res.DegradeConfig = source.DegradeConfig
	}
	if source.Version > 0 {
		res.Version = source.Version
	}
	return &res
}

func mergeTaggingPolicy(target, source *TaggingPolicy) *TaggingPolicy {
	if target == nil {
		return source
	}
	if source == nil {
		return target
	}
	res := &TaggingPolicy{
		ID:      target.ID,
		Name:    target.Name,
		Version: target.Version,
		Order:   target.Order,
	}
	if source.ID != "" {
		res.ID = source.ID
	}
	if source.Version > 0 {
		res.Version = source.Version
	}
	if source.Order > 0 {
		res.Order = source.Order
	}
	if source.Relation != "" {
		res.Relation = source.Relation
	} else {
		res.Relation = target.Relation
	}
	if len(source.Conditions) > 0 {
		res.Conditions = source.Conditions
	} else {
		res.Conditions = target.Conditions
	}
	if len(source.Actions) > 0 {
		res.Actions = source.Actions
	} else {
		res.Actions = target.Actions
	}
	return res
}
