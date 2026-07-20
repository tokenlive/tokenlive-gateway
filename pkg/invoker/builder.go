package invoker

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// Builder implements core.InvokerBuilder.
type Builder struct{}

func NewBuilder() *Builder {
	return &Builder{}
}

var _ core.InvokerBuilder = (*Builder)(nil)

// BuildInvoker constructs a concrete Invoker from config.
func (b *Builder) BuildInvoker(cfg *core.InvokerConfig, r core.InvokerDependencyResolver) (core.Invoker, error) {
	switch cfg.Type {
	case "cluster":
		return buildClusterInvoker(cfg, r)
	case "hedging":
		return buildHedgingInvoker(cfg, r)
	default:
		return buildClusterInvoker(cfg, r)
	}
}

func buildClusterInvoker(invokerCfg *core.InvokerConfig, r core.InvokerDependencyResolver) (core.Invoker, error) {
	routers := r.ResolveRouters(invokerCfg.Routers)
	// Shared process-level circuit breaker for instance-level trips (avoids heavy Redis I/O).
	cbManager := r.CircuitBreakerManager()

	knownLBStrategies := []string{"round_robin", "weighted_rr", "random", "weighted_random", "least_connections", "least_latency", "cost", "sticky", "composite", "endpoint_affinity"}
	lbs := make(map[string]core.LoadBalancer)
	for _, name := range knownLBStrategies {
		if lb := r.ResolveLoadBalancer(name); lb != nil {
			lbs[name] = lb
		}
	}

	retry := DefaultRetryStrategy
	if invokerCfg.Retry != nil {
		retry = &policy.RetryPolicy{
			Retry:         invokerCfg.Retry.Retry,
			BackoffType:   invokerCfg.Retry.BackoffType,
			BaseMs:        invokerCfg.Retry.BaseMs,
			ErrorCodes:    invokerCfg.Retry.ErrorCodes,
			ErrorMessages: invokerCfg.Retry.ErrorMessages,
			CodePolicy:    invokerCfg.Retry.CodePolicy,
			MessagePolicy: invokerCfg.Retry.MessagePolicy,
		}
	}

	ci := NewClusterInvoker(
		r.Discovery(),
		routers,
		lbs,
		retry,
		cbManager,
		r.StateStore(),
		r.Logger(),
		r.Publisher(),
	)
	ci.SetEnableActive(r.EnableActiveHealthCheck())
	if invokerCfg.LoadBalancer != "" {
		ci.SetDefaultLBStrategy(invokerCfg.LoadBalancer)
	}

	return ci, nil
}

func buildHedgingInvoker(cfg *core.InvokerConfig, r core.InvokerDependencyResolver) (core.Invoker, error) {
	// Fallback serial invoker when hedging cannot dual-call.
	clusterCfg := *cfg
	clusterCfg.Type = "cluster"
	fallback, err := buildClusterInvoker(&clusterCfg, r)
	if err != nil {
		return nil, err
	}

	routers := r.ResolveRouters(cfg.Routers)
	cbManager := r.CircuitBreakerManager()

	knownLBStrategies := []string{"round_robin", "weighted_rr", "random", "weighted_random", "least_connections", "least_latency", "cost", "sticky", "composite", "endpoint_affinity"}
	lbs := make(map[string]core.LoadBalancer)
	for _, name := range knownLBStrategies {
		if lb := r.ResolveLoadBalancer(name); lb != nil {
			lbs[name] = lb
		}
	}

	hi := NewHedgingInvoker(
		r.Discovery(),
		routers,
		lbs,
		cbManager,
		r.StateStore(),
		r.Logger(),
		fallback,
	)
	hi.enableActive = r.EnableActiveHealthCheck()
	if cfg.LoadBalancer != "" {
		hi.defaultLBStrategy = cfg.LoadBalancer
	}
	return hi, nil
}
