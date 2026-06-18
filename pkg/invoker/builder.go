package invoker

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// Builder 实现 core.InvokerBuilder 接口
type Builder struct{}

func NewBuilder() *Builder {
	return &Builder{}
}

// 静态检查接口实现
var _ core.InvokerBuilder = (*Builder)(nil)

// BuildInvoker 构造具体的 Invoker
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
	// 获取共享的进程级熔断管理器，实现实例级熔断，避免 Redis 大量读写。
	cbManager := r.CircuitBreakerManager()

	// 获取所有的负载均衡实例
	knownLBStrategies := []string{"round_robin", "weighted_rr", "random", "weighted_random", "least_connections", "least_latency", "cost", "sticky", "composite"}
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
	)
	ci.SetEnableActive(r.EnableActiveHealthCheck())
	if invokerCfg.LoadBalancer != "" {
		ci.SetDefaultLBStrategy(invokerCfg.LoadBalancer)
	}

	return ci, nil
}

func buildHedgingInvoker(cfg *core.InvokerConfig, r core.InvokerDependencyResolver) (core.Invoker, error) {
	// 1. 构建 fallback 串行调用器（用来作为对冲退化时的容错）
	clusterCfg := *cfg
	clusterCfg.Type = "cluster"
	fallback, err := buildClusterInvoker(&clusterCfg, r)
	if err != nil {
		return nil, err
	}

	// 2. 准备依赖
	routers := r.ResolveRouters(cfg.Routers)
	cbManager := r.CircuitBreakerManager()

	knownLBStrategies := []string{"round_robin", "weighted_rr", "random", "weighted_random", "least_connections", "least_latency", "cost", "sticky", "composite"}
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
