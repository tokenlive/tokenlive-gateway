package core

import (
	"fmt"
	"sort"

	"go.uber.org/zap"
)

// buildPipeline 从配置构建 Pipeline
func (e *Engine) buildPipeline(cfg *PipelineConfig) (*Pipeline, error) {
	p := &Pipeline{
		Name:         cfg.Name,
		RequestTypes: cfg.RequestTypes,
	}

	// 构建 InboundFilters
	for _, name := range cfg.InboundFilters {
		f, ok := e.getInboundFilter(name)
		if !ok {
			return nil, fmt.Errorf("inbound filter %q not found in registry", name)
		}
		p.InboundFilters = append(p.InboundFilters, f)
	}

	// 按 Order 排序 InboundFilters
	sort.Slice(p.InboundFilters, func(i, j int) bool {
		return p.InboundFilters[i].Order() < p.InboundFilters[j].Order()
	})

	// 构建 OutboundFilters
	for _, name := range cfg.OutboundFilters {
		f, ok := e.getOutboundFilter(name)
		if !ok {
			return nil, fmt.Errorf("outbound filter %q not found in registry", name)
		}
		p.OutboundFilters = append(p.OutboundFilters, f)
	}

	// 按 Order 排序 OutboundFilters
	sort.Slice(p.OutboundFilters, func(i, j int) bool {
		return p.OutboundFilters[i].Order() < p.OutboundFilters[j].Order()
	})

	// 构建 CriticalOutboundFilters 集合
	if len(cfg.CriticalOutboundFilters) > 0 {
		p.CriticalOutboundFilters = make(map[string]bool, len(cfg.CriticalOutboundFilters))
		for _, name := range cfg.CriticalOutboundFilters {
			p.CriticalOutboundFilters[name] = true
		}
	}

	// 构建 Invoker
	invoker, err := e.buildInvoker(&cfg.Invoker, cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("build invoker: %w", err)
	}
	p.Invoker = invoker

	// 初始化多态运行时 Invoker 注册表
	p.Invokers = make(map[string]Invoker)
	defaultType := cfg.Invoker.Type
	if defaultType == "" {
		defaultType = "cluster"
	}
	p.Invokers[defaultType] = invoker
	if defaultType == "cluster" {
		p.Invokers["failover"] = invoker

		// 自动为 cluster 类型的管道生成并注册对冲 (hedging) 调用器
		hedgingCfg := &InvokerConfig{
			Type:         "hedging",
			Routers:      cfg.Invoker.Routers,
			LoadBalancer: cfg.Invoker.LoadBalancer,
			Retry:        cfg.Invoker.Retry,
		}
		if hedgingInvoker, err := e.buildInvoker(hedgingCfg, cfg.Name); err == nil {
			p.Invokers["hedging"] = hedgingInvoker
		} else {
			e.logger.Warn("failed to build hedging invoker", zap.Error(err))
		}
	}
	return p, nil
}

// buildInvoker 从配置构建 Invoker
func (e *Engine) buildInvoker(cfg *InvokerConfig, pipelineName string) (Invoker, error) {
	if e.invokerBuilder == nil {
		return nil, fmt.Errorf("invoker builder not set in engine")
	}
	return e.invokerBuilder.BuildInvoker(cfg, e)
}

// resolveRouters 根据名称列表从工厂注册表创建 Router chain。
// 空列表使用默认 [capability, circuit_breaker]。
// 注意：调用方必须已持有 e.mu 锁（Init/UpdateConfig/buildPipeline 路径）。
func (e *Engine) resolveRouters(names []string) []Router {
	if len(names) == 0 {
		names = []string{"capability", "circuit_breaker", "priority"}
	}

	var routers []Router
	for _, name := range names {
		factory, ok := e.routerFactories[name]
		if !ok {
			e.logger.Warn("router factory not found, skipping", zap.String("name", name))
			continue
		}
		routers = append(routers, factory(RouterConfig{Name: name}, e.stateStore, e.logger))
	}
	return routers
}

// resolveLoadBalancer 根据名称从工厂注册表创建 LoadBalancer。
// 空名称使用默认 "round_robin"。
// 注意：调用方必须已持有 e.mu 锁（Init/UpdateConfig/buildPipeline 路径）。
func (e *Engine) resolveLoadBalancer(name string) LoadBalancer {
	if name == "" {
		name = "round_robin"
	}

	factory, ok := e.lbFactories[name]
	if !ok {
		e.logger.Warn("load balancer factory not found, falling back to round_robin", zap.String("name", name))
		factory = e.lbFactories["round_robin"]
	}
	return factory(e.stateStore)
}
