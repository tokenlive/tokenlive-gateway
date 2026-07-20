package core

import (
	"fmt"
	"sort"

	"go.uber.org/zap"
)

// buildPipeline builds a Pipeline from config.
func (e *Engine) buildPipeline(cfg *PipelineConfig) (*Pipeline, error) {
	p := &Pipeline{
		Name:         cfg.Name,
		RequestTypes: cfg.RequestTypes,
	}

	// Build InboundFilters
	for _, name := range cfg.InboundFilters {
		f, ok := e.getInboundFilter(name)
		if !ok {
			return nil, fmt.Errorf("inbound filter %q not found in registry", name)
		}
		p.InboundFilters = append(p.InboundFilters, f)
	}

	// Sort InboundFilters by Order
	sort.Slice(p.InboundFilters, func(i, j int) bool {
		return p.InboundFilters[i].Order() < p.InboundFilters[j].Order()
	})

	// Build OutboundFilters
	for _, name := range cfg.OutboundFilters {
		f, ok := e.getOutboundFilter(name)
		if !ok {
			return nil, fmt.Errorf("outbound filter %q not found in registry", name)
		}
		p.OutboundFilters = append(p.OutboundFilters, f)
	}

	// Sort OutboundFilters by Order
	sort.Slice(p.OutboundFilters, func(i, j int) bool {
		return p.OutboundFilters[i].Order() < p.OutboundFilters[j].Order()
	})

	// Build CriticalOutboundFilters set
	if len(cfg.CriticalOutboundFilters) > 0 {
		p.CriticalOutboundFilters = make(map[string]bool, len(cfg.CriticalOutboundFilters))
		for _, name := range cfg.CriticalOutboundFilters {
			p.CriticalOutboundFilters[name] = true
		}
	}

	// Build Invoker
	invoker, err := e.buildInvoker(&cfg.Invoker, cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("build invoker: %w", err)
	}
	p.Invoker = invoker

	// Initialize polymorphic runtime Invoker registry
	p.Invokers = make(map[string]Invoker)
	defaultType := cfg.Invoker.Type
	if defaultType == "" {
		defaultType = "cluster"
	}
	p.Invokers[defaultType] = invoker
	if defaultType == "cluster" {
		p.Invokers["failover"] = invoker

		// Auto-generate and register hedging invoker for cluster-type pipelines
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

// buildInvoker builds an Invoker from config.
func (e *Engine) buildInvoker(cfg *InvokerConfig, pipelineName string) (Invoker, error) {
	if e.invokerBuilder == nil {
		return nil, fmt.Errorf("invoker builder not set in engine")
	}
	return e.invokerBuilder.BuildInvoker(cfg, e)
}

// resolveRouters creates a Router chain from the factory registry by name list.
// Empty list defaults to [capability, circuit_breaker].
// Caller must hold e.mu lock (Init/UpdateConfig/buildPipeline path).
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

// resolveLoadBalancer creates a LoadBalancer from the factory registry by name.
// Empty name defaults to "round_robin".
// Caller must hold e.mu lock (Init/UpdateConfig/buildPipeline path).
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
