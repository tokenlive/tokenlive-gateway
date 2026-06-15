package core

// LoadBalancer 从候选列表中选一个 Invoker
type LoadBalancer interface {
	Select(gctx *GatewayContext, endpoints []*Endpoint) Invoker
}
