package core

// LoadBalancer picks one Invoker from candidates.
type LoadBalancer interface {
	Select(gctx *GatewayContext, endpoints []*Endpoint) Invoker
}
