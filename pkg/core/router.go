package core

// Router filters endpoint candidates (hard constraints).
// Multiple routers form a RouterChain.
// Empty result: caller decides whether to fallback.
type Router interface {
	// Name returns the router name (logs/metrics).
	Name() string
	// Route filters candidates to those matching constraints.
	Route(gctx *GatewayContext, endpoints []*Endpoint) []*Endpoint
}
