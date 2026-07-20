package core

// FilterCriticality is filter criticality.
type FilterCriticality int

const (
	BestEffort FilterCriticality = iota
	Critical
)

// InboundFilter runs before ClusterInvoker.
type InboundFilter interface {
	Name() string
	Order() int
	OnRequest(gctx *GatewayContext) error
}

// OutboundFilter runs after the response leaves the invoker.
type OutboundFilter interface {
	Name() string
	Order() int
	Criticality() FilterCriticality
	OnResponse(gctx *GatewayContext) error
}

// InboundSafeFilter marks OutboundFilters that also run on InboundError paths.
// InboundError: request rejected by InboundFilter before reaching Invoker.
type InboundSafeFilter interface {
	InboundSafe()
}
