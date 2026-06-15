package core

// FilterCriticality Filter 关键性
type FilterCriticality int

const (
	BestEffort FilterCriticality = iota
	Critical
)

// InboundFilter 请求进入 ClusterInvoker 前执行
type InboundFilter interface {
	Name() string
	Order() int
	OnRequest(gctx *GatewayContext) error
}

// OutboundFilter 响应离开后执行
type OutboundFilter interface {
	Name() string
	Order() int
	Criticality() FilterCriticality
	OnResponse(gctx *GatewayContext) error
}
