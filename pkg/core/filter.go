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

// InboundSafeFilter 标记接口：实现此接口的 OutboundFilter 在 InboundError 路径也会执行。
// InboundError 路径指请求被 InboundFilter 拒截、未到达 Invoker 的场景。
type InboundSafeFilter interface {
	InboundSafe()
}
