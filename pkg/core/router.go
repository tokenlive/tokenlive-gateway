package core

// Router Endpoint 列表的硬约束过滤器。
// 每个 Router 实现一种过滤逻辑，多个 Router 串联形成 RouterChain。
// 返回的 Endpoint 列表为空时，由调用方决定是否 fallback。
type Router interface {
	// Name 返回路由器名称，用于日志和指标标签。
	Name() string
	// Route 从候选 Endpoint 列表中过滤出满足约束的子集。
	Route(gctx *GatewayContext, endpoints []*Endpoint) []*Endpoint
}
