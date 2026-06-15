package routers

import "github.com/tokenlive/tokenlive-gateway/pkg/core"

// CapabilityRouter 根据请求类型过滤不支持该类型的 Endpoint。
// 例如：embedding 请求只路由到声明了 embedding 能力的 Endpoint。
type CapabilityRouter struct{}

func (r *CapabilityRouter) Name() string { return "capability" }

func (r *CapabilityRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if ep.SupportsRequestType(gctx.RequestType) {
			result = append(result, ep)
		}
	}
	return result
}
