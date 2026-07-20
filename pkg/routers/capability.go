package routers

import "github.com/tokenlive/tokenlive-gateway/pkg/core"

// CapabilityRouter drops endpoints that do not support the request type.
// e.g. embedding requests only reach embedding-capable endpoints.
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
