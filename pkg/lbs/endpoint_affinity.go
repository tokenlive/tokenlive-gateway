package lbs

import (
	"strconv"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// EndpointAffinityLoadBalancer 端点亲和负载均衡器
type EndpointAffinityLoadBalancer struct {
	stateStore core.StateStore
	fallback   core.LoadBalancer
}

// NewEndpointAffinityLoadBalancer 创建端点亲和负载均衡器
func NewEndpointAffinityLoadBalancer(ss core.StateStore) *EndpointAffinityLoadBalancer {
	return &EndpointAffinityLoadBalancer{
		stateStore: ss,
		fallback:   NewRoundRobin(),
	}
}

// Select 选择端点，优先匹配亲和值
func (lb *EndpointAffinityLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	sourceType := "header"
	sourceKey := "X-Endpoint-Code"
	allowDegrade := true

	// 1. 尝试从策略配置中提取 sourceType、sourceKey 和 allowDegrade
	if gctx != nil && gctx.Policy != nil && gctx.Policy.LoadBalancePolicy != nil && gctx.Policy.LoadBalancePolicy.Params != nil {
		params := gctx.Policy.LoadBalancePolicy.Params
		if st, ok := params["source_type"].(string); ok && st != "" {
			sourceType = st
		}
		if sk, ok := params["source_key"].(string); ok && sk != "" {
			sourceKey = sk
		}
		if val, ok := params["allow_degrade"]; ok {
			switch v := val.(type) {
			case bool:
				allowDegrade = v
			case string:
				if b, err := strconv.ParseBool(v); err == nil {
					allowDegrade = b
				}
			case float64:
				allowDegrade = v != 0
			}
		}
	}

	// 2. 从 HTTP 请求中提取亲和标识值
	var targetVal string
	if gctx != nil && gctx.Request != nil {
		switch sourceType {
		case "header":
			targetVal = gctx.Request.Header.Get(sourceKey)
		case "query":
			if gctx.Request.URL != nil {
				targetVal = gctx.Request.URL.Query().Get(sourceKey)
			}
		case "cookie":
			if cookie, err := gctx.Request.Cookie(sourceKey); err == nil {
				targetVal = cookie.Value
			}
		}
	}

	// 3. 在候选 endpoints 中匹配 Code 或 ID 属性
	if targetVal != "" {
		for _, ep := range endpoints {
			if ep.Code == targetVal || ep.ID == targetVal {
				return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
			}
		}

		// 若指定了端点但未匹配到，根据 allowDegrade 开关决定是否降级
		if !allowDegrade {
			return nil
		}
	}

	// 4. 若未匹配到，则执行 RoundRobin 轮询兜底
	return lb.fallback.Select(gctx, endpoints)
}
