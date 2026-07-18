package lbs

import (
	"strconv"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// EndpointAffinityLoadBalancer prefers an endpoint by affinity key.
type EndpointAffinityLoadBalancer struct {
	stateStore core.StateStore
	fallback   core.LoadBalancer
}

// NewEndpointAffinityLoadBalancer creates an affinity LB.
func NewEndpointAffinityLoadBalancer(ss core.StateStore) *EndpointAffinityLoadBalancer {
	return &EndpointAffinityLoadBalancer{
		stateStore: ss,
		fallback:   NewRoundRobin(),
	}
}

// Select prefers the affinity-matched endpoint.
func (lb *EndpointAffinityLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	sourceType := "header"
	sourceKey := "X-Endpoint-Code"
	allowDegrade := false

	// 1. Read sourceType, sourceKey, allowDegrade from policy.
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

	// 2. Extract affinity value from the HTTP request.
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

	// 3. Match candidates by Code or ID.
	if targetVal != "" {
		for _, ep := range endpoints {
			if ep.Code == targetVal || ep.ID == targetVal {
				return invoker.NewProviderInvoker(ep.ProviderImpl, ep)
			}
		}

		// Miss with affinity set: degrade only if allowDegrade.
		if !allowDegrade {
			if gctx != nil {
				gctx.FatalErr = core.ErrFatalNoAvailableEndpoint
			}
			return nil
		}
	}

	// 4. Fallback to round-robin.
	return lb.fallback.Select(gctx, endpoints)
}
