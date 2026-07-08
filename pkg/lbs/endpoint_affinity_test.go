package lbs

import (
	"context"
	"net/http"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointAffinity_SelectByHeader(t *testing.T) {
	ss := store.NewMemoryStateStore()
	lb := NewEndpointAffinityLoadBalancer(ss)

	ep1 := &core.Endpoint{ID: "ep1", Code: "code1"}
	ep2 := &core.Endpoint{ID: "ep2", Code: "code2"}
	endpoints := []*core.Endpoint{ep1, ep2}

	gctx := &core.GatewayContext{
		Ctx: context.Background(),
		Policy: &policy.Policy{
			LoadBalancePolicy: &policy.LoadBalancePolicy{
				Type: "endpoint_affinity",
				Params: map[string]interface{}{
					"source_type": "header",
					"source_key":  "X-Endpoint-Code",
				},
			},
		},
	}

	req, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("X-Endpoint-Code", "code2")
	gctx.Request = req

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestEndpointAffinity_SelectByQuery(t *testing.T) {
	ss := store.NewMemoryStateStore()
	lb := NewEndpointAffinityLoadBalancer(ss)

	ep1 := &core.Endpoint{ID: "ep1", Code: "code1"}
	ep2 := &core.Endpoint{ID: "ep2", Code: "code2"}
	endpoints := []*core.Endpoint{ep1, ep2}

	gctx := &core.GatewayContext{
		Ctx: context.Background(),
		Policy: &policy.Policy{
			LoadBalancePolicy: &policy.LoadBalancePolicy{
				Type: "endpoint_affinity",
				Params: map[string]interface{}{
					"source_type": "query",
					"source_key":  "endpoint_code",
				},
			},
		},
	}

	req, _ := http.NewRequest("POST", "/v1/chat/completions?endpoint_code=code1", nil)
	gctx.Request = req

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)
}

func TestEndpointAffinity_SelectByCookie(t *testing.T) {
	ss := store.NewMemoryStateStore()
	lb := NewEndpointAffinityLoadBalancer(ss)

	ep1 := &core.Endpoint{ID: "ep1", Code: "code1"}
	ep2 := &core.Endpoint{ID: "ep2", Code: "code2"}
	endpoints := []*core.Endpoint{ep1, ep2}

	gctx := &core.GatewayContext{
		Ctx: context.Background(),
		Policy: &policy.Policy{
			LoadBalancePolicy: &policy.LoadBalancePolicy{
				Type: "endpoint_affinity",
				Params: map[string]interface{}{
					"source_type": "cookie",
					"source_key":  "aff_cookie",
				},
			},
		},
	}

	req, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
	req.AddCookie(&http.Cookie{Name: "aff_cookie", Value: "code2"})
	gctx.Request = req

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep2", invoker.Endpoint().ID)
}

func TestEndpointAffinity_FallbackOnMiss(t *testing.T) {
	ss := store.NewMemoryStateStore()
	lb := NewEndpointAffinityLoadBalancer(ss)

	ep1 := &core.Endpoint{ID: "ep1", Code: "code1"}
	ep2 := &core.Endpoint{ID: "ep2", Code: "code2"}
	endpoints := []*core.Endpoint{ep1, ep2}

	gctx := &core.GatewayContext{
		Ctx: context.Background(),
		Policy: &policy.Policy{
			LoadBalancePolicy: &policy.LoadBalancePolicy{
				Type: "endpoint_affinity",
				Params: map[string]interface{}{
					"source_type": "header",
					"source_key":  "X-Endpoint-Code",
				},
			},
		},
	}

	req, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("X-Endpoint-Code", "non_existent")
	gctx.Request = req

	invoker := lb.Select(gctx, endpoints)
	require.NotNil(t, invoker)
	assert.Equal(t, "ep1", invoker.Endpoint().ID)
}

func TestEndpointAffinity_NoDegradeOnMiss(t *testing.T) {
	ss := store.NewMemoryStateStore()
	lb := NewEndpointAffinityLoadBalancer(ss)

	ep1 := &core.Endpoint{ID: "ep1", Code: "code1"}
	ep2 := &core.Endpoint{ID: "ep2", Code: "code2"}
	endpoints := []*core.Endpoint{ep1, ep2}

	gctx := &core.GatewayContext{
		Ctx: context.Background(),
		Policy: &policy.Policy{
			LoadBalancePolicy: &policy.LoadBalancePolicy{
				Type: "endpoint_affinity",
				Params: map[string]interface{}{
					"source_type":   "header",
					"source_key":    "X-Endpoint-Code",
					"allow_degrade": false,
				},
			},
		},
	}

	req, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("X-Endpoint-Code", "non_existent")
	gctx.Request = req

	invoker := lb.Select(gctx, endpoints)
	assert.Nil(t, invoker)
}
