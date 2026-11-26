package core_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/providers"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"go.uber.org/zap"
)

func TestChatDialectRejectionStopsRetriesHedgingAndFallback(t *testing.T) {
	for _, mode := range []string{"failover", "hedging"} {
		t.Run(mode, func(t *testing.T) {
			var upstreamCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls.Add(1)
				_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"unexpected"}}]}`)
			}))
			defer upstream.Close()
			provider := providers.NewOpenAIProvider("test", upstream.URL, "test-key", nil)
			discovery := &chaosDiscovery{endpoints: []*core.Endpoint{
				{ID: "first", Model: "requested", Provider: "test", Healthy: true, ProviderImpl: provider, DialectImplementations: []string{"chat_store_unsupported"}},
				{ID: "second", Model: "requested", Provider: "test", Healthy: true, ProviderImpl: provider},
				{ID: "fallback", Model: "fallback", Provider: "test", Healthy: true, ProviderImpl: provider},
			}}
			retry := &policy.RetryPolicy{Retry: 2, ErrorMessages: []string{"store=true"}}
			policies := &policy.Policy{
				InvocationPolicy: &policy.InvocationPolicy{
					Type: mode, RetryPolicy: retry, FallbackPolicy: &policy.FallbackPolicy{Targets: []string{"fallback"}},
				},
				CircuitBreakPolicies: []*policy.CircuitBreakPolicy{{
					Level: "INSTANCE", SlidingWindowType: "count", SlidingWindowSize: 1, MinCallsThreshold: 1,
					FailureRateThreshold: 1, ErrorMessages: []string{"store=true"},
					DegradeConfig: &policy.DegradeConfig{ResponseCode: http.StatusServiceUnavailable, ResponseBody: `{"message":"degraded"}`},
				}},
			}
			engine, breaker := newDialectEngine(t, discovery, policies, mode)
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"requested","messages":[],"store":true}`))
			response := httptest.NewRecorder()
			engine.HandleRequest(response, req)
			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), "store=true")
			require.Zero(t, upstreamCalls.Load())
			require.Equal(t, core.CircuitClosed, breaker.GetState("first"))
		})
	}
}

func TestChatDialectFollowsFallbackEndpoint(t *testing.T) {
	upstreamBodies := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		upstreamBodies <- string(body)
		if strings.HasPrefix(r.URL.Path, "/primary/") {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer upstream.Close()

	primary := providers.NewOpenAIProvider("primary", upstream.URL+"/primary", "test-key", nil)
	fallback := providers.NewOpenAIProvider("fallback", upstream.URL, "test-key", nil)
	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{
		{ID: "primary", Model: "requested", ProviderImpl: primary, DialectImplementations: []string{"chat_developer_to_system"}},
		{ID: "target", Model: "fallback", ProviderImpl: fallback, DialectImplementations: []string{"chat_store_unsupported"}},
	}}
	policies := &policy.Policy{
		InvocationPolicy: &policy.InvocationPolicy{
			RetryPolicy:    &policy.RetryPolicy{Retry: 1, ErrorCodes: []string{"502"}},
			FallbackPolicy: &policy.FallbackPolicy{Targets: []string{"fallback"}},
		},
	}
	engine, _ := newDialectEngine(t, discovery, policies, "failover")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"requested","messages":[{"role":"developer","content":"hi"}],"store":false}`))
	response := httptest.NewRecorder()
	engine.HandleRequest(response, req)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Len(t, upstreamBodies, 2)
	first, second := <-upstreamBodies, <-upstreamBodies
	require.Contains(t, first, `"role":"system"`)
	require.Contains(t, first, `"store":false`)
	require.Contains(t, second, `"role":"developer"`)
	require.NotContains(t, second, `"store"`)
}

func newDialectEngine(t *testing.T, discovery core.Discovery, policies *policy.Policy, mode string) (*core.Engine, *core.CircuitBreakerManager) {
	t.Helper()
	logger := zap.NewNop()
	store := newChaosStateStore()
	breaker := core.NewCircuitBreakerManager()
	lbs := map[string]core.LoadBalancer{"round_robin": &mockLoadBalancer{}}
	cluster := invoker.NewClusterInvoker(discovery, nil, lbs, nil, breaker, store, logger, nil)
	var pipelineInvoker core.Invoker = cluster
	if mode == "hedging" {
		pipelineInvoker = invoker.NewHedgingInvoker(discovery, nil, lbs, breaker, store, logger, cluster)
	}
	engine := core.NewEngine(&core.EngineConfig{
		Pipelines: map[string]*core.PipelineConfig{
			"chat_completion": {RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}, Invoker: core.InvokerConfig{Type: mode}},
		},
	}, discovery, store, &mockPolicyProvider{policy: policies}, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{invoker: pipelineInvoker})
	require.NoError(t, engine.Init())
	return engine, breaker
}

func TestChatDialectAllowsHedgingAfterUpstreamStart(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/slow/") {
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
			return
		}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"backup won"}}]}`)
	}))
	defer upstream.Close()
	dialects := []string{"chat_store_unsupported"}
	primary := providers.NewOpenAIProvider("primary", upstream.URL+"/slow", "test-key", nil)
	secondary := providers.NewOpenAIProvider("secondary", upstream.URL, "test-key", nil)
	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{
		{ID: "first", Model: "m", ProviderImpl: primary, DialectImplementations: dialects},
		{ID: "second", Model: "m", ProviderImpl: secondary, DialectImplementations: dialects},
	}}
	policies := &policy.Policy{
		InvocationPolicy: &policy.InvocationPolicy{Type: "hedging", RetryPolicy: &policy.RetryPolicy{BaseMs: 1}},
	}
	engine, _ := newDialectEngine(t, discovery, policies, "hedging")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[],"store":false}`))
	response := httptest.NewRecorder()
	engine.HandleRequest(response, req)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "backup won")
}
