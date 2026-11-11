package invoker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"go.uber.org/zap"
)

func TestCompositeUnknownCapacityMustNotBeEligible(t *testing.T) {
	ep := &core.Endpoint{RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
	req := &smartRequest{Messages: []byte(`[{"role":"user","content":"hi"}]`), MaxOutput: 999999, StrictCapabilities: true}
	if smartEndpointCompatible(ep, req) {
		t.Fatal("unknown context/output capacity accepted even for max_tokens=999999")
	}
}
func TestCompositeStructuredOutputMustBeRejected(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_schema","json_schema":{"name":"x","schema":{"type":"object"}}}}`)
	if _, err := parseSmartTextRequest(core.RequestTypeChatCompletion, body, true); err == nil {
		t.Fatal("new mode accepts unverified structured output")
	}
}

type reviewAffinityPolicies struct{ allowDegrade bool }

func (pconf reviewAffinityPolicies) GetPolicy(_ context.Context, _, _, model string) (*policy.Policy, error) {
	p := &policy.Policy{InvocationPolicy: &policy.InvocationPolicy{RetryPolicy: &policy.RetryPolicy{Retry: 0}}}
	if model == "cheap" {
		p.LoadBalancePolicy = &policy.LoadBalancePolicy{Type: "endpoint_affinity", Params: map[string]interface{}{"allow_degrade": pconf.allowDegrade}}
	}
	return p, nil
}

type compositeUnavailableDiscovery struct {
	smartTestDiscovery
	returnError bool
}

func (d compositeUnavailableDiscovery) List(ctx context.Context, model string) ([]*core.Endpoint, error) {
	if d.returnError && model == "cheap" {
		return nil, core.ErrNoAvailableEndpoint
	}
	return d.smartTestDiscovery.List(ctx, model)
}
func TestCompositeMissingTargetMustRespectStrongAffinity(t *testing.T) {
	for _, allow := range []bool{false, true} {
		for _, discoveryError := range []bool{false, true} {
			for _, parentAffinity := range []bool{false, true} {
				cfg := &core.SmartRoutingConfig{SchemaVersion: 2, Version: 1, Mode: "priority", PrimaryModel: "cheap"}
				provider := &compositeTestProvider{}
				d := compositeUnavailableDiscovery{smartTestDiscovery: smartTestDiscovery{models: map[string][]*core.Endpoint{"strong": {{ID: "strong", Model: "strong", Provider: "test", ProviderImpl: provider, Healthy: true, ContextLength: 100000, MaxOutputTokens: 8192, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}}}}, returnError: discoveryError}
				ss := store.NewMemoryStateStore()
				cluster := NewClusterInvoker(d, nil, map[string]core.LoadBalancer{"round_robin": &testRoundRobin{}}, &policy.RetryPolicy{}, core.NewCircuitBreakerManager(), ss, zap.NewNop(), nil)
				r := &SmartRouter{source: smartTestSource{cfg}, validator: smartTestValidator{}, policies: reviewAffinityPolicies{allowDegrade: allow}, limits: inbound.NewRateLimitFilter(ss), settler: outbound.NewTokenSettlementFilter(ss, nil, zap.NewNop()), discovery: d}
				body := `{"model":"smart","messages":[{"role":"user","content":"hello"}]}`
				g := core.AcquireContext(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
				g.Model, g.OriginalModel, g.RequestType, g.RawBody = "smart", "smart", core.RequestTypeChatCompletion, []byte(body)
				g.Policy = &policy.Policy{InvocationPolicy: &policy.InvocationPolicy{FallbackPolicy: &policy.FallbackPolicy{Targets: []string{"strong"}}}}
				if parentAffinity {
					g.Policy.LoadBalancePolicy = &policy.LoadBalancePolicy{Type: "endpoint_affinity", Params: map[string]interface{}{"allow_degrade": false}}
				}
				if err := r.Prepare(g); err != nil {
					t.Fatal(err)
				}
				_, err := r.Invoke(g, &core.Pipeline{Invoker: cluster})
				permitted := allow && !parentAffinity
				if permitted {
					if err != nil || len(provider.calls) != 1 {
						t.Fatalf("explicitly permitted fallback was blocked: err=%v calls=%v", err, provider.calls)
					}
				} else if err == nil || len(provider.calls) > 0 {
					t.Fatalf("strong affinity bypassed: err=%v calls=%v", err, provider.calls)
				}
				if !allow && !errors.Is(err, core.ErrFatalNoAvailableEndpoint) {
					t.Fatalf("missing terminal affinity classification: %v", err)
				}
				core.ReleaseContext(g)
				_ = ss.Close()
			}
		}
	}
}
