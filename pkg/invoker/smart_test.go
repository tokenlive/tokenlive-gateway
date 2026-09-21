package invoker

import (
	"context"
	"encoding/json"
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

type smartTestSource struct{ cfg *core.SmartRoutingConfig }

func (s smartTestSource) GetSmartRouting(_ context.Context, model string) (*core.SmartRoutingConfig, error) {
	if model == "smart" {
		return s.cfg.Clone(), nil
	}
	return nil, nil
}

type smartTestValidator struct{ denied string }

func (v smartTestValidator) ValidateModel(_ context.Context, model, _, _ string) (bool, error) {
	return model != v.denied, nil
}

type smartTestPolicies struct{}

func (smartTestPolicies) GetPolicy(context.Context, string, string, string) (*policy.Policy, error) {
	return &policy.Policy{InvocationPolicy: &policy.InvocationPolicy{RetryPolicy: &policy.RetryPolicy{Retry: 0}}}, nil
}

type smartTestDiscovery struct{ models map[string][]*core.Endpoint }

func (d smartTestDiscovery) List(_ context.Context, model string) ([]*core.Endpoint, error) {
	return d.models[model], nil
}
func (d smartTestDiscovery) Watch(context.Context, string) (<-chan []*core.Endpoint, error) {
	return nil, nil
}
func (d smartTestDiscovery) Close() error { return nil }

type smartTestProvider struct {
	calls        []string
	judgment     string
	failModel    string
	judgeHeaders http.Header
}

func (*smartTestProvider) Name() string                      { return "smart-test" }
func (*smartTestProvider) Type() core.ProviderType           { return "openai" }
func (*smartTestProvider) HealthCheck(context.Context) error { return nil }
func (*smartTestProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{core.RequestTypeChatCompletion}
}
func (*smartTestProvider) ValidateConfig() error { return nil }
func (p *smartTestProvider) Invoke(gctx *core.GatewayContext) error {
	p.calls = append(p.calls, gctx.Model)
	if gctx.Model == p.failModel {
		gctx.UpstreamResponse = &http.Response{StatusCode: 500, Header: http.Header{}}
		return errors.New("upstream error: status 500")
	}
	content := "answer from " + gctx.Model
	if gctx.Model == "judge" {
		content = p.judgment
		p.judgeHeaders = gctx.Request.Header.Clone()
	}
	gctx.InputTokens, gctx.OutputTokens = 9, 3
	gctx.Response = map[string]any{
		"model":   gctx.Model,
		"choices": []any{map[string]any{"message": map[string]string{"role": "assistant", "content": content}}},
		"usage":   map[string]int{"prompt_tokens": 9, "completion_tokens": 3},
	}
	return nil
}

func TestSmartRouterDispatchAndEscalation(t *testing.T) {
	for _, tc := range []struct {
		name, judgment, fail, denied string
		wantCalls                    []string
		wantScore                    *int
		wantError                    bool
	}{
		{"simple", `{"score":10}`, "", "", []string{"judge", "cheap"}, intPointer(10), false},
		{"complex", `{"score":90}`, "", "", []string{"judge", "strong"}, intPointer(90), false},
		{"invalid", `{"score":null}`, "", "", []string{"judge", "strong"}, nil, false},
		{"judge unavailable", "", "judge", "", []string{"judge", "strong"}, nil, false},
		{"upgrade", `{"score":10}`, "cheap", "", []string{"judge", "cheap", "strong"}, intPointer(10), false},
		{"no downgrade", `{"score":90}`, "strong", "", []string{"judge", "strong"}, intPointer(90), true},
		{"denied cheap", `{"score":10}`, "", "cheap", []string{"judge", "strong"}, intPointer(10), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &core.SmartRoutingConfig{Version: 1, JudgeModel: "judge", Ranges: []core.SmartRoutingRange{{Min: 0, Max: 50, Model: "cheap"}, {Min: 50, Max: 100, Model: "strong"}}}
			cfg.ApplyDefaults()
			provider := &smartTestProvider{judgment: tc.judgment, failModel: tc.fail}
			discovery := smartTestDiscovery{models: map[string][]*core.Endpoint{}}
			for _, name := range []string{"judge", "cheap", "strong"} {
				discovery.models[name] = []*core.Endpoint{{ID: name, Model: name, Provider: "test", ProviderImpl: provider, Healthy: true, ContextLength: 100000, MaxOutputTokens: 8192, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}}
			}
			ss := store.NewMemoryStateStore()
			defer ss.Close()
			cluster := NewClusterInvoker(discovery, nil, map[string]core.LoadBalancer{"round_robin": &testRoundRobin{}}, &policy.RetryPolicy{}, core.NewCircuitBreakerManager(), ss, zap.NewNop(), nil)
			router := &SmartRouter{source: smartTestSource{cfg}, validator: smartTestValidator{tc.denied}, policies: smartTestPolicies{}, judge: cluster, limits: inbound.NewRateLimitFilter(ss), settler: outbound.NewTokenSettlementFilter(ss, nil, zap.NewNop()), discovery: discovery}
			body := `{"model":"smart","messages":[{"role":"user","content":"hello"}]}`
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer private-client-key")
			gctx := core.AcquireContext(httptest.NewRecorder(), req)
			defer core.ReleaseContext(gctx)
			gctx.Model, gctx.OriginalModel, gctx.RequestType, gctx.RawBody = "smart", "smart", core.RequestTypeChatCompletion, []byte(body)
			if err := router.Prepare(gctx); err != nil {
				t.Fatal(err)
			}
			handled, err := router.Invoke(gctx, &core.Pipeline{Invoker: cluster})
			if !handled || (err != nil) != tc.wantError {
				t.Fatalf("handled=%v err=%v", handled, err)
			}
			gotCalls, _ := json.Marshal(provider.calls)
			wantCalls, _ := json.Marshal(tc.wantCalls)
			if string(gotCalls) != string(wantCalls) {
				t.Fatalf("calls=%s want=%s", gotCalls, wantCalls)
			}
			score := gctx.SmartRouting.Score
			if (score == nil) != (tc.wantScore == nil) || score != nil && *score != *tc.wantScore {
				t.Fatalf("wrong score: %v", score)
			}
			if provider.judgeHeaders.Get("Authorization") != "" {
				t.Fatal("client credentials leaked to judge")
			}
			if gctx.Model != "smart" || gctx.OriginalModel != "smart" {
				t.Fatal("logical model mutated")
			}
			if !tc.wantError && (gctx.InputTokens != 9 || gctx.OutputTokens != 3) {
				t.Fatalf("judge usage mixed into answer: %d %d", gctx.InputTokens, gctx.OutputTokens)
			}
			if gctx.SmartConfig.Version != 1 {
				t.Fatal("lost snapshot")
			}
		})
	}
}

func intPointer(v int) *int { return &v }

func TestSmartJudgeBufferCannotGrowPastConfiguredLimit(t *testing.T) {
	buffer := newSmartResponseBuffer()
	buffer.limit = 8
	if _, err := buffer.Write([]byte("123456789")); err == nil || buffer.body.Len() > 8 {
		t.Fatal("judge writer accepted oversized response")
	}
}
