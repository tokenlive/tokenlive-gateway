package invoker

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/compositerouting"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"go.uber.org/zap"
)

type compositeTestProvider struct {
	smartTestProvider
	interrupt bool
}

func (p *compositeTestProvider) Invoke(g *core.GatewayContext) error {
	if !g.IsStream {
		return p.smartTestProvider.Invoke(g)
	}
	p.calls = append(p.calls, g.Model)
	g.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
	g.ResponseWriter.WriteHeader(http.StatusOK)
	_, _ = g.ResponseWriter.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))
	if p.interrupt {
		return core.ErrNoAvailableEndpoint
	}
	_, _ = g.ResponseWriter.Write([]byte("data: [DONE]\n\n"))
	return nil
}

func TestCompositeSelectionAndStreamCommitBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		stream, interrupt, missing bool
	}{
		{name: "primary"}, {name: "missing primary", missing: true},
		{name: "stream", stream: true}, {name: "stream interrupted", stream: true, interrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &core.SmartRoutingConfig{SchemaVersion: 2, Version: 1, Mode: compositerouting.Priority, PrimaryModel: "cheap"}
			provider := &compositeTestProvider{interrupt: tc.interrupt}
			discovery := smartTestDiscovery{models: map[string][]*core.Endpoint{}}
			for _, name := range []string{"cheap", "strong"} {
				if tc.missing && name == "cheap" {
					continue
				}
				discovery.models[name] = []*core.Endpoint{{ID: name, Model: name, Provider: "test", ProviderImpl: provider, Healthy: true, ContextLength: 100000, MaxOutputTokens: 8192, Abilities: []string{"stream"}, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}}
			}
			ss := store.NewMemoryStateStore()
			defer ss.Close()
			cluster := NewClusterInvoker(discovery, nil, map[string]core.LoadBalancer{"round_robin": &testRoundRobin{}}, &policy.RetryPolicy{}, core.NewCircuitBreakerManager(), ss, zap.NewNop(), nil)
			router := &SmartRouter{source: smartTestSource{cfg}, validator: smartTestValidator{}, policies: smartTestPolicies{}, limits: inbound.NewRateLimitFilter(ss), settler: outbound.NewTokenSettlementFilter(ss, nil, zap.NewNop()), discovery: discovery}
			body := `{"model":"smart","messages":[{"role":"user","content":"hello"}]}`
			if tc.stream {
				body = strings.TrimSuffix(body, "}") + `,"stream":true}`
			}
			writer := httptest.NewRecorder()
			g := core.AcquireContext(writer, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
			defer core.ReleaseContext(g)
			g.Model, g.OriginalModel, g.RequestType, g.RawBody = "smart", "smart", core.RequestTypeChatCompletion, []byte(body)
			g.IsStream = tc.stream
			g.Policy = &policy.Policy{InvocationPolicy: &policy.InvocationPolicy{FallbackPolicy: &policy.FallbackPolicy{Targets: []string{"strong"}}}}
			if err := router.Prepare(g); err != nil {
				t.Fatal(err)
			}
			handled, err := router.Invoke(g, &core.Pipeline{Invoker: cluster})
			if !handled || (err != nil) != tc.interrupt {
				t.Fatalf("handled=%v err=%v", handled, err)
			}
			if len(provider.calls) != 1 {
				t.Fatalf("unexpected calls: %v", provider.calls)
			}
			want := "cheap"
			if tc.missing {
				want = "strong"
			}
			if provider.calls[0] != want || g.SmartRouting.JudgeUsage != nil || g.SmartRouting.Score != nil {
				t.Fatalf("unexpected routing: %+v calls=%v", g.SmartRouting, provider.calls)
			}
			if tc.stream && (!strings.Contains(writer.Body.String(), "hello") || g.TTFT == 0) {
				t.Fatalf("stream not forwarded: %s", writer.Body.String())
			}
			if g.Model != "smart" || g.OriginalModel != "smart" {
				t.Fatal("logical identity changed")
			}
		})
	}
}

func TestCompositeStreamWriterFlushCommits(t *testing.T) {
	w := &compositeStreamWriter{ResponseWriter: httptest.NewRecorder()}
	w.Flush()
	if !w.committed.Load() {
		t.Fatal("flush commits headers even without a token")
	}
}

func TestCompositeAttemptBudgetIsShared(t *testing.T) {
	ctx := core.WithCompositeAttemptBudget(t.Context(), 1)
	if err := core.ConsumeCompositeAttempt(ctx); err != nil {
		t.Fatal(err)
	}
	if err := core.ConsumeCompositeAttempt(ctx); !errors.Is(err, core.ErrCompositeAttemptBudget) {
		t.Fatal(err)
	}
}
