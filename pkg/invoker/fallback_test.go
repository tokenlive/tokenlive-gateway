package invoker_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type fallbackCall func(*core.GatewayContext) error

func (f fallbackCall) Invoke(g *core.GatewayContext) error { return f(g) }
func (fallbackCall) Endpoint() *core.Endpoint              { return nil }

func TestFallbackRequestEntrySelectsPolicyInvokerOnce(t *testing.T) {
	body := []byte(`{"model":"first","messages":[]}`)
	g := core.AcquireContext(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	defer core.ReleaseContext(g)
	g.Model, g.OriginalModel, g.RawBody = "first", "first", body
	g.Policy = &policy.Policy{InvocationPolicy: &policy.InvocationPolicy{Type: "hedging", FallbackPolicy: &policy.FallbackPolicy{Targets: []string{"next", "last"}}}}
	var models []string
	selected := fallbackCall(func(g *core.GatewayContext) error {
		models = append(models, g.Model)
		if string(g.RawBody) != string(body) {
			t.Fatalf("body changed before model %s: %s", g.Model, g.RawBody)
		}
		g.History = append(g.History, core.AttemptRecord{Model: g.Model})
		g.AttemptCount++
		if g.Model != "last" {
			g.Policy.InvocationPolicy.Type = "cluster"
			return fmt.Errorf("empty: %w", core.ErrNoAvailableEndpoint)
		}
		g.UpstreamBody = []byte(`{"answer":"ok"}`)
		return nil
	})
	pipe := &core.Pipeline{Invoker: fallbackCall(func(*core.GatewayContext) error {
		t.Fatal("default invoker selected instead of initial policy")
		return nil
	}), Invokers: map[string]core.Invoker{"hedging": selected}}
	if err := core.NewFallbackInvoker(pipe).Invoke(g); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(models, []string{"first", "next", "last"}) || !reflect.DeepEqual(g.FallbackChain, models) || g.Model != "last" || g.OriginalModel != "first" || g.AttemptCount != 3 || len(g.History) != 3 || string(g.RawBody) != string(body) {
		t.Fatalf("request fallback contract lost: models=%v chain=%v model=%s original=%s attempts=%d history=%v body=%s", models, g.FallbackChain, g.Model, g.OriginalModel, g.AttemptCount, g.History, g.RawBody)
	}
}

func TestFallbackRequestEntryStopsAtNonDegradableErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		fatal    error
		ttft     time.Duration
		affinity map[string]any
		targets  []string
		want     int
	}{
		{name: "success", targets: []string{"next"}, want: 1},
		{name: "ordinary_502", err: errors.New("bad gateway"), targets: []string{"next"}, want: 1},
		{name: "fatal_field", err: core.ErrNoAvailableEndpoint, fatal: errors.New("fatal"), targets: []string{"next"}, want: 1},
		{name: "fatal_sentinel", err: fmt.Errorf("fatal: %w", core.ErrFatalNoAvailableEndpoint), targets: []string{"next"}, want: 1},
		{name: "affinity", err: core.ErrNoAvailableEndpoint, affinity: map[string]any{"allow_degrade": false}, targets: []string{"next"}, want: 1},
		{name: "ttft", err: core.ErrNoAvailableEndpoint, ttft: time.Millisecond, targets: []string{"next"}, want: 1},
		{name: "no_targets", err: core.ErrNoAvailableEndpoint, want: 1},
		{name: "no_eligible", err: core.ErrNoAvailableEndpoint, targets: []string{"next", "last"}, want: 3},
		{name: "affinity_allows", err: core.ErrNoAvailableEndpoint, affinity: map[string]any{"allowDegrade": "true"}, targets: []string{"next"}, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := core.AcquireContext(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
			defer core.ReleaseContext(g)
			g.Model = "first"
			g.Policy = &policy.Policy{InvocationPolicy: &policy.InvocationPolicy{FallbackPolicy: &policy.FallbackPolicy{Targets: tc.targets}}}
			if tc.affinity != nil {
				g.Policy.LoadBalancePolicy = &policy.LoadBalancePolicy{Type: "endpoint_affinity", Params: tc.affinity}
			}
			calls := 0
			pipe := &core.Pipeline{Invoker: fallbackCall(func(g *core.GatewayContext) error {
				calls++
				g.FatalErr, g.TTFT = tc.fatal, tc.ttft
				return tc.err
			})}
			err := core.NewFallbackInvoker(pipe).Invoke(g)
			if !errors.Is(err, tc.err) || calls != tc.want {
				t.Fatalf("err=%v calls=%d, want err=%v calls=%d", err, calls, tc.err, tc.want)
			}
			if len(tc.targets) == 0 && len(g.FallbackChain) != 0 {
				t.Fatalf("unexpected chain %v", g.FallbackChain)
			}
		})
	}
}

func TestFallbackConfigBuildsRequestEntryWithoutWrappingRawRegistry(t *testing.T) {
	h := newSmartHTTPRig(t)
	for _, kind := range []string{"cluster", "fallback"} {
		raw, err := invoker.NewBuilder().BuildInvoker(&core.InvokerConfig{Type: kind}, h.engine)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := raw.(*invoker.ClusterInvoker); !ok {
			t.Fatalf("%s raw invoker is %T", kind, raw)
		}
	}
	if err := h.engine.UpdateConfig(&core.EngineConfig{Pipelines: map[string]*core.PipelineConfig{"chat_completion": {
		Name: "fallback", InboundFilters: []string{"rate_limit"}, OutboundFilters: []string{h.receipt.Name()}, Invoker: core.InvokerConfig{Type: "fallback"},
	}}}); err != nil {
		t.Fatal(err)
	}
	h.discovery.RegisterService("cheap", nil)
	p, _ := h.config.GetPolicy(context.Background(), "", "", "cheap")
	p.InvocationPolicy.Type = "fallback"
	p.InvocationPolicy.FallbackPolicy = &policy.FallbackPolicy{Targets: []string{"strong"}}
	h.config.policies["cheap"] = p
	w := h.request(context.Background(), "/v1/chat/completions", strings.Replace(smartHTTPBody, `"model":"smart"`, `"model":"cheap"`, 1))
	requireSmartHTTPAnswer(t, w, "strong-wire")
	h.requireCalls("strong-1")
	if h.receipt.invocations != 1 || h.receipt.model != "strong" || h.receipt.original != "cheap" {
		t.Fatalf("outer/final contract lost: %+v", h.receipt)
	}
}
