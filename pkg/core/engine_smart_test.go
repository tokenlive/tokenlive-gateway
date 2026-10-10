package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type smartHook struct{ prepared, invoked bool }

func (s *smartHook) Prepare(g *GatewayContext) error {
	s.prepared = true
	return nil
}
func (s *smartHook) Invoke(g *GatewayContext, p *Pipeline) (bool, error) {
	s.invoked = true
	g.Response = map[string]string{"model": "selected", "answer": "ok"}
	return true, nil
}

func TestEngineUsesSmartRoutingOnlyAfterInbound(t *testing.T) {
	for _, reject := range []bool{false, true} {
		filter := &testInboundFilter{name: "validate"}
		if reject {
			filter.onReqErr = &testHTTPError{code: 403, message: "denied"}
		}
		engine := newTestEngine(map[string]*Pipeline{"chat_completion": {InboundFilters: []InboundFilter{filter}}})
		hook := &smartHook{}
		engine.SetSmartRouter(hook)
		w := httptest.NewRecorder()
		engine.HandleRequest(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"smart","messages":[{"role":"user","content":"hi"}]}`)))
		if !hook.prepared || !filter.called || hook.invoked == reject {
			t.Fatalf("wrong stages: %+v filter=%v", hook, filter.called)
		}
		if !reject && !strings.Contains(w.Body.String(), `"answer":"ok"`) {
			t.Fatalf("response=%s", w.Body.String())
		}
	}
}

func TestEngineBuildsAndInvokesRequestEntryOnce(t *testing.T) {
	for _, handled := range []bool{false, true} {
		t.Run(fmtBool(handled), func(t *testing.T) {
			engine := newTestEngine(nil)
			builder := &testInvokerBuilder{}
			engine.SetInvokerBuilder(builder)
			engine.config = &EngineConfig{Pipelines: map[string]*PipelineConfig{"chat_completion": {
				Name: "chat", Invoker: InvokerConfig{Type: "cluster"},
			}}}
			engine.policyProvider = &mockPolicyProvider{policy: &policy.Policy{InvocationPolicy: &policy.InvocationPolicy{
				FallbackPolicy: &policy.FallbackPolicy{Targets: []string{"next", "last"}},
			}}}
			if handled {
				engine.SetSmartRouter(&smartHook{})
			}
			if err := engine.Init(); err != nil {
				t.Fatal(err)
			}
			pipe := engine.pipelines["chat_completion"]
			entry := pipe.RequestInvoker
			calls := 0
			pipe.RequestInvoker = &testInspectInvoker{invokeFunc: func(g *GatewayContext) error {
				calls++
				return entry.Invoke(g)
			}}
			pipe.Invoker = &testInspectInvoker{invokeFunc: func(g *GatewayContext) error {
				g.Response = map[string]string{"answer": "request-entry"}
				return nil
			}}
			w := httptest.NewRecorder()
			engine.HandleRequest(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"ordinary","messages":[]}`)))
			want := 1
			if handled {
				want = 0
			}
			if calls != want {
				t.Fatalf("request entry calls=%d, want %d; HTTP %d %s", calls, want, w.Code, w.Body.String())
			}
		})
	}
}

func fmtBool(value bool) string {
	if value {
		return "smart_handled"
	}
	return "ordinary"
}
