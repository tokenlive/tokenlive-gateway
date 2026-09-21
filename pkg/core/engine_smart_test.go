package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
