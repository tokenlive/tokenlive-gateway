package invoker_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func TestSmartHTTPChildPolicyPermissions(t *testing.T) {
	for _, model := range []string{"cheap", "judge"} {
		t.Run(model, func(t *testing.T) {
			h := newSmartHTTPRig(t)
			p, _ := h.config.GetPolicy(context.Background(), "", "", model)
			p.Permissions = []string{"smart", "strong"}
			h.config.policies[model] = p
			direct := h.request(context.Background(), "/v1/chat/completions", strings.Replace(smartHTTPBody, `"model":"smart"`, `"model":"`+model+`"`, 1))
			if direct.Code != 403 {
				t.Fatalf("direct denial=%d", direct.Code)
			}
			w := h.request(context.Background(), "/v1/chat/completions", smartHTTPBody)
			for _, call := range h.callSnapshot() {
				if call.Model == model+"-wire" {
					t.Fatalf("forbidden %s called through composite, HTTP%d", model, w.Code)
				}
			}
		})
	}
}

func TestSmartHTTPConditionalTargetHardLimits(t *testing.T) {
	for _, tc := range []struct{ kind, key, value string }{
		{"header", "X-Workspace-ID", "workspace-fixture"},
		{"query", "plan", "blocked"},
		{"cookie", "session", "must-not-leak"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			h := newSmartHTTPRig(t)
			p, _ := h.config.GetPolicy(context.Background(), "", "", "cheap")
			p.LimitPolicies = []*policy.LimitPolicy{{
				ID: "conditional-deny", Type: "request",
				Conditions:     []*matcher.TagCondition{{Type: tc.kind, Key: tc.key, OpType: "EQUAL", Values: []string{tc.value}}},
				SlidingWindows: []*policy.SlidingWindow{{Threshold: 0, TimeWindowInMs: 60000}},
			}}
			h.config.policies["cheap"] = p
			route := "/v1/chat/completions?plan=blocked"
			direct := h.request(context.Background(), route, strings.Replace(smartHTTPBody, `"model":"smart"`, `"model":"cheap"`, 1))
			if direct.Code != 429 {
				t.Fatalf("direct denial=%d", direct.Code)
			}
			w := h.request(context.Background(), route, smartHTTPBody)
			if w.Code != 429 {
				t.Fatalf("conditional hard quota bypass: HTTP%d %s", w.Code, w.Body.String())
			}
			for _, call := range h.callSnapshot() {
				if call.Model == "cheap-wire" || call.Model == "strong-wire" {
					t.Fatalf("quota bypassed: %s", call.Endpoint)
				}
			}
		})
	}
}

func TestSmartHTTPHedgingTerminalErrorDoesNotUpgrade(t *testing.T) {
	h := newSmartHTTPRig(t)
	p, _ := h.config.GetPolicy(context.Background(), "", "", "cheap")
	p.InvocationPolicy.Type = "hedging"
	p.InvocationPolicy.RetryPolicy.BaseMs = 100
	h.config.policies["cheap"] = p
	h.respond = func(w http.ResponseWriter, r *http.Request, call smartHTTPCall) {
		switch call.Endpoint {
		case "cheap-1":
			http.Error(w, "invalid request", 400)
		case "cheap-2":
			time.Sleep(10 * time.Millisecond)
			http.Error(w, "unavailable", 503)
		default:
			h.writeDefault(w, call)
		}
	}
	w := h.request(context.Background(), "/v1/chat/completions", smartHTTPBody)
	if w.Code != 400 {
		t.Fatalf("terminal error lost: HTTP%d %s", w.Code, w.Body.String())
	}
	for _, call := range h.callSnapshot() {
		if call.Model == "strong-wire" {
			t.Fatal("terminal 400 caused upgrade")
		}
	}
}
