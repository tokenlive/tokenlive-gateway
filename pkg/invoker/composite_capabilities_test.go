package invoker

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"testing"
)

func TestCompositeChecksFullRequestCapacityAndStream(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		context, output, requested int64
		stream                     bool
		abilities                  []string
		eligible                   bool
	}{
		{name: "known capacity", context: 4096, output: 1024, requested: 512, eligible: true},
		{name: "unknown context", output: 1024, requested: 512},
		{name: "unknown output", context: 4096, requested: 512},
		{name: "output exceeds limit", context: 4096, output: 1024, requested: 2048},
		{name: "prompt plus output exceeds context", context: 150, output: 1024, requested: 100},
		{name: "undeclared streaming", context: 4096, output: 1024, requested: 512, stream: true},
		{name: "declared streaming", context: 4096, output: 1024, requested: 512, stream: true, abilities: []string{"stream"}, eligible: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := &core.Endpoint{ContextLength: tc.context, MaxOutputTokens: tc.output, Abilities: tc.abilities, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
			req := &smartRequest{Messages: []byte(`[{"role":"user","content":"hello"}]`), MaxOutput: tc.requested, Stream: tc.stream, StrictCapabilities: true}
			if got := smartEndpointCompatible(ep, req); got != tc.eligible {
				t.Fatalf("eligible=%v want %v", got, tc.eligible)
			}
		})
	}
	// Difficulty compatibility: the old unknown-capacity behavior is isolated.
	ep := &core.Endpoint{RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
	if !smartEndpointCompatible(ep, &smartRequest{Messages: []byte(`[]`)}) {
		t.Fatal("legacy capacity behavior changed")
	}
}
func TestCompositeCapacityRouterRechecksEveryEndpoint(t *testing.T) {
	g := &core.GatewayContext{RequestType: core.RequestTypeChatCompletion, RawBody: []byte(`{"messages":[{"role":"user","content":"hi"}],"stream":true,"max_tokens":512}`)}
	unknown := &core.Endpoint{ID: "unknown", RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
	good := &core.Endpoint{ID: "good", ContextLength: 4096, MaxOutputTokens: 1024, Abilities: []string{"stream"}, RequestTypes: unknown.RequestTypes}
	got := (smartCapacityRouter{strict: true}).Route(g, []*core.Endpoint{unknown, good})
	if len(got) != 1 || got[0].ID != "good" {
		t.Fatalf("incompatible retry endpoint was not filtered: %+v", got)
	}
}
func TestCompositeStructuredResponseFormatRejectsBeforeInvocation(t *testing.T) {
	for _, value := range []string{`{"type":"json_schema","json_schema":{}}`, `{"type":"json_object"}`} {
		cfg := &core.SmartRoutingConfig{SchemaVersion: 2, Version: 1, Mode: "priority", PrimaryModel: "cheap"}
		router := &SmartRouter{source: smartTestSource{cfg}}
		g := &core.GatewayContext{Model: "smart", RequestType: core.RequestTypeChatCompletion, RawBody: []byte(`{"messages":[{"role":"user","content":"hi"}],"response_format":` + value + `}`)}
		if err := router.Prepare(g); err == nil {
			t.Fatal("structured output entered executor")
		}
	}
}

func TestCompositeCapacityGuardPreservesDifficultyRequestBoundary(t *testing.T) {
	g := &core.GatewayContext{RequestType: core.RequestTypeChatCompletion, RawBody: []byte(`{"messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_object"}}`)}
	ep := &core.Endpoint{ID: "legacy", RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
	if len((smartCapacityRouter{}).Route(g, []*core.Endpoint{ep})) != 1 {
		t.Fatal("difficulty request/capacity compatibility changed")
	}
	if len((smartCapacityRouter{strict: true}).Route(g, []*core.Endpoint{ep})) != 0 {
		t.Fatal("new mode accepted structured output")
	}
}
