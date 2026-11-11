package config

import (
	"encoding/json"
	"testing"
)

func TestCompositeHTTPCapabilitiesSurviveResolution(t *testing.T) {
	var cfg GatewayConfig
	data := []byte(`{"providers":{"p":{"protocol":"openai"}},"models":{"child":{"request_types":["chat_completion"],"context_length":64000,"max_output_tokens":8192,"abilities":["stream"],"endpoints":[{"id":"override","provider":"p","url":"https://example.test","real_model":"child","context_length":32000,"max_output_tokens":4096},{"id":"inherit","provider":"p","url":"https://example.test","real_model":"child"}]}}}`)
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	eps := Resolve(&cfg)["child"]
	if len(eps) != 2 {
		t.Fatalf("endpoints=%v", eps)
	}
	if eps[0].ContextLength != 32000 || eps[0].MaxOutputTokens != 4096 || eps[1].ContextLength != 64000 || eps[1].MaxOutputTokens != 8192 {
		t.Fatalf("capacity inheritance lost: %+v", eps)
	}
	for _, ep := range eps {
		if len(ep.Abilities) != 1 || ep.Abilities[0] != "stream" {
			t.Fatalf("stream declaration lost: %+v", ep)
		}
	}
	eps[0].Abilities[0] = "changed"
	if cfg.Models["child"].Abilities[0] != "stream" {
		t.Fatal("resolved endpoint aliases source abilities")
	}
}
