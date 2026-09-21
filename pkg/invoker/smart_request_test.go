package invoker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestSmartRequestRejectsUnsupportedInputs(t *testing.T) {
	for _, body := range []string{
		`{"messages":[{"role":"user","content":"hello"}],"stream":true}`,
		`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`,
		`{"messages":[{"role":"tool","content":"result"}]}`,
		`{"messages":[{"role":"assistant","content":null,"tool_calls":[{}]}]}`,
		`{"messages":[{"role":"user","content":"hi"}],"tools":[{}]}`,
		`{"messages":[{"role":"user","content":"hi"}],"functions":[{}]}`,
		`{"messages":[]}`,
		`{"messages":[{"role":"user","content":"hi"}],"max_tokens":-1}`,
		`{"messages":[{"role":"user","content":"hi"}],"stream":"true"}`,
	} {
		if _, err := parseSmartRequest(core.RequestTypeChatCompletion, []byte(body)); err == nil {
			t.Fatalf("accepted unsupported request: %s", body)
		}
	}
	if _, err := parseSmartRequest(core.RequestTypeResponses, []byte(`{"input":"hello"}`)); err == nil {
		t.Fatal("accepted responses protocol")
	}
}

func TestSmartJudgeInputPreservesHistoryAndEnforcesBudget(t *testing.T) {
	body := []byte(`{"messages":[{"role":"system","content":"verify every assumption"},{"role":"user","content":"prove an invariant"},{"role":"assistant","content":"first step"},{"role":"user","content":"continue"}]}`)
	req, err := parseSmartRequest(core.RequestTypeChatCompletion, body)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &core.SmartRoutingConfig{JudgeModel: "judge", JudgeMaxInputBytes: 65536, JudgeMaxOutputTokens: 256}
	judgeBody, err := buildSmartJudgeRequest(req, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var judge struct {
		Messages []struct{ Role, Content string }
	}
	if err := json.Unmarshal(judgeBody, &judge); err != nil {
		t.Fatal(err)
	}
	if len(judge.Messages) != 2 || judge.Messages[0].Role != "system" {
		t.Fatalf("expected trusted instruction and data envelope, got %+v", judge)
	}
	for _, content := range []string{"verify every assumption", "prove an invariant", "first step", "continue"} {
		if !strings.Contains(judge.Messages[1].Content, content) {
			t.Fatalf("missing history %q", content)
		}
	}
	cfg.JudgeMaxInputBytes = 20
	if _, err := buildSmartJudgeRequest(req, cfg); err == nil {
		t.Fatal("oversized context must fail rather than be truncated")
	}
}

func TestSmartJudgeScoreParsingAndBoundarySelection(t *testing.T) {
	for _, body := range []string{
		`{}`, `{"score":null}`, `{"score":"20"}`, `{"score":20.5}`, `{"score":-1}`,
		`{"score":101}`, `{"score":80,"uncertain":true}`, "not json", `{"score":10} {"score":99}`,
	} {
		if _, _, err := parseSmartScore([]byte(body)); err == nil {
			t.Fatalf("accepted invalid judgment %s", body)
		}
	}
	ranges := []core.SmartRoutingRange{{Min: 0, Max: 40, Model: "a"}, {Min: 40, Max: 75, Model: "b"}, {Min: 75, Max: 100, Model: "c"}}
	for _, tc := range []struct{ score, index int }{{0, 0}, {39, 0}, {40, 1}, {74, 1}, {75, 2}, {100, 2}} {
		if got := smartRangeIndex(ranges, tc.score); got != tc.index {
			t.Fatalf("score %d: index %d, want %d", tc.score, got, tc.index)
		}
	}
	score, _, err := parseSmartScore([]byte(`{"score":0,"uncertain":false,"reason_code":"simple"}`))
	if err != nil || score != 0 {
		t.Fatalf("zero is a real score: %d, %v", score, err)
	}
}

func TestSmartModelRewritePreservesExactPayloadNumbers(t *testing.T) {
	body := []byte(`{"model":"smart","seed":9007199254740993,"messages":[{"role":"user","content":"hi"}]}`)
	got := smartModelBody(body, "cheap")
	if !strings.Contains(string(got), `9007199254740993`) || !strings.Contains(string(got), `"model":"cheap"`) {
		t.Fatalf("request payload changed beyond model: %s", got)
	}
	upstream := replaceModelInBody(got, "upstream-model")
	if !strings.Contains(string(upstream), `9007199254740993`) {
		t.Fatalf("provider alias rewrite lost payload precision: %s", upstream)
	}
}
