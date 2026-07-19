package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestAnthropicResponsesInvoker_NonStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "sk-ant-test" {
			t.Errorf("unexpected x-api-key: %s", r.Header.Get("x-api-key"))
		}
		if r.Header.Get("anthropic-version") != "2023-06-01" {
			t.Errorf("unexpected anthropic-version: %s", r.Header.Get("anthropic-version"))
		}

		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("upstream request not json: %v", err)
		}
		// Responses request must have been translated to Anthropic Messages shape
		if req["model"] != "claude-sonnet-4-20250514" {
			t.Errorf("model = %v", req["model"])
		}
		if _, ok := req["max_tokens"]; !ok {
			t.Error("expected max_tokens to be synthesized")
		}
		if _, ok := req["input"]; ok {
			t.Error("input field should not leak to anthropic upstream")
		}
		msgs, ok := req["messages"].([]interface{})
		if !ok || len(msgs) != 1 {
			t.Errorf("messages = %v", req["messages"])
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"msg_777","type":"message","role":"assistant","content":[{"type":"text","text":"Hi there"}],"stop_reason":"end_turn","usage":{"input_tokens":12,"output_tokens":6}}`))
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
	gctx := &core.GatewayContext{
		Ctx:           context.Background(),
		RequestType:   core.RequestTypeResponses,
		Model:         "claude-sonnet-4-20250514",
		OriginalModel: "claude-sonnet-4",
		RawBody:       []byte(`{"model":"claude-sonnet-4","input":"hi"}`),
		IsStream:      false,
	}

	if err := p.Invoke(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 12 || gctx.OutputTokens != 6 {
		t.Errorf("usage = %d/%d", gctx.InputTokens, gctx.OutputTokens)
	}
	if gctx.Response == nil {
		t.Fatal("expected gctx.Response to be set")
	}
	resp, ok := gctx.Response.(map[string]interface{})
	if !ok {
		t.Fatalf("gctx.Response type = %T", gctx.Response)
	}
	if resp["id"] != "resp_777" {
		t.Errorf("id = %v", resp["id"])
	}
	if resp["object"] != "response" || resp["status"] != "completed" {
		t.Errorf("resp = %v", resp)
	}
	if resp["model"] != "claude-sonnet-4" {
		t.Errorf("model = %v, want client-facing alias", resp["model"])
	}
	output := resp["output"].([]interface{})
	if len(output) != 1 {
		t.Fatalf("output len = %d", len(output))
	}
	msg := output[0].(map[string]interface{})
	if msg["type"] != "message" {
		t.Errorf("output item = %v", msg)
	}
}

func TestAnthropicResponsesInvoker_NonStreamThinking(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("upstream request not json: %v", err)
		}
		thinking, ok := req["thinking"].(map[string]interface{})
		if !ok || thinking["type"] != "enabled" {
			t.Errorf("thinking = %v", req["thinking"])
		}
		if _, ok := req["temperature"]; ok {
			t.Error("temperature should be stripped when thinking enabled")
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"msg_888","type":"message","role":"assistant","content":[{"type":"thinking","thinking":"hmm","signature":"sig"},{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":5,"output_tokens":10}}`))
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeResponses,
		Model:       "claude-sonnet-4-20250514",
		RawBody:     []byte(`{"model":"m","input":"hi","reasoning":{"effort":"low"},"temperature":0.7}`),
		IsStream:    false,
	}

	if err := p.Invoke(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	output := gctx.Response.(map[string]interface{})["output"].([]interface{})
	if len(output) != 2 {
		t.Fatalf("output len = %d", len(output))
	}
	reasoning := output[0].(map[string]interface{})
	if reasoning["type"] != "reasoning" || reasoning["encrypted_content"] != "sig" {
		t.Errorf("reasoning = %v", reasoning)
	}
}

func TestAnthropicResponsesInvoker_Stream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		events := []string{
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_S1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":2,\"output_tokens\":0}}}\n\n",
			"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
			"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n",
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
		}
		for _, ev := range events {
			w.Write([]byte(ev))
			flusher.Flush()
		}
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)

	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            context.Background(),
		RequestType:    core.RequestTypeResponses,
		Model:          "claude-sonnet-4-20250514",
		OriginalModel:  "claude-sonnet-4",
		RawBody:        []byte(`{"model":"claude-sonnet-4","input":"hi","stream":true}`),
		IsStream:       true,
		ResponseWriter: rec,
		StartTime:      time.Now().Add(-100 * time.Millisecond),
	}

	if err := p.Invoke(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"event: response.created",
		"event: response.in_progress",
		"event: response.output_item.added",
		"event: response.content_part.added",
		"event: response.output_text.delta",
		"event: response.output_text.done",
		"event: response.output_item.done",
		"event: response.done",
		"event: response.completed",
		"data: [DONE]",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in stream output:\n%s", want, body)
		}
	}
	if !strings.Contains(body, `"id":"resp_S1"`) {
		t.Errorf("response id not rewritten:\n%s", body)
	}

	if gctx.InputTokens != 12 { // 10 + 2 cached normalized
		t.Errorf("InputTokens = %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 4 {
		t.Errorf("OutputTokens = %d", gctx.OutputTokens)
	}
	if gctx.CachedTokens != 2 {
		t.Errorf("CachedTokens = %d", gctx.CachedTokens)
	}
	if gctx.TTFT <= 0 {
		t.Error("expected TTFT > 0")
	}
	if gctx.Tags["response_completed_sent"] != "true" {
		t.Error("response_completed_sent tag should be set")
	}
	if gctx.Tags["response_id"] != "resp_S1" {
		t.Errorf("response_id tag = %v", gctx.Tags["response_id"])
	}
}

func TestAnthropicResponsesInvoker_UpstreamErrorTranslated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`))
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeResponses,
		Model:       "claude-sonnet-4-20250514",
		RawBody:     []byte(`{"model":"m","input":"hi"}`),
		IsStream:    false,
	}

	err := p.Invoke(gctx)
	if err == nil {
		t.Fatal("expected error for upstream 429")
	}
	// Status prefix must survive for engine.getErrorCode, body becomes Responses envelope
	if !strings.Contains(err.Error(), "upstream error: status 429") {
		t.Errorf("status prefix lost: %v", err)
	}
	if !strings.Contains(err.Error(), `"type":"rate_limit_error"`) || !strings.Contains(err.Error(), "slow down") {
		t.Errorf("anthropic error not translated into envelope: %v", err)
	}
	if strings.Contains(err.Error(), `"type":"error"`) {
		t.Errorf("raw anthropic envelope should have been replaced: %v", err)
	}
}
