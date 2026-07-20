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

func TestAnthropicMessagesInvoker_NonStream(t *testing.T) {
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
		json.Unmarshal(body, &req)
		if _, ok := req["messages"]; !ok {
			t.Error("expected messages field")
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"msg_123","type":"message","role":"assistant","content":[{"type":"text","text":"Hello!"}],"usage":{"input_tokens":15,"output_tokens":8}}`))
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"max_tokens":100}`),
		IsStream:    false,
	}

	err := p.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 15 {
		t.Errorf("expected prompt_tokens=15, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 8 {
		t.Errorf("expected completion_tokens=8, got %d", gctx.OutputTokens)
	}
	if gctx.UpstreamBody == nil {
		t.Fatal("expected UpstreamBody to be set")
	}
	if gctx.Response != nil {
		t.Fatal("Response should NOT be set (Engine uses UpstreamBody for passthrough)")
	}
}

func TestAnthropicMessagesInvoker_Stream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		events := []string{
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" there\"}}\n\n",
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
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
		RequestType:    core.RequestTypeMessages,
		RawBody:        []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"stream":true}`),
		IsStream:       true,
		ResponseWriter: rec,
		StartTime:      time.Now().Add(-100 * time.Millisecond),
	}

	err := p.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 10 {
		t.Errorf("expected prompt_tokens=10, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 5 {
		t.Errorf("expected completion_tokens=5, got %d", gctx.OutputTokens)
	}
	if gctx.TTFT <= 0 {
		t.Error("expected TTFT > 0")
	}
	if rec.Body.Len() == 0 {
		t.Error("expected body to be written to ResponseWriter")
	}
}

func TestAnthropicMessagesInvoker_UpstreamError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"bad request"}}`))
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[],"max_tokens":10}`),
		IsStream:    false,
	}

	err := p.Invoke(gctx)
	if err == nil {
		t.Fatal("expected error for upstream 400")
	}
}

func TestAnthropicMessagesInvoker_CustomHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Custom-Auth") != "custom-value" {
			t.Errorf("expected X-Custom-Auth: custom-value, got %s", r.Header.Get("X-Custom-Auth"))
		}
		if r.Header.Get("x-api-key") != "override-key" {
			t.Errorf("expected x-api-key: override-key, got %s", r.Header.Get("x-api-key"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"test"}],"max_tokens":100}`),
		IsStream:    false,
		SelectedEndpoint: &core.Endpoint{
			Headers: map[string]string{
				"X-Custom-Auth": "custom-value",
				"x-api-key":     "override-key",
			},
		},
	}

	err := p.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestAnthropicProvider_RequestTypes(t *testing.T) {
	p := NewAnthropicProvider("anthropic", "", "", nil)
	caps := p.RequestTypes()
	if len(caps) != 2 || caps[0] != core.RequestTypeMessages || caps[1] != core.RequestTypeResponses {
		t.Errorf("expected [messages responses], got %v", caps)
	}
}

func TestAnthropicProvider_HealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
	err := p.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestAnthropicMessages_ProbeNonStream(t *testing.T) {
	p := NewAnthropicProvider("anthropic", "http://localhost:1234", "test-key", nil)

	reqBody := `{"model": "claude-sonnet-4-20250514", "messages": [{"role": "user", "content": "."}], "max_tokens": 1}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "claude-sonnet-4-20250514"
	gctx.IsStream = false

	invoker := &anthropicMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal probe response: %v", err)
	}
	if resp["type"] != "message" || resp["model"] != "claude-sonnet-4-20250514" {
		t.Errorf("unexpected probe response: %v", resp)
	}
}

func TestAnthropicMessages_ProbeStream(t *testing.T) {
	p := NewAnthropicProvider("anthropic", "http://localhost:1234", "test-key", nil)

	reqBody := `{"model": "claude-sonnet-4-20250514", "messages": [{"role": "user", "content": "."}], "max_tokens": 1, "stream": true}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "claude-sonnet-4-20250514"
	gctx.IsStream = true

	invoker := &anthropicMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	body := w.Body.String()
	if !strings.Contains(body, "event: message_start") || !strings.Contains(body, "event: message_stop") {
		t.Errorf("expected stream events, got %s", body)
	}
}
