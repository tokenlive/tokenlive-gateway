package providers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestOpenAIProvider_ChatCompletion_NonStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected auth header: %s", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":      "chatcmpl-123",
			"object":  "chat.completion",
			"choices": []interface{}{},
			"usage": map[string]interface{}{
				"prompt_tokens":     10,
				"completion_tokens": 20,
				"total_tokens":      30,
			},
		})
	}))
	defer server.Close()

	p := NewOpenAIProvider("openai", server.URL, "test-key", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeChatCompletion,
		RawBody:     []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}`),
		IsStream:    false,
	}

	err := p.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 10 {
		t.Errorf("expected prompt_tokens=10, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 20 {
		t.Errorf("expected completion_tokens=20, got %d", gctx.OutputTokens)
	}
}

func TestOpenAIProvider_ChatCompletion_Stream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		chunks := []string{
			"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n",
			"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n",
			"data: {\"id\":\"chatcmpl-1\",\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
			"data: [DONE]\n\n",
		}
		for _, chunk := range chunks {
			_, _ = w.Write([]byte(chunk))
			flusher.Flush()
		}
	}))
	defer server.Close()

	p := NewOpenAIProvider("openai", server.URL, "test-key", nil)

	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            context.Background(),
		RequestType:    core.RequestTypeChatCompletion,
		RawBody:        []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stream":true}`),
		IsStream:       true,
		ResponseWriter: rec,
		StartTime:      time.Now().Add(-100 * time.Millisecond),
	}

	err := p.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 5 {
		t.Errorf("expected prompt_tokens=5, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 2 {
		t.Errorf("expected completion_tokens=2, got %d", gctx.OutputTokens)
	}
	if gctx.TTFT <= 0 {
		t.Error("expected TTFT > 0 for stream")
	}
}

func TestOpenAIProvider_Embedding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"object": "list",
			"data":   []interface{}{},
			"usage": map[string]interface{}{
				"prompt_tokens": 8,
				"total_tokens":  8,
			},
		})
	}))
	defer server.Close()

	p := NewOpenAIProvider("openai", server.URL, "test-key", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeEmbedding,
		RawBody:     []byte(`{"model":"text-embedding-3-small","input":"hello"}`),
		IsStream:    false,
	}

	err := p.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 8 {
		t.Errorf("expected prompt_tokens=8, got %d", gctx.InputTokens)
	}
}

func TestOpenAIProvider_UpstreamError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":{"message":"server error","type":"server_error"}}`))
	}))
	defer server.Close()

	p := NewOpenAIProvider("openai", server.URL, "test-key", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeChatCompletion,
		RawBody:     []byte(`{"model":"gpt-4","messages":[]}`),
		IsStream:    false,
	}

	err := p.Invoke(gctx)
	if err == nil {
		t.Fatal("expected error for upstream 500")
	}
	if gctx.UpstreamResponse == nil || gctx.UpstreamResponse.StatusCode != 500 {
		t.Error("expected UpstreamResponse with status 500")
	}
}

func TestOpenAIProvider_RequestTypes(t *testing.T) {
	p := NewOpenAIProvider("openai", "", "", nil)
	caps := p.RequestTypes()
	if len(caps) != 4 {
		t.Fatalf("expected 4 requestTypes, got %d", len(caps))
	}
}

func TestOpenAIProvider_HealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	p := NewOpenAIProvider("openai", server.URL, "test-key", nil)
	err := p.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestOpenAIProvider_HealthCheck_Fail(t *testing.T) {
	p := NewOpenAIProvider("openai", "http://localhost:1", "test-key", nil)
	err := p.HealthCheck(context.Background())
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

func TestOpenAIProvider_CustomHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Custom-Auth") != "custom-value" {
			t.Errorf("expected custom header X-Custom-Auth: custom-value, got %s", r.Header.Get("X-Custom-Auth"))
		}
		if r.Header.Get("Authorization") != "override-bearer" {
			t.Errorf("expected overridden Authorization: override-bearer, got %s", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":      "chatcmpl-123",
			"object":  "chat.completion",
			"choices": []interface{}{},
			"usage":   map[string]interface{}{},
		})
	}))
	defer server.Close()

	p := NewOpenAIProvider("openai", server.URL, "test-key", nil)
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeChatCompletion,
		RawBody:     []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}`),
		IsStream:    false,
		SelectedEndpoint: &core.Endpoint{
			Headers: map[string]string{
				"X-Custom-Auth": "custom-value",
				"Authorization": "override-bearer",
			},
		},
	}

	err := p.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestEnsureStreamUsage_InjectsWhenMissing(t *testing.T) {
	body := []byte(`{"model":"gpt-4","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	result := ensureStreamUsage(body)

	var m map[string]interface{}
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	opts, ok := m["stream_options"].(map[string]interface{})
	if !ok {
		t.Fatal("expected stream_options to be present")
	}
	if opts["include_usage"] != true {
		t.Fatalf("expected include_usage=true, got %v", opts["include_usage"])
	}
}

func TestEnsureStreamUsage_PreservesExisting(t *testing.T) {
	body := []byte(`{"model":"gpt-4","stream":true,"stream_options":{"include_usage":true,"other":1}}`)
	result := ensureStreamUsage(body)

	var m map[string]interface{}
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	opts := m["stream_options"].(map[string]interface{})
	if opts["include_usage"] != true {
		t.Fatal("expected include_usage to remain true")
	}
	if opts["other"] != float64(1) {
		t.Fatal("expected other field to be preserved")
	}
}

func TestEnsureStreamUsage_SetsWhenFalse(t *testing.T) {
	body := []byte(`{"model":"gpt-4","stream":true,"stream_options":{"include_usage":false}}`)
	result := ensureStreamUsage(body)

	var m map[string]interface{}
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	opts := m["stream_options"].(map[string]interface{})
	if opts["include_usage"] != true {
		t.Fatalf("expected include_usage to be overwritten to true, got %v", opts["include_usage"])
	}
}

func TestEnsureStreamUsage_ReturnsOriginalOnInvalidJSON(t *testing.T) {
	body := []byte(`not json`)
	result := ensureStreamUsage(body)
	if string(result) != string(body) {
		t.Fatal("expected original body on invalid JSON")
	}
}
