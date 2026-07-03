package providers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestGeminiProvider_GenerateContent_NonStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models/gemini-2.5-flash:generateContent" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.RawQuery != "" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}
		if r.Header.Get("x-goog-api-key") != "gemini-key" {
			t.Fatalf("unexpected x-goog-api-key: %s", r.Header.Get("x-goog-api-key"))
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"contents"`) {
			t.Fatalf("expected native body passthrough, got %s", string(body))
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"hello"}]}}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":11,"totalTokenCount":18}}`))
	}))
	defer server.Close()

	p := NewGeminiProvider("gemini", server.URL, "gemini-key", []string{"gemini-2.5-flash"})
	gctx := &core.GatewayContext{
		Ctx:         context.Background(),
		RequestType: core.RequestTypeGeminiGenerateContent,
		Model:       "gemini-2.5-flash",
		RawBody:     []byte(`{"contents":[{"parts":[{"text":"hi"}]}]}`),
		IsStream:    false,
		SelectedEndpoint: &core.Endpoint{
			UpstreamModel: "gemini-2.5-flash",
		},
	}

	if err := p.Invoke(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 7 {
		t.Fatalf("InputTokens=%d, want 7", gctx.InputTokens)
	}
	if gctx.OutputTokens != 11 {
		t.Fatalf("OutputTokens=%d, want 11", gctx.OutputTokens)
	}
	if string(gctx.UpstreamBody) == "" {
		t.Fatal("expected upstream body passthrough to be recorded")
	}
	if gctx.Response != nil {
		t.Fatal("expected Response to stay nil for passthrough")
	}
}

func TestGeminiProvider_GenerateContent_Stream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models/gemini-2.5-flash:streamGenerateContent" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("alt") != "sse" {
			t.Fatalf("expected alt=sse, got query %s", r.URL.RawQuery)
		}
		if r.Header.Get("x-goog-api-key") != "gemini-key" {
			t.Fatalf("unexpected x-goog-api-key: %s", r.Header.Get("x-goog-api-key"))
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		events := []string{
			"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hel\"}]}}]}\n\n",
			"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"lo\"}]}}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":2,\"totalTokenCount\":7}}\n\n",
		}
		for _, event := range events {
			_, _ = w.Write([]byte(event))
			flusher.Flush()
		}
	}))
	defer server.Close()

	p := NewGeminiProvider("gemini", server.URL, "gemini-key", []string{"gemini-2.5-flash"})
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            context.Background(),
		RequestType:    core.RequestTypeGeminiGenerateContent,
		Model:          "gemini-2.5-flash",
		RawBody:        []byte(`{"contents":[{"parts":[{"text":"hi"}]}]}`),
		IsStream:       true,
		ResponseWriter: rec,
		StartTime:      time.Now().Add(-100 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{
			UpstreamModel: "gemini-2.5-flash",
		},
	}

	if err := p.Invoke(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.InputTokens != 5 {
		t.Fatalf("InputTokens=%d, want 5", gctx.InputTokens)
	}
	if gctx.OutputTokens != 2 {
		t.Fatalf("OutputTokens=%d, want 2", gctx.OutputTokens)
	}
	if !strings.Contains(rec.Body.String(), `"usageMetadata"`) {
		t.Fatalf("expected SSE passthrough body, got %s", rec.Body.String())
	}
	if gctx.TTFT <= 0 {
		t.Fatal("expected TTFT to be recorded")
	}
}

func TestGeminiProvider_RequestTypes(t *testing.T) {
	p := NewGeminiProvider("gemini", "", "", nil)
	caps := p.RequestTypes()
	if len(caps) != 1 || caps[0] != core.RequestTypeGeminiGenerateContent {
		t.Fatalf("expected [gemini_generate_content], got %v", caps)
	}
}
