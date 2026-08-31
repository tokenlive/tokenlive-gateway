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

func TestHandleOpenAIStream_SplitErrorFrameIsNotForwarded(t *testing.T) {
	const upstreamError = `{"error":{"cause":"","code":504,"message":"模型返回异常，无具体用量信息","status":"INVALID_RESPONSE"},"requestId":"repro-request","result":null}`

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		`{"model":"kimi-k3","messages":[],"stream":true}`,
	))
	gctx := core.AcquireContext(rec, req)
	defer core.ReleaseContext(gctx)
	gctx.RequestType = core.RequestTypeChatCompletion
	gctx.Model = "kimi-k3"
	gctx.IsStream = true
	gctx.StartTime = time.Now()

	resp := &http.Response{
		Body: &chunkedStreamReadCloser{chunks: [][]byte{
			[]byte("data: " + upstreamError + "\n"),
			[]byte("\n"),
		}},
	}

	err := handleOpenAIStream(gctx, resp)
	if err == nil || !strings.Contains(err.Error(), "upstream stream returned error event") {
		t.Fatalf("expected detected upstream error, got %v", err)
	}
	if gctx.TTFT != 0 {
		t.Fatalf("error frame must be intercepted before starting the client stream, got TTFT %s", gctx.TTFT)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("error frame must not be forwarded to the client, got %q", rec.Body.String())
	}
}

func TestHandleOpenAIStream_CROnlyErrorFrameIsNotForwarded(t *testing.T) {
	const upstreamError = `{"error":{"message":"upstream failed","type":"upstream_error"}}`

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(rec, req)
	defer core.ReleaseContext(gctx)
	gctx.RequestType = core.RequestTypeChatCompletion
	gctx.IsStream = true
	gctx.StartTime = time.Now()

	resp := &http.Response{
		Body: &chunkedStreamReadCloser{chunks: [][]byte{
			[]byte("data: " + upstreamError + "\r"),
			[]byte("\r"),
		}},
	}

	err := handleOpenAIStream(gctx, resp)
	if err == nil || !strings.Contains(err.Error(), "upstream stream returned error event") {
		t.Fatalf("expected detected upstream error, got %v", err)
	}
	if gctx.TTFT != 0 || rec.Body.Len() != 0 {
		t.Fatalf("CR-only error frame leaked: TTFT=%s body=%q", gctx.TTFT, rec.Body.String())
	}
}

func TestHandleOpenAIStream_SplitNormalFramesAreForwardedUnchanged(t *testing.T) {
	const firstFrame = "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\r\n\r\n"
	const usageFrame = "data: {\"id\":\"chatcmpl-1\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n"
	const doneFrame = "data: [DONE]\n\n"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(rec, req)
	defer core.ReleaseContext(gctx)
	gctx.RequestType = core.RequestTypeChatCompletion
	gctx.Model = "gpt-4"
	gctx.IsStream = true
	gctx.StartTime = time.Now()

	resp := &http.Response{
		Body: &chunkedStreamReadCloser{chunks: [][]byte{
			[]byte(strings.TrimSuffix(firstFrame, "\r\n")),
			[]byte("\r\n"),
			[]byte(usageFrame + doneFrame),
		}},
	}

	if err := handleOpenAIStream(gctx, resp); err != nil {
		t.Fatalf("expected normal split stream to succeed, got %v", err)
	}
	if got, want := rec.Body.String(), firstFrame+usageFrame+doneFrame; got != want {
		t.Fatalf("stream bytes changed:\nwant %q\ngot  %q", want, got)
	}
	if gctx.TTFT <= 0 {
		t.Fatal("expected normal stream to record TTFT")
	}
	if gctx.InputTokens != 5 || gctx.OutputTokens != 2 {
		t.Fatalf("expected usage 5/2, got %d/%d", gctx.InputTokens, gctx.OutputTokens)
	}
}

func TestHandleOpenAIStream_TruncatedCompletionAtEOFStaysIncomplete(t *testing.T) {
	const createdFrame = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\",\"model\":\"gpt-4\"}}\n\n"
	const truncatedCompletedFrame = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"status\":\"completed\"}}\n"

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	gctx := core.AcquireContext(rec, req)
	defer core.ReleaseContext(gctx)
	gctx.RequestType = core.RequestTypeResponses
	gctx.Model = "gpt-4"
	gctx.IsStream = true
	gctx.StartTime = time.Now()

	resp := &http.Response{
		Body: &chunkedStreamReadCloser{chunks: [][]byte{
			[]byte(createdFrame),
			[]byte(truncatedCompletedFrame),
		}},
	}

	err := handleOpenAIStream(gctx, resp)
	if err == nil || !strings.Contains(err.Error(), "incomplete SSE frame") {
		t.Fatalf("expected incomplete SSE frame error, got %v", err)
	}
	if got := rec.Body.String(); got != createdFrame {
		t.Fatalf("truncated frame must not be forwarded:\nwant %q\ngot  %q", createdFrame, got)
	}
	if gctx.GetTagValue("response_completed_sent") != "" {
		t.Fatal("truncated response.completed must not mark the response complete")
	}
}

func TestHandleOpenAIStream_OversizedFrameIsRejectedBeforeForwarding(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(rec, req)
	defer core.ReleaseContext(gctx)
	gctx.RequestType = core.RequestTypeChatCompletion
	gctx.Model = "gpt-4"
	gctx.IsStream = true
	gctx.StartTime = time.Now()

	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader("data: " + strings.Repeat("x", (1<<20)+1))),
	}

	err := handleOpenAIStream(gctx, resp)
	if err == nil || !strings.Contains(err.Error(), "SSE frame exceeds") {
		t.Fatalf("expected oversized SSE frame error, got %v", err)
	}
	if gctx.TTFT != 0 {
		t.Fatalf("oversized frame must be rejected before starting the client stream, got TTFT %s", gctx.TTFT)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("oversized frame must not be forwarded, got %d bytes", rec.Body.Len())
	}
}

type chunkedStreamReadCloser struct {
	chunks [][]byte
	index  int
}

func (r *chunkedStreamReadCloser) Read(p []byte) (int, error) {
	if r.index >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.index])
	r.index++
	return n, nil
}

func (r *chunkedStreamReadCloser) Close() error { return nil }

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
