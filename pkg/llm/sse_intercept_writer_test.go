package llm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// mockFlusher wraps httptest.ResponseRecorder to support Flusher interface
type mockFlusher struct {
	*httptest.ResponseRecorder
	flushCount int
}

func (f *mockFlusher) Flush() {
	f.flushCount++
}

func TestSSEInterceptWriter_RecordsTTFT(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	w := NewSSEInterceptWriter(gctx)
	if gctx.TTFT != 0 {
		t.Fatal("expected TTFT=0 before first write")
	}

	_, err := w.Write([]byte("data: test\n\n"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	if gctx.TTFT <= 0 {
		t.Error("expected TTFT > 0 after first write")
	}
}

func TestSSEInterceptWriter_ParsesTokensFromStream(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	w := NewSSEInterceptWriter(gctx)

	streamData := "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\" there\"}}]}\n\n" +
		"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n" +
		"data: [DONE]\n\n"

	_, err := w.Write([]byte(streamData))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	if gctx.InputTokens != 10 {
		t.Errorf("expected prompt_tokens=10, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 5 {
		t.Errorf("expected completion_tokens=5, got %d", gctx.OutputTokens)
	}
}

func TestSSEInterceptWriter_PassthroughToUnderlyingWriter(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	w := NewSSEInterceptWriter(gctx)
	payload := []byte("data: {\"test\":true}\n\n")
	_, _ = w.Write(payload)

	if rec.Body.String() != string(payload) {
		t.Errorf("expected data to pass through to underlying writer")
	}
}

func TestSSEInterceptWriter_FlushDelegates(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	w := NewSSEInterceptWriter(gctx)
	w.Flush()

	if flusher.flushCount != 1 {
		t.Errorf("expected flush count=1, got %d", flusher.flushCount)
	}
}

func TestSSEInterceptWriter_HeaderDelegates(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	w := NewSSEInterceptWriter(gctx)
	w.Header().Set("X-Custom", "value")

	if rec.Header().Get("X-Custom") != "value" {
		t.Error("expected Header() to delegate to underlying writer")
	}
}

func TestSSEInterceptWriter_WriteHeaderDelegates(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	w := NewSSEInterceptWriter(gctx)
	w.WriteHeader(http.StatusAccepted)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, rec.Code)
	}
}

func TestSSEInterceptWriter_CustomTokenExtractor(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	// 自定义提取器：从 {"tokens":{"in":N,"out":N}} 格式提取
	custom := func(data string) (int, int, int, int) {
		var payload struct {
			Tokens *struct {
				In  int `json:"in"`
				Out int `json:"out"`
			} `json:"tokens"`
		}
		if err := json.Unmarshal([]byte(data), &payload); err != nil || payload.Tokens == nil {
			return 0, 0, 0, 0
		}
		return payload.Tokens.In, payload.Tokens.Out, 0, 0
	}

	w := NewSSEInterceptWriter(gctx, WithTokenExtractor(custom))

	streamData := "data: {\"tokens\":{\"in\":15,\"out\":8}}\n\n" +
		"data: [DONE]\n\n"

	_, err := w.Write([]byte(streamData))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	if gctx.InputTokens != 15 {
		t.Errorf("expected prompt_tokens=15, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 8 {
		t.Errorf("expected completion_tokens=8, got %d", gctx.OutputTokens)
	}
}
