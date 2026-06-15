# Plan A: Provider + SSE + Config Wiring

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable the tokenlive-gateway to make real LLM API calls by implementing OpenAI and Anthropic providers with full SSE streaming support, and wiring hot-reload config loading.

**Architecture:** Providers implement `core.Provider` interface, receiving a `GatewayContext` containing the raw HTTP request. Each provider translates between OpenAI-compatible format and the upstream LLM API. Streaming responses are piped through `SSEInterceptWriter` which intercepts SSE frames for token counting and TTFT measurement. The config watcher loads YAML via Viper into `EngineConfig` for hot-reload.

**Tech Stack:** Go stdlib `net/http`, `encoding/json`, `bufio.Scanner`, `github.com/spf13/viper`, `go.uber.org/zap`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `pkg/llm/sse_parser.go` | Create | SSE frame parser: extract data payloads, detect [DONE], parse usage chunk for tokens |
| `pkg/llm/sse_parser_test.go` | Create | Unit tests for SSE parser |
| `pkg/llm/sse_intercept_writer.go` | Create | `http.ResponseWriter` wrapper: records TTFT, feeds SSE frames to parser, fills `gctx` tokens |
| `pkg/llm/sse_intercept_writer_test.go` | Create | Unit tests with mock Flusher |
| `pkg/llm/providers/openai.go` | Create | OpenAI provider: chat/completions, embeddings, model list; stream + non-stream |
| `pkg/llm/providers/openai_test.go` | Create | Tests with httptest mock server |
| `pkg/llm/providers/anthropic.go` | Create | Anthropic provider: Messages API → OpenAI format conversion; stream + non-stream |
| `pkg/llm/providers/anthropic_test.go` | Create | Tests with httptest mock server |
| `pkg/llm/factory.go` | Create | Provider factory: `NewProvider(name, config) (core.Provider, error)` with registry |
| `pkg/llm/factory_test.go` | Create | Factory tests |
| `pkg/core/config_watcher.go:145-149` | Modify | Implement `LoadConfig()` using Viper YAML deserialization |
| `cmd/server/wire/provider.go` | Modify | Wire real providers into Engine via factory |
| `cmd/server/wire/wire.go` | Modify | Add LLM factory to Wire set (if needed) |

---

### Task 1: SSE Parser

**Files:**

- Create: `pkg/llm/sse_parser.go`
- Create: `pkg/llm/sse_parser_test.go`

- [ ] **Step 1: Write failing tests for SSEParser**

```go
// pkg/llm/sse_parser_test.go
package llm

import (
 "testing"
)

func TestSSEParser_ParseSimpleEvent(t *testing.T) {
 p := NewSSEParser()
 events := p.Feed([]byte("data: {\"id\":\"chatcmpl-1\"}\n\n"))
 if len(events) != 1 {
  t.Fatalf("expected 1 event, got %d", len(events))
 }
 if events[0].Data != `{"id":"chatcmpl-1"}` {
  t.Errorf("unexpected data: %s", events[0].Data)
 }
}

func TestSSEParser_ParseMultipleEvents(t *testing.T) {
 p := NewSSEParser()
 input := "data: {\"a\":1}\n\ndata: {\"b\":2}\n\n"
 events := p.Feed([]byte(input))
 if len(events) != 2 {
  t.Fatalf("expected 2 events, got %d", len(events))
 }
}

func TestSSEParser_MultiLineData(t *testing.T) {
 p := NewSSEParser()
 input := "data: line1\ndata: line2\n\n"
 events := p.Feed([]byte(input))
 if len(events) != 1 {
  t.Fatalf("expected 1 event, got %d", len(events))
 }
 // Multi-line data concatenated with newline
 if events[0].Data != "line1\nline2" {
  t.Errorf("expected 'line1\\nline2', got '%s'", events[0].Data)
 }
}

func TestSSEParser_DoneSignal(t *testing.T) {
 p := NewSSEParser()
 events := p.Feed([]byte("data: [DONE]\n\n"))
 if len(events) != 1 {
  t.Fatalf("expected 1 event, got %d", len(events))
 }
 if !events[0].Done {
  t.Error("expected Done=true for [DONE] event")
 }
}

func TestSSEParser_ExtractUsageTokens(t *testing.T) {
 p := NewSSEParser()
 usageJSON := `{"id":"chatcmpl-1","usage":{"prompt_tokens":10,"completion_tokens":20}}`
 events := p.Feed([]byte("data: " + usageJSON + "\n\n"))
 if len(events) != 1 {
  t.Fatalf("expected 1 event, got %d", len(events))
 }
 if events[0].PromptTokens != 10 {
  t.Errorf("expected prompt_tokens=10, got %d", events[0].PromptTokens)
 }
 if events[0].CompletionTokens != 20 {
  t.Errorf("expected completion_tokens=20, got %d", events[0].CompletionTokens)
 }
}

func TestSSEParser_PartialData(t *testing.T) {
 p := NewSSEParser()
 // Feed data in chunks
 events1 := p.Feed([]byte("data: {\"id"))
 if len(events1) != 0 {
  t.Fatalf("expected 0 events from partial feed, got %d", len(events1))
 }
 events2 := p.Feed([]byte("\":\"1\"}\n\n"))
 if len(events2) != 1 {
  t.Fatalf("expected 1 event after completion, got %d", len(events2))
 }
}

func TestSSEParser_SkipEmptyLines(t *testing.T) {
 p := NewSSEParser()
 input := "\n\ndata: test\n\n\n\ndata: test2\n\n"
 events := p.Feed([]byte(input))
 if len(events) != 2 {
  t.Fatalf("expected 2 events, got %d", len(events))
 }
}

func TestSSEParser_IgnoreNonDataFields(t *testing.T) {
 p := NewSSEParser()
 input := "event: message\nid: 123\ndata: payload\n\n"
 events := p.Feed([]byte(input))
 if len(events) != 1 {
  t.Fatalf("expected 1 event, got %d", len(events))
 }
 if events[0].Data != "payload" {
  t.Errorf("expected 'payload', got '%s'", events[0].Data)
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/llm/ -run TestSSEParser -v`
Expected: compilation error — `NewSSEParser` and `SSEParser` not defined.

- [ ] **Step 3: Implement SSEParser**

```go
// pkg/llm/sse_parser.go
package llm

import (
 "encoding/json"
 "strings"
)

// SSEEvent represents a single parsed SSE event
type SSEEvent struct {
 Data             string
 Done             bool
 PromptTokens     int
 CompletionTokens int
}

// SSEParser incrementally parses SSE frames from a byte stream.
// Feed() may be called with partial data; it buffers incomplete lines
// and returns fully parsed events.
type SSEParser struct {
 buf strings.Builder
}

// NewSSEParser creates a new SSEParser
func NewSSEParser() *SSEParser {
 return &SSEParser{}
}

// Feed processes incoming bytes and returns any complete SSE events found.
func (p *SSEParser) Feed(data []byte) []SSEEvent {
 p.buf.Write(data)

 var events []SSEEvent
 fullText := p.buf.String()

 // Process complete blocks (delimited by \n\n)
 for {
  idx := strings.Index(fullText, "\n\n")
  if idx < 0 {
   break
  }
  block := fullText[:idx]
  fullText = fullText[idx+2:]

  if ev, ok := p.parseBlock(block); ok {
   events = append(events, ev)
  }
 }

 // Keep remaining incomplete data
 p.buf.Reset()
 p.buf.WriteString(fullText)

 return events
}

// parseBlock parses a single SSE block (everything before \n\n).
func (p *SSEParser) parseBlock(block string) (SSEEvent, bool) {
 var dataLines []string

 for _, line := range strings.Split(block, "\n") {
  line = strings.TrimSpace(line)
  if line == "" {
   continue
  }

  if strings.HasPrefix(line, "data: ") {
   dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
  }
  // Ignore event:, id:, retry: fields
 }

 if len(dataLines) == 0 {
  return SSEEvent{}, false
 }

 data := strings.Join(dataLines, "\n")
 ev := SSEEvent{Data: data}

 // Check for [DONE] sentinel
 if data == "[DONE]" {
  ev.Done = true
  return ev, true
 }

 // Try to extract usage tokens from JSON
 p.extractUsage(data, &ev)

 return ev, true
}

// extractUsage attempts to parse usage from the JSON data.
// Works for OpenAI format: {"usage":{"prompt_tokens":N,"completion_tokens":N}}
func (p *SSEParser) extractUsage(data string, ev *SSEEvent) {
 var payload struct {
  Usage *struct {
   PromptTokens     int `json:"prompt_tokens"`
   CompletionTokens int `json:"completion_tokens"`
  } `json:"usage"`
 }
 if err := json.Unmarshal([]byte(data), &payload); err != nil {
  return
 }
 if payload.Usage != nil {
  ev.PromptTokens = payload.Usage.PromptTokens
  ev.CompletionTokens = payload.Usage.CompletionTokens
 }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/llm/ -run TestSSEParser -v`
Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/llm/sse_parser.go pkg/llm/sse_parser_test.go
git commit -m "feat(llm): add SSE frame parser with incremental Feed() and usage token extraction"
```

---

### Task 2: SSEInterceptWriter

**Files:**

- Create: `pkg/llm/sse_intercept_writer.go`
- Create: `pkg/llm/sse_intercept_writer_test.go`

- [ ] **Step 1: Write failing tests for SSEInterceptWriter**

```go
// pkg/llm/sse_intercept_writer_test.go
package llm

import (
 "net/http"
 "net/http/httptest"
 "testing"
 "time"

 "tokenlive-gateway/pkg/core"
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

 // Simulate OpenAI streaming response with usage in final chunk
 streamData := "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n" +
  "data: {\"choices\":[{\"delta\":{\"content\":\" there\"}}]}\n\n" +
  "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n" +
  "data: [DONE]\n\n"

 _, err := w.Write([]byte(streamData))
 if err != nil {
  t.Fatalf("write error: %v", err)
 }

 if gctx.PromptTokens != 10 {
  t.Errorf("expected prompt_tokens=10, got %d", gctx.PromptTokens)
 }
 if gctx.CompletionTokens != 5 {
  t.Errorf("expected completion_tokens=5, got %d", gctx.CompletionTokens)
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/llm/ -run TestSSEInterceptWriter -v`
Expected: compilation error — `NewSSEInterceptWriter` not defined.

- [ ] **Step 3: Implement SSEInterceptWriter**

```go
// pkg/llm/sse_intercept_writer.go
package llm

import (
 "net/http"
 "time"

 "tokenlive-gateway/pkg/core"
)

// SSEInterceptWriter transparently wraps http.ResponseWriter.
// It records TTFT on first write, feeds all bytes to SSEParser,
// and populates gctx token counts when usage data arrives.
type SSEInterceptWriter struct {
 http.ResponseWriter
 gctx      *core.GatewayContext
 parser    *SSEParser
 firstByte bool
}

// NewSSEInterceptWriter creates a writer that intercepts SSE frames.
func NewSSEInterceptWriter(gctx *core.GatewayContext) *SSEInterceptWriter {
 return &SSEInterceptWriter{
  ResponseWriter: gctx.ResponseWriter,
  gctx:           gctx,
  parser:         NewSSEParser(),
 }
}

// Write intercepts bytes, records TTFT, and parses SSE frames.
func (w *SSEInterceptWriter) Write(p []byte) (int, error) {
 if !w.firstByte {
  w.firstByte = true
  w.gctx.TTFT = time.Since(w.gctx.StartTime)
 }

 // Parse SSE frames for token extraction
 events := w.parser.Feed(p)
 for _, ev := range events {
  if ev.PromptTokens > 0 || ev.CompletionTokens > 0 {
   w.gctx.PromptTokens = ev.PromptTokens
   w.gctx.CompletionTokens = ev.CompletionTokens
  }
 }

 // Pass through to underlying writer
 return w.ResponseWriter.Write(p)
}

// Flush delegates to underlying Flusher if supported.
func (w *SSEInterceptWriter) Flush() {
 if f, ok := w.ResponseWriter.(http.Flusher); ok {
  f.Flush()
 }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/llm/ -run TestSSEInterceptWriter -v`
Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/llm/sse_intercept_writer.go pkg/llm/sse_intercept_writer_test.go
git commit -m "feat(llm): add SSEInterceptWriter for TTFT recording and stream token extraction"
```

---

### Task 3: OpenAI Provider

**Files:**

- Create: `pkg/llm/providers/openai.go`
- Create: `pkg/llm/providers/openai_test.go`

- [ ] **Step 1: Write failing tests for OpenAIProvider**

```go
// pkg/llm/providers/openai_test.go
package providers

import (
 "context"
 "encoding/json"
 "io"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"

 "tokenlive-gateway/pkg/core"
)

func TestOpenAIProvider_ChatCompletion_NonStream(t *testing.T) {
 // Mock OpenAI server
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
 if gctx.PromptTokens != 10 {
  t.Errorf("expected prompt_tokens=10, got %d", gctx.PromptTokens)
 }
 if gctx.CompletionTokens != 20 {
  t.Errorf("expected completion_tokens=20, got %d", gctx.CompletionTokens)
 }
}

func TestOpenAIProvider_ChatCompletion_Stream(t *testing.T) {
 // Mock OpenAI streaming server
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

 // Use httptest.ResponseRecorder as the underlying writer
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
 if gctx.PromptTokens != 5 {
  t.Errorf("expected prompt_tokens=5, got %d", gctx.PromptTokens)
 }
 if gctx.CompletionTokens != 2 {
  t.Errorf("expected completion_tokens=2, got %d", gctx.CompletionTokens)
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
 if gctx.PromptTokens != 8 {
  t.Errorf("expected prompt_tokens=8, got %d", gctx.PromptTokens)
 }
}

func TestOpenAIProvider_ModelList(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  if r.URL.Path != "/models" {
   t.Errorf("unexpected path: %s", r.URL.Path)
  }
  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(map[string]interface{}{
   "object": "list",
   "data": []interface{}{
    map[string]interface{}{"id": "gpt-4", "object": "model"},
   },
  })
 }))
 defer server.Close()

 p := NewOpenAIProvider("openai", server.URL, "test-key", nil)
 gctx := &core.GatewayContext{
  Ctx:         context.Background(),
  RequestType: core.RequestTypeModelList,
  RawBody:     nil,
  IsStream:    false,
 }

 err := p.Invoke(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if gctx.Response == nil {
  t.Error("expected Response to be set for model_list")
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
 if len(caps) != 3 {
  t.Fatalf("expected 3 RequestTypes, got %d", len(caps))
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/llm/providers/ -run TestOpenAI -v`
Expected: compilation error — `NewOpenAIProvider` not defined.

- [ ] **Step 3: Implement OpenAIProvider**

```go
// pkg/llm/providers/openai.go
package providers

import (
 "bytes"
 "context"
 "encoding/json"
 "fmt"
 "io"
 "net/http"
 "strings"
 "time"

 "tokenlive-gateway/pkg/core"
 "tokenlive-gateway/pkg/llm"
)

const defaultTimeout = 60 * time.Second

// OpenAIProvider implements core.Provider for OpenAI-compatible RequestTypes.
type OpenAIProvider struct {
 name      string
 baseURL   string
 apiKey    string
 client    *http.Client
 models    []string
}

// NewOpenAIProvider creates an OpenAI provider.
// baseURL should be the base URL without trailing path (e.g., "https://api.openai.com/v1").
func NewOpenAIProvider(name, baseURL, apiKey string, models []string) *OpenAIProvider {
 return &OpenAIProvider{
  name:    name,
  baseURL: strings.TrimRight(baseURL, "/"),
  apiKey:  apiKey,
  client:  &http.Client{Timeout: defaultTimeout},
  models:  models,
 }
}

func (p *OpenAIProvider) Name() string               { return p.name }
func (p *OpenAIProvider) Type() core.ProviderType     { return core.ProviderOpenAI }
func (p *OpenAIProvider) ValidateConfig() error       { return nil }

func (p *OpenAIProvider) RequestTypes() []core.RequestType {
 return []core.RequestType{
  core.RequestTypeChatCompletion,
  core.RequestTypeEmbedding,
  core.RequestTypeModelList,
 }
}

func (p *OpenAIProvider) HealthCheck(ctx context.Context) error {
 req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", nil)
 if err != nil {
  return err
 }
 req.Header.Set("Authorization", "Bearer "+p.apiKey)

 resp, err := p.client.Do(req)
 if err != nil {
  return err
 }
 resp.Body.Close()

 if resp.StatusCode >= 500 {
  return fmt.Errorf("health check failed: status %d", resp.StatusCode)
 }
 return nil
}

func (p *OpenAIProvider) Invoke(gctx *core.GatewayContext) error {
 switch gctx.RequestType {
 case core.RequestTypeModelList:
  return p.invokeModelList(gctx)
 case core.RequestTypeEmbedding:
  return p.invokeEmbedding(gctx)
 default:
  return p.invokeChatCompletion(gctx)
 }
}

func (p *OpenAIProvider) invokeChatCompletion(gctx *core.GatewayContext) error {
 endpoint := p.baseURL + "/chat/completions"
 return p.doRequest(gctx, endpoint)
}

func (p *OpenAIProvider) invokeEmbedding(gctx *core.GatewayContext) error {
 endpoint := p.baseURL + "/embeddings"
 return p.doRequest(gctx, endpoint)
}

func (p *OpenAIProvider) invokeModelList(gctx *core.GatewayContext) error {
 endpoint := p.baseURL + "/models"
 req, err := http.NewRequestWithContext(gctx.Ctx, http.MethodGet, endpoint, nil)
 if err != nil {
  return fmt.Errorf("create request: %w", err)
 }
 req.Header.Set("Authorization", "Bearer "+p.apiKey)

 resp, err := p.client.Do(req)
 if err != nil {
  return fmt.Errorf("upstream request: %w", err)
 }
 defer resp.Body.Close()
 gctx.UpstreamResponse = resp

 if resp.StatusCode >= 400 {
  body, _ := io.ReadAll(resp.Body)
  return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
 }

 body, err := io.ReadAll(resp.Body)
 if err != nil {
  return fmt.Errorf("read response: %w", err)
 }
 gctx.UpstreamBody = body

 var result map[string]interface{}
 if err := json.Unmarshal(body, &result); err != nil {
  return fmt.Errorf("parse response: %w", err)
 }
 gctx.Response = result
 return nil
}

// doRequest sends the raw body to the upstream endpoint.
// For streaming requests, it wraps the response writer with SSEInterceptWriter.
// For non-streaming requests, it reads the full response into gctx.
func (p *OpenAIProvider) doRequest(gctx *core.GatewayContext, endpoint string) error {
 req, err := http.NewRequestWithContext(gctx.Ctx, http.MethodPost, endpoint, bytes.NewReader(gctx.RawBody))
 if err != nil {
  return fmt.Errorf("create request: %w", err)
 }
 req.Header.Set("Content-Type", "application/json")
 req.Header.Set("Authorization", "Bearer "+p.apiKey)

 resp, err := p.client.Do(req)
 if err != nil {
  return fmt.Errorf("upstream request: %w", err)
 }
 defer resp.Body.Close()
 gctx.UpstreamResponse = resp

 if resp.StatusCode >= 400 {
  body, _ := io.ReadAll(resp.Body)
  return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
 }

 if gctx.IsStream {
  return p.handleStream(gctx, resp)
 }
 return p.handleNonStream(gctx, resp)
}

// handleStream pipes the upstream SSE stream through SSEInterceptWriter.
func (p *OpenAIProvider) handleStream(gctx *core.GatewayContext, resp *http.Response) error {
 // Install SSEInterceptWriter on the GatewayContext's ResponseWriter
 writer := llm.NewSSEInterceptWriter(gctx)

 // Set SSE headers
 writer.Header().Set("Content-Type", "text/event-stream")
 writer.Header().Set("Cache-Control", "no-cache")
 writer.Header().Set("Connection", "keep-alive")
 writer.WriteHeader(http.StatusOK)

 // Pipe upstream response to client
 buf := make([]byte, 4096)
 for {
  n, err := resp.Body.Read(buf)
  if n > 0 {
   if _, werr := writer.Write(buf[:n]); werr != nil {
    return werr
   }
   writer.Flush()
  }
  if err != nil {
   if err == io.EOF {
    break
   }
   return fmt.Errorf("read upstream stream: %w", err)
  }
 }
 return nil
}

// handleNonStream reads the full response body into gctx.
func (p *OpenAIProvider) handleNonStream(gctx *core.GatewayContext, resp *http.Response) error {
 body, err := io.ReadAll(resp.Body)
 if err != nil {
  return fmt.Errorf("read response: %w", err)
 }
 gctx.UpstreamBody = body

 var result map[string]interface{}
 if err := json.Unmarshal(body, &result); err != nil {
  return fmt.Errorf("parse response: %w", err)
 }
 gctx.Response = result

 // Extract token usage
 if usage, ok := result["usage"].(map[string]interface{}); ok {
  if pt, ok := usage["prompt_tokens"].(float64); ok {
   gctx.PromptTokens = int(pt)
  }
  if ct, ok := usage["completion_tokens"].(float64); ok {
   gctx.CompletionTokens = int(ct)
  }
 }
 return nil
}

// Compile-time check
var _ core.Provider = (*OpenAIProvider)(nil)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/llm/providers/ -run TestOpenAI -v`
Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/llm/providers/openai.go pkg/llm/providers/openai_test.go
git commit -m "feat(providers): add OpenAI provider with chat, embedding, model list, and stream support"
```

---

### Task 4: Anthropic Provider

**Files:**

- Create: `pkg/llm/providers/anthropic.go`
- Create: `pkg/llm/providers/anthropic_test.go`

- [ ] **Step 1: Write failing tests for AnthropicProvider**

```go
// pkg/llm/providers/anthropic_test.go
package providers

import (
 "context"
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "testing"
 "time"

 "tokenlive-gateway/pkg/core"
)

func TestAnthropicProvider_ChatCompletion_NonStream(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  if r.URL.Path != "/v1/messages" {
   t.Errorf("unexpected path: %s", r.URL.Path)
  }
  if r.Header.Get("x-api-key") != "sk-ant-test" {
   t.Errorf("unexpected api key header: %s", r.Header.Get("x-api-key"))
  }
  if r.Header.Get("anthropic-version") != "2023-06-01" {
   t.Errorf("unexpected anthropic-version: %s", r.Header.Get("anthropic-version"))
  }

  // Verify request body was converted to Anthropic format
  var body map[string]interface{}
  json.NewDecoder(r.Body).Decode(&body)
  if _, ok := body["messages"]; !ok {
   t.Error("expected messages field in request body")
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(map[string]interface{}{
   "id":    "msg_123",
   "type":  "message",
   "role":  "assistant",
   "content": []map[string]interface{}{
    {"type": "text", "text": "Hello!"},
   },
   "usage": map[string]interface{}{
    "input_tokens":  15,
    "output_tokens": 8,
   },
  })
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
 gctx := &core.GatewayContext{
  Ctx:         context.Background(),
  RequestType: core.RequestTypeChatCompletion,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}]}`),
  IsStream:    false,
 }

 err := p.Invoke(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if gctx.PromptTokens != 15 {
  t.Errorf("expected prompt_tokens=15, got %d", gctx.PromptTokens)
 }
 if gctx.CompletionTokens != 8 {
  t.Errorf("expected completion_tokens=8, got %d", gctx.CompletionTokens)
 }
 // Verify response was converted to OpenAI format
 if gctx.Response == nil {
  t.Fatal("expected Response to be set")
 }
 resp := gctx.Response.(map[string]interface{})
 if resp["object"] != "chat.completion" {
  t.Errorf("expected object=chat.completion, got %v", resp["object"])
 }
}

func TestAnthropicProvider_Stream(t *testing.T) {
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
   _, _ = w.Write([]byte(ev))
   flusher.Flush()
  }
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)

 rec := httptest.NewRecorder()
 gctx := &core.GatewayContext{
  Ctx:            context.Background(),
  RequestType:    core.RequestTypeChatCompletion,
  RawBody:        []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"stream":true}`),
  IsStream:       true,
  ResponseWriter: rec,
  StartTime:      time.Now().Add(-100 * time.Millisecond),
 }

 err := p.Invoke(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if gctx.PromptTokens != 10 {
  t.Errorf("expected prompt_tokens=10, got %d", gctx.PromptTokens)
 }
 if gctx.CompletionTokens != 5 {
  t.Errorf("expected completion_tokens=5, got %d", gctx.CompletionTokens)
 }
 if gctx.TTFT <= 0 {
  t.Error("expected TTFT > 0")
 }
}

func TestAnthropicProvider_UpstreamError(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  w.WriteHeader(http.StatusBadRequest)
  w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"bad request"}}`))
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
 gctx := &core.GatewayContext{
  Ctx:         context.Background(),
  RequestType: core.RequestTypeChatCompletion,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[]}`),
  IsStream:    false,
 }

 err := p.Invoke(gctx)
 if err == nil {
  t.Fatal("expected error for upstream 400")
 }
}

func TestAnthropicProvider_RequestTypes(t *testing.T) {
 p := NewAnthropicProvider("anthropic", "", "", nil)
 caps := p.RequestTypes()
 if len(caps) != 1 || caps[0] != core.RequestTypeChatCompletion {
  t.Errorf("expected [chat_completion], got %v", caps)
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

// TestAnthropicProvider_ConvertRequest verifies OpenAI → Anthropic request conversion
func TestAnthropicProvider_ConvertRequest(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  var body map[string]interface{}
  json.NewDecoder(r.Body).Decode(&body)

  // Anthropic format should have "messages" as array and no "model" wrapper
  if _, ok := body["model"]; !ok {
   t.Error("expected model field")
  }
  if _, ok := body["messages"]; !ok {
   t.Error("expected messages field")
  }
  if _, ok := body["max_tokens"]; !ok {
   t.Error("expected max_tokens field (required by Anthropic)")
  }

  w.Header().Set("Content-Type", "application/json")
  json.NewEncoder(w).Encode(map[string]interface{}{
   "id":      "msg_test",
   "type":    "message",
   "role":    "assistant",
   "content": []map[string]interface{}{{"type": "text", "text": "ok"}},
   "usage":   map[string]interface{}{"input_tokens": 1, "output_tokens": 1},
  })
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
 gctx := &core.GatewayContext{
  Ctx:         context.Background(),
  RequestType: core.RequestTypeChatCompletion,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"test"}],"max_tokens":100}`),
  IsStream:    false,
 }

 err := p.Invoke(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/llm/providers/ -run TestAnthropic -v`
Expected: compilation error — `NewAnthropicProvider` not defined.

- [ ] **Step 3: Implement AnthropicProvider**

```go
// pkg/llm/providers/anthropic.go
package providers

import (
 "bufio"
 "bytes"
 "context"
 "encoding/json"
 "fmt"
 "io"
 "net/http"
 "strings"
 "time"

 "tokenlive-gateway/pkg/core"
)

// AnthropicProvider implements core.Provider for the Anthropic Messages API.
// It converts between OpenAI-compatible request/response format and Anthropic native format.
type AnthropicProvider struct {
 name    string
 baseURL string
 apiKey  string
 client  *http.Client
}

// NewAnthropicProvider creates an Anthropic provider.
func NewAnthropicProvider(name, baseURL, apiKey string, _ []string) *AnthropicProvider {
 return &AnthropicProvider{
  name:    name,
  baseURL: strings.TrimRight(baseURL, "/"),
  apiKey:  apiKey,
  client:  &http.Client{Timeout: defaultTimeout},
 }
}

func (p *AnthropicProvider) Name() string               { return p.name }
func (p *AnthropicProvider) Type() core.ProviderType     { return core.ProviderAnthropic }
func (p *AnthropicProvider) ValidateConfig() error       { return nil }

func (p *AnthropicProvider) RequestTypes() []core.RequestType {
 return []core.RequestType{core.RequestTypeChatCompletion}
}

func (p *AnthropicProvider) HealthCheck(ctx context.Context) error {
 req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/messages",
  strings.NewReader(`{"model":"claude-sonnet-4-20250514","max_tokens":1,"messages":[{"role":"user","content":"ping"}`))
 if err != nil {
  return err
 }
 req.Header.Set("Content-Type", "application/json")
 req.Header.Set("x-api-key", p.apiKey)
 req.Header.Set("anthropic-version", "2023-06-01")

 resp, err := p.client.Do(req)
 if err != nil {
  return err
 }
 resp.Body.Close()

 if resp.StatusCode >= 500 {
  return fmt.Errorf("health check failed: status %d", resp.StatusCode)
 }
 return nil
}

func (p *AnthropicProvider) Invoke(gctx *core.GatewayContext) error {
 if gctx.RequestType != core.RequestTypeChatCompletion {
  return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
 }

 endpoint := p.baseURL + "/v1/messages"
 req, err := http.NewRequestWithContext(gctx.Ctx, http.MethodPost, endpoint, bytes.NewReader(gctx.RawBody))
 if err != nil {
  return fmt.Errorf("create request: %w", err)
 }
 req.Header.Set("Content-Type", "application/json")
 req.Header.Set("x-api-key", p.apiKey)
 req.Header.Set("anthropic-version", "2023-06-01")

 resp, err := p.client.Do(req)
 if err != nil {
  return fmt.Errorf("upstream request: %w", err)
 }
 defer resp.Body.Close()
 gctx.UpstreamResponse = resp

 if resp.StatusCode >= 400 {
  body, _ := io.ReadAll(resp.Body)
  return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
 }

 if gctx.IsStream {
  return p.handleStream(gctx, resp)
 }
 return p.handleNonStream(gctx, resp)
}

// handleNonStream reads Anthropic response and converts to OpenAI format.
func (p *AnthropicProvider) handleNonStream(gctx *core.GatewayContext, resp *http.Response) error {
 body, err := io.ReadAll(resp.Body)
 if err != nil {
  return fmt.Errorf("read response: %w", err)
 }
 gctx.UpstreamBody = body

 var anthropicResp map[string]interface{}
 if err := json.Unmarshal(body, &anthropicResp); err != nil {
  return fmt.Errorf("parse response: %w", err)
 }

 // Convert Anthropic response to OpenAI format
 openaiResp := p.convertToOpenAIResponse(anthropicResp)
 gctx.Response = openaiResp

 // Extract tokens
 if usage, ok := anthropicResp["usage"].(map[string]interface{}); ok {
  if it, ok := usage["input_tokens"].(float64); ok {
   gctx.PromptTokens = int(it)
  }
  if ot, ok := usage["output_tokens"].(float64); ok {
   gctx.CompletionTokens = int(ot)
  }
 }
 return nil
}

// convertToOpenAIResponse converts an Anthropic Messages response to OpenAI chat completion format.
func (p *AnthropicProvider) convertToOpenAIResponse(anthropic map[string]interface{}) map[string]interface{} {
 id, _ := anthropic["id"].(string)

 // Extract text content
 var content string
 if contentArr, ok := anthropic["content"].([]interface{}); ok {
  for _, block := range contentArr {
   if b, ok := block.(map[string]interface{}); ok {
    if b["type"] == "text" {
     if t, ok := b["text"].(string); ok {
      content += t
     }
    }
   }
  }
 }

 choice := map[string]interface{}{
  "index": 0,
  "message": map[string]interface{}{
   "role":    "assistant",
   "content": content,
  },
  "finish_reason": "stop",
 }

 return map[string]interface{}{
  "id":      id,
  "object":  "chat.completion",
  "choices": []interface{}{choice},
  "usage":   p.convertUsage(anthropic),
 }
}

// convertUsage extracts usage from Anthropic format into OpenAI format.
func (p *AnthropicProvider) convertUsage(anthropic map[string]interface{}) map[string]interface{} {
 usage := map[string]interface{}{}
 if u, ok := anthropic["usage"].(map[string]interface{}); ok {
  if it, ok := u["input_tokens"].(float64); ok {
   usage["prompt_tokens"] = int(it)
  }
  if ot, ok := u["output_tokens"].(float64); ok {
   usage["completion_tokens"] = int(ot)
  }
 }
 return usage
}

// handleStream reads Anthropic SSE events and converts them to OpenAI SSE format.
func (p *AnthropicProvider) handleStream(gctx *core.GatewayContext, resp *http.Response) error {
 // Set SSE headers
 gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
 gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
 gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
 gctx.ResponseWriter.WriteHeader(http.StatusOK)

 var (
  firstByte  bool
  inputTokens  int
  outputTokens int
 )

 scanner := bufio.NewScanner(resp.Body)
 for scanner.Scan() {
  line := scanner.Text()

  // Anthropic SSE uses "event: xxx\ndata: xxx" pairs
  if !strings.HasPrefix(line, "data: ") {
   continue
  }
  data := strings.TrimPrefix(line, "data: ")

  var event map[string]interface{}
  if err := json.Unmarshal([]byte(data), &event); err != nil {
   continue
  }

  eventType, _ := event["type"].(string)

  // Record TTFT on first content
  if !firstByte && eventType == "content_block_delta" {
   firstByte = true
   gctx.TTFT = time.Since(gctx.StartTime)
  }

  // Convert to OpenAI SSE format and write
  openaiEvent := p.convertStreamEvent(eventType, event)
  if openaiEvent != nil {
   out, _ := json.Marshal(openaiEvent)
   fmt.Fprintf(gctx.ResponseWriter, "data: %s\n\n", string(out))
   if f, ok := gctx.ResponseWriter.(http.Flusher); ok {
    f.Flush()
   }
  }

  // Extract tokens from usage events
  if eventType == "message_start" {
   if msg, ok := event["message"].(map[string]interface{}); ok {
    if u, ok := msg["usage"].(map[string]interface{}); ok {
     if it, ok := u["input_tokens"].(float64); ok {
      inputTokens = int(it)
     }
    }
   }
  }
  if eventType == "message_delta" {
   if u, ok := event["usage"].(map[string]interface{}); ok {
    if ot, ok := u["output_tokens"].(float64); ok {
     outputTokens = int(ot)
    }
   }
  }
 }

 // Write [DONE] sentinel
 fmt.Fprintf(gctx.ResponseWriter, "data: [DONE]\n\n")
 if f, ok := gctx.ResponseWriter.(http.Flusher); ok {
  f.Flush()
 }

 gctx.PromptTokens = inputTokens
 gctx.CompletionTokens = outputTokens
 return scanner.Err()
}

// convertStreamEvent converts an Anthropic SSE event to OpenAI streaming chunk format.
func (p *AnthropicProvider) convertStreamEvent(eventType string, event map[string]interface{}) map[string]interface{} {
 switch eventType {
 case "content_block_delta":
  delta, _ := event["delta"].(map[string]interface{})
  text, _ := delta["text"].(string)
  return map[string]interface{}{
   "choices": []interface{}{
    map[string]interface{}{
     "delta": map[string]interface{}{
      "content": text,
     },
    },
   },
  }
 case "message_delta":
  return map[string]interface{}{
   "choices": []interface{}{
    map[string]interface{}{
     "delta":         map[string]interface{}{},
     "finish_reason": "stop",
    },
   },
  }
 default:
  // message_start, content_block_start, content_block_stop, message_stop → no OpenAI equivalent
  return nil
 }
}

// Compile-time check
var _ core.Provider = (*AnthropicProvider)(nil)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/llm/providers/ -run TestAnthropic -v`
Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/llm/providers/anthropic.go pkg/llm/providers/anthropic_test.go
git commit -m "feat(providers): add Anthropic provider with Messages API conversion and stream support"
```

---

### Task 5: Provider Factory

**Files:**

- Create: `pkg/llm/factory.go`
- Create: `pkg/llm/factory_test.go`

- [ ] **Step 1: Write failing tests for provider factory**

```go
// pkg/llm/factory_test.go
package llm

import (
 "testing"

 "tokenlive-gateway/pkg/core"
 "tokenlive-gateway/pkg/llm/providers"
)

func TestNewProvider_OpenAI(t *testing.T) {
 p, err := NewProvider("openai", ProviderConfig{
  Name:   "openai",
  BaseURL: "https://api.openai.com/v1",
  APIKey: "sk-test",
 })
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if p.Name() != "openai" {
  t.Errorf("expected name 'openai', got '%s'", p.Name())
 }
 if p.Type() != core.ProviderOpenAI {
  t.Errorf("expected type openai, got %s", p.Type())
 }
}

func TestNewProvider_Anthropic(t *testing.T) {
 p, err := NewProvider("anthropic", ProviderConfig{
  Name:   "anthropic",
  BaseURL: "https://api.anthropic.com",
  APIKey: "sk-ant-test",
 })
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if p.Type() != core.ProviderAnthropic {
  t.Errorf("expected type anthropic, got %s", p.Type())
 }
}

func TestNewProvider_UnknownType(t *testing.T) {
 _, err := NewProvider("unknown", ProviderConfig{
  Name:   "unknown",
  BaseURL: "http://localhost",
  APIKey: "test",
 })
 if err == nil {
  t.Fatal("expected error for unknown provider type")
 }
}

func TestNewProvider_EmptyBaseURL(t *testing.T) {
 _, err := NewProvider("openai", ProviderConfig{
  Name:   "openai",
  BaseURL: "",
  APIKey: "sk-test",
 })
 if err == nil {
  t.Fatal("expected error for empty base URL")
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/llm/ -run TestNewProvider -v`
Expected: compilation error — `NewProvider` and `ProviderConfig` not defined.

- [ ] **Step 3: Implement provider factory**

```go
// pkg/llm/factory.go
package llm

import (
 "fmt"

 "tokenlive-gateway/pkg/core"
 "tokenlive-gateway/pkg/llm/providers"
)

// ProviderConfig holds provider creation parameters.
type ProviderConfig struct {
 Name    string
 BaseURL string
 APIKey  string
 Models  []string
}

// NewProvider creates a core.Provider by type name.
func NewProvider(providerType string, cfg ProviderConfig) (core.Provider, error) {
 if cfg.BaseURL == "" {
  return nil, fmt.Errorf("provider %q: base_url is required", cfg.Name)
 }

 switch core.ProviderType(providerType) {
 case core.ProviderOpenAI:
  return providers.NewOpenAIProvider(cfg.Name, cfg.BaseURL, cfg.APIKey, cfg.Models), nil
 case core.ProviderAnthropic:
  return providers.NewAnthropicProvider(cfg.Name, cfg.BaseURL, cfg.APIKey, cfg.Models), nil
 default:
  return nil, fmt.Errorf("unsupported provider type: %s", providerType)
 }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/llm/ -run TestNewProvider -v`
Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/llm/factory.go pkg/llm/factory_test.go
git commit -m "feat(llm): add provider factory with OpenAI and Anthropic registration"
```

---

### Task 6: ConfigWatcher LoadConfig

**Files:**

- Modify: `pkg/core/config_watcher.go:145-149`

- [ ] **Step 1: Write failing test for LoadConfig**

Add to `pkg/core/config_watcher_test.go` (create if needed):

```go
package core

import (
 "os"
 "path/filepath"
 "testing"
)

func TestLoadConfig_ValidYAML(t *testing.T) {
 dir := t.TempDir()
 configPath := filepath.Join(dir, "test.yml")

 yamlContent := `
pipelines:
  default:
    name: default
    request_types: [chat_completion]
    inbound_filters: [auth, rate_limit, validate]
    outbound_filters: [token_settlement, metrics, access_log]
    invoker:
      type: cluster
      retry:
        max_retries: 2
        backoff:
          type: exponential_jitter
          base_ms: 200
          max_ms: 5000
`
 if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
  t.Fatal(err)
 }

 config, err := LoadConfig(configPath)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if config == nil {
  t.Fatal("expected non-nil config")
 }
 if _, ok := config.Pipelines["default"]; !ok {
  t.Error("expected 'default' pipeline")
 }
 if config.Pipelines["default"].Invoker.Retry.MaxRetries != 2 {
  t.Errorf("expected max_retries=2, got %d", config.Pipelines["default"].Invoker.Retry.MaxRetries)
 }
}

func TestLoadConfig_InvalidPath(t *testing.T) {
 _, err := LoadConfig("/nonexistent/path.yml")
 if err == nil {
  t.Fatal("expected error for nonexistent file")
 }
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
 dir := t.TempDir()
 configPath := filepath.Join(dir, "bad.yml")
 os.WriteFile(configPath, []byte("{{{invalid yaml"), 0644)

 _, err := LoadConfig(configPath)
 if err == nil {
  t.Fatal("expected error for invalid YAML")
 }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/core/ -run TestLoadConfig -v`
Expected: FAIL — `LoadConfig` returns error "config loading not implemented".

- [ ] **Step 3: Implement LoadConfig**

Replace lines 145-149 in `pkg/core/config_watcher.go`:

```go
// LoadConfig 从 YAML 文件加载 EngineConfig
func LoadConfig(path string) (*EngineConfig, error) {
 v := viper.New()
 v.SetConfigFile(path)
 v.SetConfigType("yaml")

 if err := v.ReadInConfig(); err != nil {
  return nil, fmt.Errorf("read config file: %w", err)
 }

 config := &EngineConfig{}
 if err := v.Unmarshal(config); err != nil {
  return nil, fmt.Errorf("unmarshal config: %w", err)
 }

 return config, nil
}
```

Also add `"github.com/spf13/viper"` to the import block in `config_watcher.go`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/core/ -run TestLoadConfig -v`
Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/core/config_watcher.go pkg/core/config_watcher_test.go
git commit -m "feat(core): implement LoadConfig with Viper YAML deserialization for hot-reload"
```

---

### Task 7: Wire Providers into Engine

**Files:**

- Modify: `cmd/server/wire/provider.go`

- [ ] **Step 1: Update `NewGatewayEngine` to create real providers**

In `cmd/server/wire/provider.go`, update the `NewGatewayEngine` function to create providers via the factory and attach them to endpoints. Key changes:

1. Import `"tokenlive-gateway/pkg/llm"` (the factory package)
2. After building `gwDiscovery`, create providers and attach them to endpoints

```go
// In NewGatewayEngine, after building gwDiscovery, add:

// Create providers and attach to endpoints
providers, err := createProviders(v)
if err != nil {
    return nil, fmt.Errorf("create providers: %w", err)
}

// Attach providers to endpoints in discovery
attachProvidersToDiscovery(staticDiscovery, providers)
```

Add helper functions:

```go
// createProviders reads provider configs from Viper and creates core.Provider instances
func createProviders(v *viper.Viper) (map[string]core.Provider, error) {
    providers := make(map[string]core.Provider)

    type modelMapping struct {
        Provider string `mapstructure:"provider"`
        APIKey   string `mapstructure:"api_key"`
        APIBase  string `mapstructure:"api_base"`
    }

    var models []modelMapping
    if err := v.UnmarshalKey("llm.model_list", &models); err != nil {
        return nil, err
    }

    // Deduplicate by provider name, pick first API key and base
    seen := make(map[string]bool)
    for _, m := range models {
        if seen[m.Provider] {
            continue
        }
        seen[m.Provider] = true

        apiBase := m.APIBase
        if apiBase == "" {
            if base, ok := defaultAPIBase[m.Provider]; ok {
                apiBase = base
            }
        }
        if apiBase == "" {
            continue
        }

        p, err := llm.NewProvider(m.Provider, llm.ProviderConfig{
            Name:    m.Provider,
            BaseURL: apiBase,
            APIKey:  m.APIKey,
        })
        if err != nil {
            // Log warning but don't fail startup for unconfigured providers
            continue
        }
        providers[m.Provider] = p
    }

    return providers, nil
}

// attachProvidersToDiscovery sets ProviderImpl on endpoints that have a matching provider
func attachProvidersToDiscovery(sd *discovery.StaticDiscovery, providers map[string]core.Provider) {
    // This requires access to the registered instances
    // The StaticDiscovery already has them registered; we need to update their metadata
    // For now, the DiscoveryAdapter will look up providers during List()
    // The actual wiring happens through Endpoint.ProviderImpl in the adapter
}
```

- [ ] **Step 2: Update DiscoveryAdapter to set ProviderImpl**

In `pkg/core/discovery.go`, update `ServiceInstanceToEndpoint` to accept a provider lookup:

The current `ServiceInstanceToEndpoint` doesn't set `ProviderImpl`. We need to update the `DiscoveryAdapter.List` to set `Endpoint.ProviderImpl` from a providers map.

Add a `providers` map field to `DiscoveryAdapter`:

```go
type DiscoveryAdapter struct {
    inner     discovery.ServiceDiscovery
    providers *providerModelSet
    impls     map[string]Provider // provider name -> implementation
}
```

Update constructor:

```go
func NewDiscoveryAdapter(inner discovery.ServiceDiscovery, providerConfigs []ProviderConfig, impls map[string]Provider) *DiscoveryAdapter {
    return &DiscoveryAdapter{
        inner:     inner,
        providers: newProviderModelSet(providerConfigs),
        impls:     impls,
    }
}
```

Update `List` to set `ProviderImpl`:

```go
// In the goroutine that converts instances:
ep := ServiceInstanceToEndpoint(inst, pc)
ep.Model = model
if impl, ok := a.impls[pc.Name]; ok {
    ep.ProviderImpl = impl
}
eps = append(eps, ep)
```

- [ ] **Step 3: Update wire/provider.go to pass provider implementations**

```go
// In NewGatewayEngine:
providerImpls := make(map[string]core.Provider)
for name, p := range providers {
    providerImpls[name] = p
}

gwDiscovery := core.NewDiscoveryAdapter(staticDiscovery, providerConfigs, providerImpls)
```

- [ ] **Step 4: Verify compilation**

Run: `go build ./cmd/server/`
Expected: compilation succeeds.

- [ ] **Step 5: Run all tests**

Run: `go test ./pkg/llm/... ./pkg/core/... -v`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/server/wire/provider.go pkg/core/discovery.go
git commit -m "feat(wire): connect OpenAI/Anthropic providers to Engine via factory and discovery"
```

---

### Task 8: Update CLAUDE.md

**Files:**

- Modify: `CLAUDE.md`

- [ ] **Step 1: Update the architecture section to reflect providers are implemented**

In CLAUDE.md, under "### LLM Core Library (`pkg/llm/`)", update:

- Note that `providers/` now contains OpenAI and Anthropic implementations
- Add `sse_parser.go` and `sse_intercept_writer.go` to the file listing
- Note `factory.go` as the provider creation entry point
