package llm

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/usage"
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
	custom := func(data string) usage.Observation {
		var payload struct {
			Tokens *struct {
				In  int `json:"in"`
				Out int `json:"out"`
			} `json:"tokens"`
		}
		if err := json.Unmarshal([]byte(data), &payload); err != nil || payload.Tokens == nil {
			return usage.Observation{}
		}
		return usage.OpenAI(fmt.Sprintf(`{"usage":{"prompt_tokens":%d,"completion_tokens":%d}}`, payload.Tokens.In, payload.Tokens.Out))
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

func TestSSEInterceptWriter_ValidatesBeforeObservingOrWritingBatch(t *testing.T) {
	for _, priorBatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("prior-batch=%v", priorBatch), func(t *testing.T) {
			rec := httptest.NewRecorder()
			gctx := &core.GatewayContext{
				ResponseWriter: rec, StartTime: time.Now(), RequestType: core.RequestTypeMessages,
				SelectedEndpoint: &core.Endpoint{ProviderProtocol: "anthropic"},
			}
			stopped, decoded := 0, 0
			gctx.RegisterTTFTimer(func() { stopped++ })
			wantErr := errors.New("rejected batch")
			w := NewSSEInterceptWriter(gctx,
				WithTokenExtractor(func(data string) usage.Observation {
					decoded++
					return usage.Anthropic(data)
				}),
				WithSSEBatchValidator(func(_ []byte, events []SSEEvent) error {
					for _, event := range events {
						if event.Data == "reject" {
							return wantErr
						}
					}
					return nil
				}),
			)
			accepted := "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":3}}}\n\n" +
				"data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"ok\"}}\n\n"
			var wantUsage usage.Tokens
			wantBody, wantChars, wantDecoded, wantStopped := "", 0, 0, 0
			if priorBatch {
				if _, err := w.Write([]byte(accepted)); err != nil {
					t.Fatal(err)
				}
				wantUsage = usage.Tokens{InputTokens: 7, OutputTokens: 3}
				wantBody, wantChars, wantDecoded, wantStopped = accepted, 2, 2, 1
			}
			rejected := "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"must not count\"}}\n\n" +
				"data: {\"type\":\"message_stop\",\"usage\":{\"input_tokens\":100,\"output_tokens\":20}}\n\n" +
				"data: reject\n\n"
			if n, err := w.Write([]byte(rejected)); n != 0 || !errors.Is(err, wantErr) {
				t.Fatalf("rejected write = %d, %v", n, err)
			}
			if rec.Body.String() != wantBody || gctx.Usage() != wantUsage || gctx.TransmittedChars != wantChars || decoded != wantDecoded || stopped != wantStopped {
				t.Fatalf("rejected batch changed accepted state: body=%q usage=%+v chars=%d decodes=%d timers=%d", rec.Body.String(), gctx.Usage(), gctx.TransmittedChars, decoded, stopped)
			}
			if gctx.GetTagValue("message_stop_sent") != "" || (gctx.TTFT > 0) != priorBatch {
				t.Fatalf("rejected batch marked completion/TTFT: tags=%v TTFT=%v", gctx.Tags, gctx.TTFT)
			}
		})
	}
}

func TestSSEInterceptWriter_FragmentedFramesShareValidationAndObservation(t *testing.T) {
	const first = ": keepalive\r\nevent: chunk\r\nid: opaque\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\r\n\r\n"
	const last = "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\ndata: [DONE]\n\n"
	payload := []byte(first + last)
	for _, split := range []int{1, len(first) - 3, len(first) + 7, len(payload) - 1} {
		t.Run(fmt.Sprintf("split=%d", split), func(t *testing.T) {
			rec := httptest.NewRecorder()
			g := &core.GatewayContext{ResponseWriter: rec, StartTime: time.Now()}
			var validated, decoded []string
			w := NewSSEInterceptWriter(g,
				WithSSEBatchValidator(func(_ []byte, events []SSEEvent) error {
					for _, event := range events {
						validated = append(validated, event.Data)
					}
					return nil
				}),
				WithTokenExtractor(func(data string) usage.Observation {
					decoded = append(decoded, data)
					return usage.OpenAI(data)
				}),
			)
			for _, fragment := range [][]byte{payload[:split], payload[split:]} {
				if n, err := w.Write(fragment); n != len(fragment) || err != nil {
					t.Fatalf("write = %d, %v", n, err)
				}
			}
			if rec.Body.String() != string(payload) || g.InputTokens != 10 || g.OutputTokens != 5 || g.TransmittedChars != 6 {
				t.Fatalf("fragmented observation changed bytes or usage: body=%q usage=%+v chars=%d", rec.Body.String(), g.Usage(), g.TransmittedChars)
			}
			if len(validated) != 3 || len(decoded) != 3 {
				t.Fatalf("each event must be validated and decoded once: validated=%v decoded=%v", validated, decoded)
			}
			for i := range validated {
				if validated[i] != decoded[i] {
					t.Fatalf("validation and observation saw different events: %v / %v", validated, decoded)
				}
			}
		})
	}
}

type failingSSEWriter struct {
	*httptest.ResponseRecorder
	err error
}

func (w *failingSSEWriter) Write([]byte) (int, error) { return 0, w.err }

func TestSSEInterceptWriter_ObservesAcceptedUsageBeforeUnderlyingWrite(t *testing.T) {
	wantErr := errors.New("client write failed")
	g := &core.GatewayContext{
		ResponseWriter: &failingSSEWriter{ResponseRecorder: httptest.NewRecorder(), err: wantErr},
		StartTime:      time.Now(),
	}
	w := NewSSEInterceptWriter(g)
	payload := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n")
	if n, err := w.Write(payload); n != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("write = %d, %v", n, err)
	}
	if g.Usage() != (usage.Tokens{InputTokens: 10, OutputTokens: 5}) || g.TransmittedChars != 5 || g.TTFT <= 0 {
		t.Fatalf("accepted observation changed on write failure: usage=%+v chars=%d TTFT=%v", g.Usage(), g.TransmittedChars, g.TTFT)
	}
}
