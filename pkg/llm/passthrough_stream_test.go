package llm

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestPassthroughStream_RelaysAndExtractsTokens(t *testing.T) {
	rec := httptest.NewRecorder()
	flusher := &mockFlusher{ResponseRecorder: rec}
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: flusher,
		StartTime:      time.Now(),
		IsStream:       true,
	}

	sse := "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":10}}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
		"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":5}}\n\n"
	resp := &http.Response{Body: io.NopCloser(strings.NewReader(sse))}

	if err := PassthroughStream(gctx, resp, AnthropicTokenExtractor); err != nil {
		t.Fatalf("PassthroughStream: %v", err)
	}

	// Body relayed verbatim.
	if got := rec.Body.String(); got != sse {
		t.Errorf("body not relayed verbatim:\n got %q\nwant %q", got, sse)
	}
	// SSE header set.
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	// Token extraction ran.
	if gctx.InputTokens != 10 || gctx.OutputTokens != 5 {
		t.Errorf("tokens = (%d, %d), want (10, 5)", gctx.InputTokens, gctx.OutputTokens)
	}
	// TTFT recorded.
	if gctx.TTFT <= 0 {
		t.Error("expected TTFT > 0")
	}
}

// PassthroughStream must not close resp.Body — the caller owns its lifecycle.
func TestPassthroughStream_DoesNotCloseBody(t *testing.T) {
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            t.Context(),
		ResponseWriter: &mockFlusher{ResponseRecorder: rec},
		StartTime:      time.Now(),
		IsStream:       true,
	}

	tracked := &closeTrackingReader{Reader: strings.NewReader("data: {}\n\n")}
	resp := &http.Response{Body: tracked}

	if err := PassthroughStream(gctx, resp, AnthropicTokenExtractor); err != nil {
		t.Fatalf("PassthroughStream: %v", err)
	}
	if tracked.closed {
		t.Error("PassthroughStream must not close resp.Body")
	}
}

type closeTrackingReader struct {
	io.Reader
	closed bool
}

func (c *closeTrackingReader) Close() error {
	c.closed = true
	return nil
}
