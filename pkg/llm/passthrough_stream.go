package llm

import (
	"fmt"
	"io"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// PassthroughStream relays an upstream SSE response to the client verbatim while extracting
// token usage via extractor. It sets the standard SSE response headers, then copies the body
// in 4KB chunks through an SSEInterceptWriter (which records TTFT and token counts).
//
// It does NOT close resp.Body — the caller owns the body's lifecycle (typically a
// `defer resp.Body.Close()` that also covers the non-streaming path). Used by providers whose
// upstream protocol matches the client protocol (Anthropic, Gemini).
func PassthroughStream(gctx *core.GatewayContext, resp *http.Response, extractor TokenExtractor) error {
	writer := NewSSEInterceptWriter(gctx, WithTokenExtractor(extractor))
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)

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
