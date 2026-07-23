package llm

import (
	"context"
	"errors"
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
	// Do not WriteHeader before the first upstream body byte. Calling WriteHeader
	// early marks TTFT incorrectly and can make Claude Code think the stream has
	// started while xAI is still computing the first event on large contexts.

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
			return fmt.Errorf("read upstream stream: %w", classifyStreamReadError(gctx, err))
		}
	}
	return nil
}

// classifyStreamReadError distinguishes client disconnect from gateway/upstream cancels.
func classifyStreamReadError(gctx *core.GatewayContext, err error) error {
	if err == nil {
		return nil
	}
	if gctx != nil && gctx.Request != nil {
		if cerr := gctx.Request.Context().Err(); cerr != nil {
			return fmt.Errorf("client disconnected: %w", err)
		}
	}
	if gctx != nil && gctx.Ctx != nil {
		if cause := context.Cause(gctx.Ctx); cause != nil && !errors.Is(cause, context.Canceled) {
			return cause
		}
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("context canceled: %w", err)
	}
	return err
}
