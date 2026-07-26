package llm

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// SSEOption configures SSEInterceptWriter.
type SSEOption func(*SSEInterceptWriter)

// WithTokenExtractor sets a custom token extractor.
// Defaults to OpenAI format extraction.
func WithTokenExtractor(te TokenExtractor) SSEOption {
	return func(w *SSEInterceptWriter) {
		w.tokenExtractor = te
	}
}

// SSEInterceptWriter transparently wraps http.ResponseWriter.
// Records TTFT (time to first byte), feeds all bytes into SSEParser,
// and fills gctx token counts when usage data arrives.
type SSEInterceptWriter struct {
	http.ResponseWriter
	gctx           *core.GatewayContext
	parser         *SSEParser
	firstByte      bool
	tokenExtractor TokenExtractor // nil = use SSEParser default extraction
}

// NewSSEInterceptWriter creates a writer that intercepts SSE frames.
func NewSSEInterceptWriter(gctx *core.GatewayContext, opts ...SSEOption) *SSEInterceptWriter {
	w := &SSEInterceptWriter{
		ResponseWriter: gctx.ResponseWriter,
		gctx:           gctx,
		parser:         NewSSEParser(),
	}
	for _, opt := range opts {
		opt(w)
	}
	w.Header().Set("X-Accel-Buffering", "no")
	return w
}

// Write intercepts the byte stream, records TTFT, and parses SSE frames.
func (w *SSEInterceptWriter) Write(p []byte) (int, error) {
	if !w.firstByte {
		w.firstByte = true
		w.gctx.TriggerFirstByte()
	}

events := w.parser.Feed(p)
		for _, ev := range events {
			var in, out, cached, cacheCreated int
			if w.tokenExtractor != nil {
				in, out, cached, cacheCreated = w.tokenExtractor(ev.Data)
			} else {
				in, out, cached, cacheCreated = ev.InputTokens, ev.OutputTokens, ev.CachedTokens, ev.CacheCreationTokens
			}
			ApplyUsage(w.gctx, in, out, cached, cacheCreated)

			protocol := ""
			if w.gctx.SelectedEndpoint != nil {
				protocol = w.gctx.SelectedEndpoint.ProviderProtocol
			}
			w.gctx.TransmittedChars += ExtractContentLength(protocol, ev.Data)

			// Native Anthropic Messages passthrough: mark completion when message_stop is seen
			// so engine can distinguish normal EOF from premature disconnect.
			if w.gctx.RequestType == core.RequestTypeMessages && isAnthropicMessageStop(ev.Data) {
				if w.gctx.Tags == nil {
					w.gctx.Tags = make(map[string]string)
				}
				w.gctx.Tags["message_stop_sent"] = "true"
			}
		}

		return w.ResponseWriter.Write(p)
	}

// isAnthropicMessageStop reports whether SSE data is an Anthropic message_stop event payload.
func isAnthropicMessageStop(data string) bool {
	data = strings.TrimSpace(data)
	if data == "" || data == "[DONE]" {
		return false
	}
	// Fast path before JSON parse.
	if !strings.Contains(data, "message_stop") {
		return false
	}
	var ev struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return false
	}
	return ev.Type == "message_stop"
}

// Flush delegates to the underlying Flusher if supported.
func (w *SSEInterceptWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// WriteHeader intercepts status code writes; once headers are sent, TTFT is considered started.
func (w *SSEInterceptWriter) WriteHeader(statusCode int) {
	if !w.firstByte {
		w.firstByte = true
		w.gctx.TriggerFirstByte()
	}
	w.ResponseWriter.WriteHeader(statusCode)
}
