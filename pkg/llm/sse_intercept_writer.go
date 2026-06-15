package llm

import (
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// SSEOption 配置 SSEInterceptWriter 的可选参数。
type SSEOption func(*SSEInterceptWriter)

// WithTokenExtractor 设置自定义 token 提取器。
// 默认使用 OpenAI 格式提取。
func WithTokenExtractor(te TokenExtractor) SSEOption {
	return func(w *SSEInterceptWriter) {
		w.tokenExtractor = te
	}
}

// SSEInterceptWriter 透明包装 http.ResponseWriter。
// 记录 TTFT（首字节时间），将所有字节送入 SSEParser 解析，
// 当 usage 数据到达时填充 gctx 的 token 计数。
type SSEInterceptWriter struct {
	http.ResponseWriter
	gctx           *core.GatewayContext
	parser         *SSEParser
	firstByte      bool
	tokenExtractor TokenExtractor // nil = 使用 SSEParser 默认提取
}

// NewSSEInterceptWriter 创建拦截 SSE 帧的 writer。
func NewSSEInterceptWriter(gctx *core.GatewayContext, opts ...SSEOption) *SSEInterceptWriter {
	w := &SSEInterceptWriter{
		ResponseWriter: gctx.ResponseWriter,
		gctx:           gctx,
		parser:         NewSSEParser(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Write 拦截字节流，记录 TTFT，并解析 SSE 帧。
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
		if in > 0 {
			w.gctx.InputTokens = in
		}
		if out > 0 {
			w.gctx.OutputTokens = out
		}
		if cached > 0 {
			w.gctx.CachedTokens = cached
		}
		if cacheCreated > 0 {
			w.gctx.CacheCreationTokens = cacheCreated
		}

		protocol := ""
		if w.gctx.SelectedEndpoint != nil {
			protocol = w.gctx.SelectedEndpoint.ProviderProtocol
		}
		w.gctx.TransmittedChars += ExtractContentLength(protocol, ev.Data)
	}

	return w.ResponseWriter.Write(p)
}

// Flush 委托给底层 Flusher（如果支持）。
func (w *SSEInterceptWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// WriteHeader 拦截状态码写入，一旦写出即认为首包响应开始，提前置位 TTFT
func (w *SSEInterceptWriter) WriteHeader(statusCode int) {
	if !w.firstByte {
		w.firstByte = true
		w.gctx.TriggerFirstByte()
	}
	w.ResponseWriter.WriteHeader(statusCode)
}
