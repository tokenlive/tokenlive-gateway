package middleware

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/duke-git/lancet/v2/cryptor"
	"github.com/duke-git/lancet/v2/random"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

var _ http.Flusher = (*bodyLogWriter)(nil)

func RequestLogMiddleware(logger *log.Logger) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		uuid, err := random.UUIdV4()
		if err != nil {
			return
		}
		trace := cryptor.Md5String(uuid)
		// 仅固化全局追踪的 trace 字段
		logger.WithValue(ctx, zap.String("trace", trace))
		ctx.Header("X-Trace-Id", trace)

		// 准备一次性日志字段，不使用 logger.WithValue 固化，防止后续调用链重复打印
		fields := []zap.Field{
			zap.String("request_method", ctx.Request.Method),
			zap.String("request_url", ctx.Request.URL.String()),
			zap.Any("request_headers", maskHeader(ctx.Request.Header)), // 使用 maskHeader 过滤敏感字段
		}

		if ctx.Request.Body != nil {
			bodyBytes, _ := ctx.GetRawData()
			ctx.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // 关键点
			fields = append(fields, zap.String("request_params", string(bodyBytes)))
		}

		// 只在这里输出一次详细请求日志
		logger.WithContext(ctx).Info("Request", fields...)
		ctx.Next()
	}
}

func ResponseLogMiddleware(logger *log.Logger) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		blw := &bodyLogWriter{body: bytes.NewBufferString(""), ResponseWriter: ctx.Writer}
		ctx.Writer = blw
		startTime := time.Now()
		ctx.Next()
		duration := time.Since(startTime).String()

		respBody := ""
		contentType := ctx.Writer.Header().Get("Content-Type")
		if strings.Contains(contentType, "text/event-stream") {
			respBody = "<Stream Response>"
		} else {
			respBody = blw.body.String()
		}

		logger.WithContext(ctx).Info("Response", zap.Any("response_body", respBody), zap.Any("time", duration))
	}
}

type bodyLogWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w *bodyLogWriter) Write(b []byte) (int, error) {
	// 如果是流式响应，不写入内存以防内存泄露
	if !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") {
		w.body.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

func (w *bodyLogWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
