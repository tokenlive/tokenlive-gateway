package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRequestLogMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	// Initialize log.Logger
	zapLogger, _ := zap.NewDevelopment()
	logger := &log.Logger{Logger: zapLogger}

	r.Use(RequestLogMiddleware(logger))
	r.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// Check X-Trace-Id header is present and is a 32-char hex string (MD5 output)
	traceID := w.Header().Get("X-Trace-Id")
	assert.NotEmpty(t, traceID)
	assert.Len(t, traceID, 32)
}

func TestRequestLogMiddleware_SkipsLLMRequestBody(t *testing.T) {
	gin.SetMode(gin.TestMode)

	core, logs := observer.New(zap.InfoLevel)
	logger := &log.Logger{Logger: zap.New(core)}

	r := gin.New()
	r.Use(RequestLogMiddleware(logger))
	r.POST("/v1/messages", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(strings.Repeat("x", 32*1024)))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	for _, entry := range logs.All() {
		if entry.Message != "Request" {
			continue
		}
		_, ok := entry.ContextMap()["request_params"]
		assert.False(t, ok, "LLM request body should not be logged by global middleware")
		return
	}
	t.Fatal("request log entry not found")
}

func TestResponseLogMiddleware_SkipsMetricsResponseBody(t *testing.T) {
	gin.SetMode(gin.TestMode)

	core, logs := observer.New(zap.InfoLevel)
	logger := &log.Logger{Logger: zap.New(core)}

	r := gin.New()
	r.Use(ResponseLogMiddleware(logger))
	r.GET("/metrics", func(c *gin.Context) {
		c.String(http.StatusOK, strings.Repeat("metric_line 1\n", 2048))
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	for _, entry := range logs.All() {
		if entry.Message != "Response" {
			continue
		}
		assert.Equal(t, "<omitted>", entry.ContextMap()["response_body"])
		return
	}
	t.Fatal("response log entry not found")
}
