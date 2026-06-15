package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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
