package middleware

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// LoggingConfig 日志配置
type LoggingConfig struct {
	Logger      *zap.Logger
	SkipPaths   []string // 跳过日志的路径
	EnableBody  bool     // 是否记录请求体
	MaxBodySize int      // 最大记录的请求体大小
}

// NewLoggingMiddleware 创建日志中间件
func NewLoggingMiddleware(config *LoggingConfig) gin.HandlerFunc {
	if config.MaxBodySize == 0 {
		config.MaxBodySize = 4096
	}

	skipPaths := make(map[string]bool)
	for _, path := range config.SkipPaths {
		skipPaths[path] = true
	}

	return func(c *gin.Context) {
		// 跳过指定路径
		if skipPaths[c.Request.URL.Path] {
			c.Next()
			return
		}

		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		// 读取请求体（如果启用）
		var requestBody string
		if config.EnableBody && c.Request.Body != nil {
			bodyBytes, _ := io.ReadAll(c.Request.Body)
			if len(bodyBytes) > config.MaxBodySize {
				requestBody = string(bodyBytes[:config.MaxBodySize]) + "... (truncated)"
			} else {
				requestBody = string(bodyBytes)
			}
			// 恢复请求体
			c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		// 处理请求
		c.Next()

		// 计算延迟
		latency := time.Since(start)
		statusCode := c.Writer.Status()

		// 构建日志字段
		fields := []zap.Field{
			zap.Int("status", statusCode),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.Duration("latency", latency),
			zap.String("user_agent", c.Request.UserAgent()),
			zap.Any("headers", maskHeader(c.Request.Header)),
		}

		// 添加 Trace ID
		if traceID := c.Writer.Header().Get("X-Trace-Id"); traceID != "" {
			fields = append(fields, zap.String("trace", traceID))
		}

		// 添加 API Key（如果存在）
		if apiKey, exists := c.Get("api_key"); exists {
			fields = append(fields, zap.String("api_key", maskAPIKey(apiKey.(string))))
		}

		// 添加请求体（如果启用）
		if config.EnableBody && requestBody != "" {
			fields = append(fields, zap.String("request_body", requestBody))
		}

		// 添加错误信息（如果存在）
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("errors", c.Errors.String()))
		}

		// 根据状态码选择日志级别
		if statusCode >= 500 {
			config.Logger.Error("Request error", fields...)
		} else if statusCode >= 400 {
			config.Logger.Warn("Request warning", fields...)
		} else {
			config.Logger.Info("Request completed", fields...)
		}
	}
}

// maskHeader 过滤并对敏感 Header 字段脱敏
func maskHeader(header http.Header) map[string][]string {
	masked := make(map[string][]string)
	for k, v := range header {
		lowerKey := strings.ToLower(k)
		if strings.Contains(lowerKey, "authorization") ||
			strings.Contains(lowerKey, "key") ||
			strings.Contains(lowerKey, "token") ||
			strings.Contains(lowerKey, "secret") {
			maskedVals := make([]string, len(v))
			for i, val := range v {
				maskedVals[i] = maskAPIKey(val)
			}
			masked[k] = maskedVals
		} else {
			masked[k] = v
		}
	}
	return masked
}
