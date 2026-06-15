package core

import (
	"fmt"
	"net/http"
)

// ErrorFormatter 把 (httpCode, error) 序列化为对应协议簇的错误响应体。
type ErrorFormatter interface {
	Format(code int, err error) map[string]interface{}
	FormatSSE(code int, err error) string
}

// ErrorFormatterForRequestType 根据 RequestType 返回对应协议簇的错误格式器。
func ErrorFormatterForRequestType(rt RequestType) ErrorFormatter {
	switch rt {
	case RequestTypeMessages:
		return anthropicErrorFormatter{}
	default:
		return openaiErrorFormatter{}
	}
}

// ===== OpenAI 风格(默认,保持向后兼容) =====

type openaiErrorFormatter struct{}

func (openaiErrorFormatter) Format(code int, err error) map[string]interface{} {
	return map[string]interface{}{
		"error": map[string]interface{}{
			"message": err.Error(),
			"type":    "gateway_error",
			"code":    code,
		},
	}
}

func (openaiErrorFormatter) FormatSSE(code int, err error) string {
	return fmt.Sprintf("data: {\"error\": {\"message\": %q, \"type\": \"upstream_error\"}}\n\n", err.Error())
}

// ===== Anthropic 原生 =====

type anthropicErrorFormatter struct{}

func (anthropicErrorFormatter) Format(code int, err error) map[string]interface{} {
	return map[string]interface{}{
		"type": "error",
		"error": map[string]interface{}{
			"type":    anthropicErrorType(code),
			"message": err.Error(),
		},
	}
}

func (anthropicErrorFormatter) FormatSSE(code int, err error) string {
	return fmt.Sprintf("event: error\ndata: {\"type\": \"error\", \"error\": {\"type\": \"upstream_error\", \"message\": %q}}\n\n", err.Error())
}

func anthropicErrorType(code int) string {
	switch code {
	case http.StatusBadRequest:
		return "invalid_request_error"
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusForbidden:
		return "permission_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusRequestEntityTooLarge:
		return "request_too_large"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	case 529:
		return "overloaded_error"
	default:
		if code >= 500 {
			return "api_error"
		}
		return "invalid_request_error"
	}
}
