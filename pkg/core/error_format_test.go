package core

import (
	"errors"
	"net/http"
	"testing"
)

func TestAnthropicErrorType(t *testing.T) {
	tests := []struct {
		code int
		want string
	}{
		{http.StatusBadRequest, "invalid_request_error"},
		{http.StatusUnauthorized, "authentication_error"},
		{http.StatusForbidden, "permission_error"},
		{http.StatusNotFound, "not_found_error"},
		{http.StatusRequestEntityTooLarge, "request_too_large"},
		{http.StatusTooManyRequests, "rate_limit_error"},
		{529, "overloaded_error"},
		{http.StatusServiceUnavailable, "api_error"},
		{http.StatusInternalServerError, "api_error"},
		{http.StatusBadGateway, "api_error"},
		{418, "invalid_request_error"},
		{600, "api_error"},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.code), func(t *testing.T) {
			got := anthropicErrorType(tt.code)
			if got != tt.want {
				t.Errorf("anthropicErrorType(%d) = %q, want %q", tt.code, got, tt.want)
			}
		})
	}
}

func TestErrorFormatterForRequestType_Messages(t *testing.T) {
	f := ErrorFormatterForRequestType(RequestTypeMessages)
	if _, ok := f.(anthropicErrorFormatter); !ok {
		t.Errorf("expected anthropicErrorFormatter, got %T", f)
	}
	result := f.Format(http.StatusBadRequest, errors.New("bad request"))
	topType, ok := result["type"].(string)
	if !ok || topType != "error" {
		t.Errorf("top-level type = %q, want \"error\"", topType)
	}
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing error object")
	}
	if errObj["type"] != "invalid_request_error" {
		t.Errorf("error.type = %q, want invalid_request_error", errObj["type"])
	}
	if errObj["message"] != "bad request" {
		t.Errorf("error.message = %q, want \"bad request\"", errObj["message"])
	}

	// 校验 FormatSSE 的输出
	sseResult := f.FormatSSE(http.StatusTooManyRequests, errors.New("rate limited"))
	expectedSSE := "event: error\ndata: {\"type\": \"error\", \"error\": {\"type\": \"upstream_error\", \"message\": \"rate limited\"}}\n\n"
	if sseResult != expectedSSE {
		t.Errorf("FormatSSE = %q, want %q", sseResult, expectedSSE)
	}
}

func TestErrorFormatterForRequestType_Default(t *testing.T) {
	f := ErrorFormatterForRequestType(RequestTypeChatCompletion)
	if _, ok := f.(openaiErrorFormatter); !ok {
		t.Errorf("expected openaiErrorFormatter, got %T", f)
	}
	result := f.Format(http.StatusInternalServerError, errors.New("fail"))
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing error object")
	}
	if errObj["type"] != "gateway_error" {
		t.Errorf("error.type = %q, want gateway_error", errObj["type"])
	}
	if errObj["code"] != http.StatusInternalServerError {
		t.Errorf("error.code = %v, want 500", errObj["code"])
	}

	// 校验 FormatSSE 的输出
	sseResult := f.FormatSSE(http.StatusInternalServerError, errors.New("fail"))
	expectedSSE := "data: {\"error\": {\"message\": \"fail\", \"type\": \"upstream_error\"}}\n\n"
	if sseResult != expectedSSE {
		t.Errorf("FormatSSE = %q, want %q", sseResult, expectedSSE)
	}
}

func TestErrorFormatterForRequestType_NilRequestType(t *testing.T) {
	f := ErrorFormatterForRequestType(RequestType("unknown"))
	result := f.Format(404, errors.New("not found"))
	if _, ok := result["error"].(map[string]interface{}); !ok {
		t.Error("expected openai format for unknown RequestType")
	}
}
