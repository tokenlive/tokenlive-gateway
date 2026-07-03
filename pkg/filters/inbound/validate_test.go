package inbound

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

type mockModelValidator func(ctx context.Context, model string, tenant string, userID string) (bool, error)

func (m mockModelValidator) ValidateModel(ctx context.Context, model string, tenant string, userID string) (bool, error) {
	return m(ctx, model, tenant, userID)
}

func TestValidateFilter_MissingMessages(t *testing.T) {
	knownModels := map[string]bool{"gpt-4": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "gpt-4",
		RequestType: core.RequestTypeChatCompletion,
		RawBody:     []byte(`{"model":"gpt-4","stream":false}`),
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for missing messages")
	}
	if getHTTPErrorCode(err) != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, getHTTPErrorCode(err))
	}
	if !strings.Contains(err.Error(), "messages") {
		t.Errorf("expected 'messages' in error, got '%s'", err.Error())
	}
}

func TestValidateFilter_EmptyMessages(t *testing.T) {
	knownModels := map[string]bool{"gpt-4": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "gpt-4",
		RequestType: core.RequestTypeChatCompletion,
		RawBody:     []byte(`{"model":"gpt-4","messages":[]}`),
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for empty messages")
	}
	if getHTTPErrorCode(err) != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, getHTTPErrorCode(err))
	}
	if !strings.Contains(err.Error(), "messages") {
		t.Errorf("expected 'messages' in error, got '%s'", err.Error())
	}
}

func TestValidateFilter_ValidChatRequest(t *testing.T) {
	knownModels := map[string]bool{"gpt-4": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "gpt-4",
		RequestType: core.RequestTypeChatCompletion,
		RawBody:     []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}`),
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error for valid chat request, got: %v", err)
	}
}

func TestValidateFilter_Messages_MissingMaxTokens(t *testing.T) {
	knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "claude-sonnet-4-20250514",
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}]}`),
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for missing max_tokens")
	}
	if getHTTPErrorCode(err) != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", getHTTPErrorCode(err))
	}
	if !strings.Contains(err.Error(), "max_tokens is required") {
		t.Errorf("expected 'max_tokens is required', got '%s'", err.Error())
	}
}

func TestValidateFilter_Messages_MaxTokensZero(t *testing.T) {
	knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "claude-sonnet-4-20250514",
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"max_tokens":0}`),
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for max_tokens=0")
	}
	if getHTTPErrorCode(err) != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", getHTTPErrorCode(err))
	}
	if !strings.Contains(err.Error(), "must be positive") {
		t.Errorf("expected 'must be positive', got '%s'", err.Error())
	}
}

func TestValidateFilter_Messages_MissingMessages(t *testing.T) {
	knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "claude-sonnet-4-20250514",
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","max_tokens":100}`),
	}

	err := f.OnRequest(gctx)
	if err == nil {
		t.Fatal("expected error for missing messages")
	}
	if getHTTPErrorCode(err) != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", getHTTPErrorCode(err))
	}
	if !strings.Contains(err.Error(), "messages") {
		t.Errorf("expected 'messages' in error, got '%s'", err.Error())
	}
}

func TestValidateFilter_Messages_Valid(t *testing.T) {
	knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "claude-sonnet-4-20250514",
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"max_tokens":100}`),
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error for valid messages request, got: %v", err)
	}
}

func TestValidateFilter_Messages_ValidWithOptional(t *testing.T) {
	knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	gctx := &core.GatewayContext{
		Model:       "claude-sonnet-4-20250514",
		RequestType: core.RequestTypeMessages,
		RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","system":"You are helpful","messages":[{"role":"user","content":"hi"}],"max_tokens":100,"stream":true,"tools":[]}`),
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestValidateFilter_GeminiGenerateContent(t *testing.T) {
	knownModels := map[string]bool{"gemini-2.5-flash": true}
	validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
		return knownModels[model], nil
	})
	f := NewValidateFilter(validator)

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "valid minimal native body",
			body: `{"contents":[{"parts":[{"text":"hi"}]}]}`,
		},
		{
			name:    "invalid JSON",
			body:    `{broken`,
			wantErr: "invalid JSON body",
		},
		{
			name:    "missing contents",
			body:    `{"generationConfig":{"temperature":0.2}}`,
			wantErr: "contents is required",
		},
		{
			name:    "empty contents",
			body:    `{"contents":[]}`,
			wantErr: "contents is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gctx := &core.GatewayContext{
				Model:       "gemini-2.5-flash",
				RequestType: core.RequestTypeGeminiGenerateContent,
				RawBody:     []byte(tt.body),
			}

			err := f.OnRequest(gctx)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if getHTTPErrorCode(err) != http.StatusBadRequest {
				t.Fatalf("expected status 400, got %d", getHTTPErrorCode(err))
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}
