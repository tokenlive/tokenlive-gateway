package outbound

import (
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestAccessLogFilter_RedactsAPIKey(t *testing.T) {
	coreObs, logs := observer.New(zap.InfoLevel)
	logger := zap.New(coreObs)
	f := NewAccessLogFilter(logger, nil, nil, nil, nil)

	gctx := &core.GatewayContext{
		OriginalModel: "gpt-4",
		Model:         "gpt-4",
		StartTime:     time.Now().Add(-100 * time.Millisecond),
		APIKey:        "sk-abcdefghijklmnopqrstuvwxyz12345678",
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if logs.Len() == 0 {
		t.Fatal("expected at least one log entry")
	}

	entry := logs.All()[0]
	apiKeyField := entry.ContextMap()["api_key"]
	if apiKeyField == nil {
		t.Fatal("expected api_key field in log entry")
	}

	apiKeyValue := apiKeyField.(string)
	// 不应是完整 key
	if apiKeyValue == "sk-abcdefghijklmnopqrstuvwxyz12345678" {
		t.Error("api_key should not be logged in plaintext")
	}
	// 应包含脱敏标记
	if !strings.Contains(apiKeyValue, "***") {
		t.Errorf("expected redacted key containing '***', got '%s'", apiKeyValue)
	}
	// 验证保留首尾各 4 字符
	expected := "sk-a***5678"
	if apiKeyValue != expected {
		t.Errorf("expected redacted key '%s', got '%s'", expected, apiKeyValue)
	}
}

func TestAccessLogFilterBuildsPortalIdentityFields(t *testing.T) {
	f := NewAccessLogFilter(zap.NewNop(), nil, nil, nil, nil)
	gctx := &core.GatewayContext{
		StartTime:   time.Now().Add(-50 * time.Millisecond),
		APIKey:      "tl_live_abcdefghijklmnopqrstuvwxyz123456",
		APIKeyID:    "ak_1",
		APIKeyHash:  "hash_1",
		WorkspaceID: "wsp_1",
		UserID:      "usr_1",
		Model:       "gpt-4o",
	}

	item := f.buildAccessLogItem(gctx)

	if item.WorkspaceID != "wsp_1" {
		t.Fatalf("WorkspaceID = %q, want wsp_1", item.WorkspaceID)
	}
	if item.APIKeyID != "ak_1" {
		t.Fatalf("APIKeyID = %q, want ak_1", item.APIKeyID)
	}
	if item.APIKeyHash != "hash_1" {
		t.Fatalf("APIKeyHash = %q, want hash_1", item.APIKeyHash)
	}
}

func TestRedactKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"long key", "sk-abcdefghijklmnopqrstuvwxyz123456", "sk-a***3456"},
		{"8 chars", "12345678", "***"},
		{"short key", "abc", "***"},
		{"empty", "", "***"},
		{"7 chars", "1234567", "***"},
		{"9 chars", "123456789", "1234***6789"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactKey(tt.key)
			if got != tt.want {
				t.Errorf("redactKey(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}
