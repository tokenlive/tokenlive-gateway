package service

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"go.uber.org/zap"
)

func TestApiKeyServiceValidateAPIKeyHashKey(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	apiKey := "tl_live_service_hash"
	pepper := "api-key-pepper"
	keyHash := config.HashAPIKey(apiKey, pepper)
	mr.HSet(store.RedisKeyApiKeyHash(keyHash),
		"key_id", "ak_1",
		"user_id", "usr_1",
		"workspace_id", "wsp_1",
		"tenant", "tenant_a",
		"user_tenant", "tenant_a",
		"status", "1",
		"quota", "-1",
		"expires_at", "0",
	)

	logger := &log.Logger{Logger: zap.NewNop()}
	svc := NewApiKeyService(config.NewRedisGatewayProviderWithAPIKeyPepper(client, pepper), logger)
	info, err := svc.ValidateKey(context.Background(), apiKey)
	if err != nil {
		t.Fatalf("ValidateKey() err = %v", err)
	}
	if info.UserID != "usr_1" || info.WorkspaceID != "wsp_1" || info.Tenant != "tenant_a" || info.UserTenant != "tenant_a" {
		t.Fatalf("ValidateKey() = %+v, want APIKey runtime identity", info)
	}
	if info.KeyID != "ak_1" {
		t.Fatalf("KeyID = %q, want ak_1", info.KeyID)
	}
	if info.KeyHash != keyHash {
		t.Fatalf("KeyHash = %q, want %q", info.KeyHash, keyHash)
	}
}
