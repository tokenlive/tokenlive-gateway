package config

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

func newRedisProviderAPIKeyTestClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client, mr
}

func TestRedisGatewayProviderGetApiKeyPrefersAPIKeyHashLookup(t *testing.T) {
	client, mr := newRedisProviderAPIKeyTestClient(t)
	ctx := context.Background()
	apiKey := "tl_live_hash_lookup"
	pepper := "api-key-pepper"
	keyHash := HashAPIKey(apiKey, pepper)

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
	provider := NewRedisGatewayProviderWithAPIKeyPepper(client, pepper)
	got, err := provider.GetApiKey(ctx, apiKey)
	if err != nil {
		t.Fatalf("GetApiKey() err = %v", err)
	}
	if got.UserID != "usr_1" || got.WorkspaceID != "wsp_1" || got.Tenant != "tenant_a" || got.Quota != -1 {
		t.Fatalf("GetApiKey() = %+v, want APIKey hash fields", got)
	}
}

func TestRedisGatewayProviderGetApiKeyIgnoresLegacyPlaintext(t *testing.T) {
	client, mr := newRedisProviderAPIKeyTestClient(t)
	ctx := context.Background()
	apiKey := "sk-legacy-key"

	mr.HSet("aigw:apikey:"+apiKey,
		"user_id", "legacy_user",
		"user_tenant", "legacy_tenant",
		"status", "1",
		"quota", "500",
		"expires_at", "0",
	)

	provider := NewRedisGatewayProviderWithAPIKeyPepper(client, "api-key-pepper")
	if _, err := provider.GetApiKey(ctx, apiKey); err == nil {
		t.Fatalf("GetApiKey() err = nil, want missing hash key error")
	}
}

func TestRedisGatewayProviderGetApiKeyRequiresPepper(t *testing.T) {
	client, mr := newRedisProviderAPIKeyTestClient(t)
	ctx := context.Background()
	apiKey := "tl_live_no_pepper"
	keyHash := HashAPIKey(apiKey, "api-key-pepper")

	mr.HSet(store.RedisKeyApiKeyHash(keyHash),
		"user_id", "usr_1",
		"workspace_id", "wsp_1",
		"status", "1",
		"quota", "-1",
		"expires_at", "0",
	)

	provider := NewRedisGatewayProvider(client)
	_, err := provider.GetApiKey(ctx, apiKey)
	if err == nil {
		t.Fatalf("GetApiKey() err = nil, want missing api_key_pepper error")
	}
}

func TestRedisGatewayProviderDeductQuotaUsesAPIKeyHashKey(t *testing.T) {
	client, mr := newRedisProviderAPIKeyTestClient(t)
	ctx := context.Background()
	apiKey := "tl_live_hash_quota"
	pepper := "api-key-pepper"
	keyHash := HashAPIKey(apiKey, pepper)

	mr.HSet(store.RedisKeyApiKeyHash(keyHash),
		"user_id", "usr_1",
		"workspace_id", "wsp_1",
		"status", "1",
		"quota", "500",
		"expires_at", "0",
	)

	provider := NewRedisGatewayProviderWithAPIKeyPepper(client, pepper)
	got, err := provider.DeductQuota(ctx, apiKey, 125)
	if err != nil {
		t.Fatalf("DeductQuota() err = %v", err)
	}
	if got != 375 {
		t.Fatalf("DeductQuota() = %d, want 375", got)
	}

	hashQuota := mr.HGet(store.RedisKeyApiKeyHash(keyHash), "quota")
	if hashQuota != "375" {
		t.Fatalf("hash quota = %q, want 375", hashQuota)
	}
	if mr.Exists("aigw:apikey:" + apiKey) {
		t.Fatalf("legacy plaintext redis key should not be created")
	}
}

func TestRedisGatewayProviderDeductQuotaIgnoresLegacyPlaintext(t *testing.T) {
	client, mr := newRedisProviderAPIKeyTestClient(t)
	ctx := context.Background()
	apiKey := "sk-legacy-quota"

	mr.HSet("aigw:apikey:"+apiKey,
		"user_id", "legacy_user",
		"status", "1",
		"quota", "80",
		"expires_at", "0",
	)

	provider := NewRedisGatewayProviderWithAPIKeyPepper(client, "api-key-pepper")
	if _, err := provider.DeductQuota(ctx, apiKey, 30); err == nil {
		t.Fatalf("DeductQuota() err = nil, want missing hash key error")
	}
}
