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
		"credits", "-1",
		"expires_at", "0",
	)
	provider := NewRedisGatewayProviderWithAPIKeyPepper(client, pepper)
	got, err := provider.GetApiKey(ctx, apiKey)
	if err != nil {
		t.Fatalf("GetApiKey() err = %v", err)
	}
	if got.UserID != "usr_1" || got.WorkspaceID != "wsp_1" || got.Tenant != "tenant_a" || got.Credits != -1 {
		t.Fatalf("GetApiKey() = %+v, want APIKey hash fields", got)
	}
	if got.KeyID != "ak_1" {
		t.Fatalf("KeyID = %q, want ak_1", got.KeyID)
	}
	if got.KeyHash != keyHash {
		t.Fatalf("KeyHash = %q, want %q", got.KeyHash, keyHash)
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
		"credits", "500",
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
		"credits", "-1",
		"expires_at", "0",
	)

	provider := NewRedisGatewayProvider(client)
	_, err := provider.GetApiKey(ctx, apiKey)
	if err == nil {
		t.Fatalf("GetApiKey() err = nil, want missing api_key_pepper error")
	}
}

func TestRedisGatewayProviderDeductCreditsUsesAPIKeyHashKey(t *testing.T) {
	client, mr := newRedisProviderAPIKeyTestClient(t)
	ctx := context.Background()
	apiKey := "tl_live_hash_quota"
	pepper := "api-key-pepper"
	keyHash := HashAPIKey(apiKey, pepper)

	mr.HSet(store.RedisKeyApiKeyHash(keyHash),
		"user_id", "usr_1",
		"workspace_id", "wsp_1",
		"status", "1",
		"credits", "500",
		"expires_at", "0",
	)

	provider := NewRedisGatewayProviderWithAPIKeyPepper(client, pepper)
	got, err := provider.DeductCredits(ctx, apiKey, 125)
	if err != nil {
		t.Fatalf("DeductCredits() err = %v", err)
	}
	if got != 375 {
		t.Fatalf("DeductCredits() = %d, want 375", got)
	}

	hashCredits := mr.HGet(store.RedisKeyApiKeyHash(keyHash), "credits")
	if hashCredits != "375" {
		t.Fatalf("hash credits = %q, want 375", hashCredits)
	}
	if mr.Exists("aigw:apikey:" + apiKey) {
		t.Fatalf("legacy plaintext redis key should not be created")
	}
}

func TestRedisGatewayProviderDeductCreditsIgnoresLegacyPlaintext(t *testing.T) {
	client, mr := newRedisProviderAPIKeyTestClient(t)
	ctx := context.Background()
	apiKey := "sk-legacy-quota"

	mr.HSet("aigw:apikey:"+apiKey,
		"user_id", "legacy_user",
		"status", "1",
		"credits", "80",
		"expires_at", "0",
	)

	provider := NewRedisGatewayProviderWithAPIKeyPepper(client, "api-key-pepper")
	if _, err := provider.DeductCredits(ctx, apiKey, 30); err == nil {
		t.Fatalf("DeductCredits() err = nil, want missing hash key error")
	}
}
