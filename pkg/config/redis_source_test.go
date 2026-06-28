package config

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestRedisSource(t *testing.T) (*RedisConfigSource, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	src := NewRedisConfigSource(client, 100*time.Millisecond, zap.NewNop())
	return src, mr
}

func seedEndpoints(t *testing.T, mr *miniredis.Miniredis, modelCode string, endpoints []ResolvedEndpoint) {
	t.Helper()
	data, err := json.Marshal(endpoints)
	require.NoError(t, err)
	require.NoError(t, mr.Set(store.RedisKeyConfigEndpoints(modelCode), string(data)))
}

func TestRedisSource_GetEndpoints_CacheHit(t *testing.T) {
	src, mr := newTestRedisSource(t)
	ctx := context.Background()

	expected := []ResolvedEndpoint{
		{RealModel: "gpt-4", ProviderName: "openai", Priority: 1},
	}
	seedEndpoints(t, mr, "gpt-4", expected)

	endpoints, ok := src.GetEndpoints(ctx, "gpt-4")
	assert.True(t, ok)
	assert.Equal(t, expected, endpoints)

	mr.Del(store.RedisKeyConfigEndpoints("gpt-4"))

	// 缓存命中，即使 Redis 已删除仍能返回
	endpoints, ok = src.GetEndpoints(ctx, "gpt-4")
	assert.True(t, ok)
	assert.Equal(t, expected, endpoints)
}

func TestRedisSource_GetEndpoints_EndpointIDAndCodeAliases(t *testing.T) {
	src, mr := newTestRedisSource(t)
	ctx := context.Background()

	raw := `[{
		"endpoint_id": "endpoint-id-1",
		"endpoint_code": "endpoint-code-1",
		"real_model": "gpt-4",
		"provider_name": "openai",
		"request_types": ["chat_completion"]
	}]`
	require.NoError(t, mr.Set(store.RedisKeyConfigEndpoints("gpt-4"), raw))

	endpoints, ok := src.GetEndpoints(ctx, "gpt-4")
	require.True(t, ok)
	require.Len(t, endpoints, 1)
	assert.Equal(t, "endpoint-id-1", endpoints[0].ID)
	assert.Equal(t, "endpoint-code-1", endpoints[0].Code)
}

func TestRedisSource_GetEndpoints_CacheMiss_RedisDown(t *testing.T) {
	src, _ := newTestRedisSource(t)
	ctx := context.Background()

	endpoints, ok := src.GetEndpoints(ctx, "nonexistent")
	assert.False(t, ok)
	assert.Nil(t, endpoints)
}

func TestRedisSource_VersionPolling_ClearsCache(t *testing.T) {
	src, mr := newTestRedisSource(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expected := []ResolvedEndpoint{
		{RealModel: "gpt-4", ProviderName: "openai", Priority: 1},
	}
	// Seed endpoints first to let lazy loading fetch version
	seedEndpoints(t, mr, "gpt-4", expected)
	mr.HSet(store.RedisKeyConfigModelVersions, "gpt-4", "1")

	// Perform first load to cache gpt-4 and record version
	endpoints, ok := src.GetEndpoints(ctx, "gpt-4")
	assert.True(t, ok)
	assert.Equal(t, expected, endpoints)

	// Start polling
	go src.StartPolling(ctx)
	time.Sleep(150 * time.Millisecond)

	// Version changes on Redis
	mr.HSet(store.RedisKeyConfigModelVersions, "gpt-4", "2")
	time.Sleep(200 * time.Millisecond)

	src.mu.RLock()
	_, cached := src.cache["gpt-4"]
	src.mu.RUnlock()
	assert.False(t, cached)
}

func TestRedisSource_ClearCache(t *testing.T) {
	src, mr := newTestRedisSource(t)
	ctx := context.Background()

	expected := []ResolvedEndpoint{
		{RealModel: "gpt-4", ProviderName: "openai", Priority: 1},
	}
	seedEndpoints(t, mr, "gpt-4", expected)

	endpoints, ok := src.GetEndpoints(ctx, "gpt-4")
	assert.True(t, ok)
	assert.Equal(t, expected, endpoints)

	src.ClearCache()

	mr.Del(store.RedisKeyConfigEndpoints("gpt-4"))

	endpoints, ok = src.GetEndpoints(ctx, "gpt-4")
	assert.False(t, ok)
	assert.Nil(t, endpoints)
}
