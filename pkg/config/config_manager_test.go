package config

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestYAMLConfig() *GatewayConfig {
	return &GatewayConfig{
		Models: map[string]ModelConfig{
			"gpt-4": {
				RequestTypes: []string{"chat_completion"},
				Endpoints: []EndpointConfig{
					{Provider: "openai", URL: "https://api.openai.com/v1", Priority: 1},
				},
			},
			"claude-sonnet": {
				RequestTypes: []string{"chat_completion"},
				Endpoints: []EndpointConfig{
					{Provider: "anthropic", URL: "https://api.anthropic.com", Priority: 1},
				},
			},
		},
		Providers: map[string]ProviderConfig{
			"openai":    {Protocol: "openai", APIKey: "sk-yaml"},
			"anthropic": {Protocol: "anthropic", APIKey: "sk-ant"},
		},
		Fallbacks: map[string][]string{
			"gpt-4": {"claude-sonnet"},
		},
	}
}

func TestConfigManager_YAMLOnly(t *testing.T) {
	yamlCfg := newTestYAMLConfig()
	mgr := NewConfigManager(yamlCfg, nil, zap.NewNop())

	endpoints := mgr.GetEndpoints(context.Background(), "gpt-4")
	assert.Len(t, endpoints, 1)
	assert.Equal(t, "openai", endpoints[0].ProviderName)
	assert.Equal(t, "sk-yaml", endpoints[0].APIKey)

	unknown := mgr.GetEndpoints(context.Background(), "nonexistent")
	assert.Nil(t, unknown)

	fb := mgr.GetFallbacks()
	assert.Equal(t, []string{"claude-sonnet"}, fb["gpt-4"])
}

func TestConfigManager_RedisOverridesYAML(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	redisEndpoints := []ResolvedEndpoint{
		{ProviderName: "redis-openai", APIKey: "sk-redis", Priority: 1},
	}
	data, err := json.Marshal(redisEndpoints)
	require.NoError(t, err)
	rdb.Set(context.Background(), "aigw:config:endpoints:gpt-4", data, 0)

	redisSrc := NewRedisConfigSource(rdb, 0, zap.NewNop())

	yamlCfg := newTestYAMLConfig()
	mgr := NewConfigManager(yamlCfg, redisSrc, zap.NewNop())

	ctx := context.Background()

	endpoints := mgr.GetEndpoints(ctx, "gpt-4")
	assert.Len(t, endpoints, 1)
	assert.Equal(t, "redis-openai", endpoints[0].ProviderName)
	assert.Equal(t, "sk-redis", endpoints[0].APIKey)

	yamlOnly := mgr.GetEndpoints(ctx, "claude-sonnet")
	assert.Len(t, yamlOnly, 1)
	assert.Equal(t, "anthropic", yamlOnly[0].ProviderName)
}

func TestConfigManager_RedisDown_FallbackToYAML(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	redisSrc := NewRedisConfigSource(rdb, 0, zap.NewNop())

	yamlCfg := newTestYAMLConfig()
	mgr := NewConfigManager(yamlCfg, redisSrc, zap.NewNop())

	ctx := context.Background()

	mr.Close()

	endpoints := mgr.GetEndpoints(ctx, "gpt-4")
	assert.Len(t, endpoints, 1)
	assert.Equal(t, "openai", endpoints[0].ProviderName)
	assert.Equal(t, "sk-yaml", endpoints[0].APIKey)
}

func TestConfigManager_AllKnownModels(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	// 在 Redis 中配置多个模型
	redisEndpoints := []ResolvedEndpoint{
		{ProviderName: "ollama", Priority: 1},
	}
	data, err := json.Marshal(redisEndpoints)
	require.NoError(t, err)

	ctx := context.Background()
	// 设置 endpoints 配置
	rdb.Set(ctx, "aigw:config:endpoints:llama-3", data, 0)
	rdb.Set(ctx, "aigw:config:endpoints:qwen-2.5", data, 0)
	// 设置 model_versions（模拟 Admin 的同步行为）
	rdb.HSet(ctx, "aigw:config:model_versions", "llama-3", 1)
	rdb.HSet(ctx, "aigw:config:model_versions", "qwen-2.5", 1)

	redisSrc := NewRedisConfigSource(rdb, 0, zap.NewNop())

	// 无需预先调用 GetEndpoints，AllKnownModels 应该主动从 model_versions 获取
	yamlCfg := newTestYAMLConfig()
	mgr := NewConfigManager(yamlCfg, redisSrc, zap.NewNop())

	known := mgr.AllKnownModels()
	// YAML 中的模型
	assert.True(t, known["gpt-4"])
	assert.True(t, known["claude-sonnet"])
	// Redis 中的模型（从 model_versions 获取）
	assert.True(t, known["llama-3"])
	assert.True(t, known["qwen-2.5"])
	assert.False(t, known["nonexistent"])
}

func TestConfigManager_StartRedisPolling_NilSafe(t *testing.T) {
	mgr := NewConfigManager(newTestYAMLConfig(), nil, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mgr.StartRedisPolling(ctx)
}

func TestConfigManager_StartRedisPolling(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	redisSrc := NewRedisConfigSource(rdb, 100*time.Millisecond, zap.NewNop())
	mgr := NewConfigManager(newTestYAMLConfig(), redisSrc, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	mgr.StartRedisPolling(ctx)
}

func TestConfigManager_OwnerOf_YAMLHit(t *testing.T) {
	mgr := NewConfigManager(newTestYAMLConfig(), nil, zap.NewNop())
	owner := mgr.OwnerOf(context.Background(), "gpt-4")
	assert.Equal(t, "openai", owner)
}

func TestConfigManager_OwnerOf_RedisHit(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	redisEndpoints := []ResolvedEndpoint{
		{ProviderName: "anthropic", APIKey: "sk-redis", Priority: 1},
	}
	data, err := json.Marshal(redisEndpoints)
	require.NoError(t, err)
	rdb.Set(context.Background(), "aigw:config:endpoints:claude-3-opus", data, 0)

	redisSrc := NewRedisConfigSource(rdb, 0, zap.NewNop())
	mgr := NewConfigManager(newTestYAMLConfig(), redisSrc, zap.NewNop())

	owner := mgr.OwnerOf(context.Background(), "claude-3-opus")
	assert.Equal(t, "anthropic", owner)
}

func TestConfigManager_OwnerOf_Miss(t *testing.T) {
	mgr := NewConfigManager(newTestYAMLConfig(), nil, zap.NewNop())
	owner := mgr.OwnerOf(context.Background(), "non-existent-model")
	assert.Equal(t, "", owner)
}

func TestConfigManager_OwnerOf_MultiEndpoint_TakesFirst(t *testing.T) {
	cfg := &GatewayConfig{
		Models: map[string]ModelConfig{
			"shared-model": {
				RequestTypes: []string{"chat_completion"},
				Endpoints: []EndpointConfig{
					{Provider: "first-provider", URL: "https://first.example.com", Priority: 1},
					{Provider: "second-provider", URL: "https://second.example.com", Priority: 2},
				},
			},
		},
		Providers: map[string]ProviderConfig{
			"first-provider":  {Protocol: "openai", APIKey: "sk-first"},
			"second-provider": {Protocol: "openai", APIKey: "sk-second"},
		},
	}

	mgr := NewConfigManager(cfg, nil, zap.NewNop())

	endpoints := mgr.GetEndpoints(context.Background(), "shared-model")
	require.Len(t, endpoints, 2)
	require.Equal(t, "first-provider", endpoints[0].ProviderName)
	require.Equal(t, "second-provider", endpoints[1].ProviderName)

	owner := mgr.OwnerOf(context.Background(), "shared-model")
	assert.Equal(t, "first-provider", owner)
}
