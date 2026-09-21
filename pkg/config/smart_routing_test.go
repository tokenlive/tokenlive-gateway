package config

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"go.uber.org/zap"
)

func smartConfigFixture() *GatewayConfig {
	return &GatewayConfig{
		Models: map[string]ModelConfig{
			"smart": {ModelType: "smart", RequestTypes: []string{"chat_completion"}, SmartRouting: &core.SmartRoutingConfig{
				Version: 1, JudgeModel: "judge",
				Ranges: []core.SmartRoutingRange{{Min: 0, Max: 50, Model: "small"}, {Min: 50, Max: 100, Model: "large"}},
			}},
			"judge": {RequestTypes: []string{"chat_completion"}},
			"small": {RequestTypes: []string{"chat_completion"}},
			"large": {RequestTypes: []string{"chat_completion"}},
		},
		Aliases: map[string]string{"auto": "smart"},
	}
}

func TestSmartConfigValidationAndLegacyDefaults(t *testing.T) {
	cfg := smartConfigFixture()
	require.NoError(t, Validate(cfg))
	cfg.Models["nested"] = cfg.Models["smart"]
	smart := cfg.Models["smart"]
	smart.SmartRouting.JudgeModel = "nested"
	require.Error(t, Validate(cfg))
	smart.SmartRouting.JudgeModel = "judge"
	smart.SmartRouting.Ranges[0].Model = "nested"
	require.Error(t, Validate(cfg))
	delete(cfg.Models, "nested") // Disabled dependencies remain a runtime availability decision.
	require.NoError(t, Validate(cfg))
	cfg.Models["smart"] = ModelConfig{ModelType: "smart", RequestTypes: []string{"chat_completion"}}
	require.Error(t, Validate(cfg))
	cfg.Models["smart"] = ModelConfig{ModelType: "normal", RequestTypes: []string{"chat_completion"}, SmartRouting: smart.SmartRouting}
	require.Error(t, Validate(cfg))
}

func TestSmartConfigStaticSnapshotRefresh(t *testing.T) {
	ctx := context.Background()
	cfg := smartConfigFixture()
	manager := NewConfigManager(cfg, nil, zap.NewNop())
	require.True(t, manager.AllKnownModels()["smart"])
	code, ok := manager.NormalizeModelCode("SMART")
	require.True(t, ok)
	require.Equal(t, "smart", code)
	code, ok = manager.GetAlias("AUTO")
	require.True(t, ok)
	require.Equal(t, "smart", code)
	old, err := manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Equal(t, 5000, old.JudgeTimeoutMs)
	require.Empty(t, manager.GetEndpoints(ctx, "smart"))
	cfg.Models["smart"].SmartRouting.Ranges[0].Model = "changed"
	fresh, err := manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Equal(t, "small", fresh.Ranges[0].Model)
	fresh.Ranges[0].Model = "consumer mutation"
	fresh, err = manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Equal(t, "small", fresh.Ranges[0].Model)

	next := smartConfigFixture()
	next.Models["smart"].SmartRouting.Version = 2
	next.Models["smart"].SmartRouting.Ranges[0].Max = 70
	next.Models["smart"].SmartRouting.Ranges[1].Min = 70
	manager.UpdateYAMLConfig(next) // Shared static / embedded refresh entry point.
	fresh, err = manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Equal(t, int64(2), fresh.Version)
	require.Equal(t, 50, old.Ranges[0].Max)
	next.Models["smart"] = ModelConfig{RequestTypes: []string{"chat_completion"}}
	manager.UpdateYAMLConfig(next)
	fresh, err = manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Nil(t, fresh)
	delete(next.Models, "smart")
	manager.UpdateYAMLConfig(next)
	require.False(t, manager.AllKnownModels()["smart"])
	_, ok = manager.GetAlias("auto")
	require.False(t, ok, "aliases cannot resurrect removed targets")
}

func TestSmartConfigRejectsInvalidSnapshot(t *testing.T) {
	cfg := smartConfigFixture()
	cfg.Models["smart"].SmartRouting.Ranges[0].Max = 60
	manager := NewConfigManager(cfg, nil, zap.NewNop())
	got, err := manager.GetSmartRouting(context.Background(), "smart")
	require.Error(t, err)
	require.Nil(t, got)
	require.False(t, manager.AllKnownModels()["smart"])
}

func TestSmartConfigRedisLifecycle(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	source := NewRedisConfigSource(rdb, 0, zap.NewNop())
	manager := NewConfigManager(&GatewayConfig{}, source, zap.NewNop())
	cfg := smartConfigFixture().Models["smart"].SmartRouting
	write := func(version int64) {
		cfg.Version = version
		data, err := json.Marshal(cfg)
		require.NoError(t, err)
		require.NoError(t, rdb.Set(ctx, "aigw:config:smart_routing:smart", data, 0).Err())
		require.NoError(t, rdb.HSet(ctx, "aigw:config:model_versions", "smart", version).Err())
	}
	write(1)
	require.True(t, manager.AllKnownModels()["smart"])
	old, err := manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.NotNil(t, old)
	cfg.Ranges[0].Max, cfg.Ranges[1].Min = 70, 70
	write(2)
	source.checkVersion(ctx)
	fresh, err := manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Equal(t, int64(2), fresh.Version)
	require.Equal(t, 50, old.Ranges[0].Max)
	require.NoError(t, rdb.Del(ctx, "aigw:config:smart_routing:smart").Err())
	require.NoError(t, rdb.Set(ctx, "aigw:config:endpoints:smart", `[{"provider_name":"ordinary"}]`, 0).Err())
	require.NoError(t, rdb.HSet(ctx, "aigw:config:model_versions", "smart", 3).Err())
	source.checkVersion(ctx)
	fresh, err = manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Nil(t, fresh)
	require.Len(t, manager.GetEndpoints(ctx, "smart"), 1)
	require.NoError(t, rdb.Del(ctx, "aigw:config:endpoints:smart").Err())
	require.NoError(t, rdb.HDel(ctx, "aigw:config:model_versions", "smart").Err())
	source.checkVersion(ctx)
	require.False(t, manager.AllKnownModels()["smart"])
	require.Empty(t, manager.GetEndpoints(ctx, "smart"))
}

func TestSmartConfigRedisInvalidDoesNotFallBackToOrdinary(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Set(ctx, "aigw:config:smart_routing:smart", `{"version":1,"judge_model":"judge","ranges":[]}`, 0).Err())
	require.NoError(t, rdb.HSet(ctx, "aigw:config:model_versions", "smart", 1).Err())
	manager := NewConfigManager(smartConfigFixture(), NewRedisConfigSource(rdb, 0, zap.NewNop()), zap.NewNop())
	got, err := manager.GetSmartRouting(ctx, "smart")
	require.Error(t, err)
	require.Nil(t, got)
	require.False(t, manager.AllKnownModels()["smart"])
}

func TestSmartConfigHTTPPollLifecycleAndSnapshotIsolation(t *testing.T) {
	ctx := context.Background()
	current := smartConfigFixture()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(current))
	}))
	t.Cleanup(server.Close)
	provider := NewHTTPGatewayProvider(server.URL, "", false)
	poller := NewHTTPConfigPoller(provider, 0, zap.NewNop())
	manager := NewConfigManager(&GatewayConfig{}, nil, zap.NewNop())
	update := func(ctx context.Context, cfg *GatewayConfig) error {
		manager.UpdateYAMLConfig(cfg)
		return nil
	}
	require.NoError(t, poller.pollConfig(ctx, update))
	old, err := manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.NotNil(t, old)
	snapshot, err := provider.GetConfig(ctx, "")
	require.NoError(t, err)
	snapshot.Models["smart"].SmartRouting.Ranges[0].Model = "mutated"
	snapshot, err = provider.GetConfig(ctx, "")
	require.NoError(t, err)
	require.Equal(t, "small", snapshot.Models["smart"].SmartRouting.Ranges[0].Model)
	delete(current.Models, "smart")
	require.NoError(t, poller.pollConfig(ctx, update))
	require.False(t, manager.AllKnownModels()["smart"])
	require.Equal(t, "small", old.Ranges[0].Model)
}

func TestSmartConfigRedisRevocationDoesNotResurrectStaticDefinition(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := smartConfigFixture()
	data, err := json.Marshal(cfg.Models["smart"].SmartRouting)
	require.NoError(t, err)
	mr.Set("aigw:config:smart_routing:smart", string(data))
	mr.HSet("aigw:config:model_versions", "smart", "99") // Cache generation differs from document revision.
	source := NewRedisConfigSource(rdb, 0, zap.NewNop())
	manager := NewConfigManager(cfg, source, zap.NewNop())
	got, err := manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Version)
	mr.Del("aigw:config:smart_routing:smart")
	mr.HDel("aigw:config:model_versions", "smart")
	source.checkVersion(ctx)
	got, err = manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Nil(t, got)
	require.False(t, manager.AllKnownModels()["smart"], "Redis revocation is authoritative")
	_, ok := manager.NormalizeModelCode("SMART")
	require.False(t, ok)
}

func TestSmartConfigRejectsNestedDependenciesAcrossSources(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := smartConfigFixture()
	child := cfg.Models["smart"].SmartRouting.Clone()
	child.JudgeModel = "other-judge"
	child.Ranges[0].Model, child.Ranges[1].Model = "x", "y"
	data, err := json.Marshal(child)
	require.NoError(t, err)
	mr.Set("aigw:config:smart_routing:small", string(data))
	mr.HSet("aigw:config:model_versions", "small", "1")
	manager := NewConfigManager(cfg, NewRedisConfigSource(rdb, 0, zap.NewNop()), zap.NewNop())
	_, err = manager.GetSmartRouting(ctx, "smart")
	require.Error(t, err, "static smart cannot depend on a Redis smart")
}

func TestSmartConfigHTTPRejectsInvalidUpdateBeforePublication(t *testing.T) {
	cfg := smartConfigFixture()
	provider := NewHTTPGatewayProvider("", "", false)
	provider.UpdateConfig(cfg)
	cfg.Models["smart"].SmartRouting.Ranges[0].Max = 70
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(cfg))
	}))
	t.Cleanup(server.Close)
	provider.adminURL = server.URL
	poller := NewHTTPConfigPoller(provider, 0, zap.NewNop())
	called := false
	err := poller.pollConfig(context.Background(), func(context.Context, *GatewayConfig) error {
		called = true
		return nil
	})
	require.Error(t, err)
	require.False(t, called)
	snapshot, err := provider.GetConfig(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, 50, snapshot.Models["smart"].SmartRouting.Ranges[0].Max)
}

func TestSmartConfigRedisTypeChangeCannotExposeOldOrdinaryEndpoints(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := smartConfigFixture()
	cfg.Models["smart"] = ModelConfig{RequestTypes: []string{"chat_completion"}, Endpoints: []EndpointConfig{{Provider: "p", URL: "https://example.invalid"}}}
	cfg.Providers = map[string]ProviderConfig{"p": {Protocol: "openai"}}
	mr.Set("aigw:config:endpoints:smart", `[{"provider_name":"old-ordinary"}]`)
	mr.HSet("aigw:config:model_versions", "smart", "1")
	source := NewRedisConfigSource(rdb, 0, zap.NewNop())
	manager := NewConfigManager(cfg, source, zap.NewNop())
	require.Len(t, manager.GetEndpoints(ctx, "smart"), 1)
	data, err := json.Marshal(smartConfigFixture().Models["smart"].SmartRouting)
	require.NoError(t, err)
	mr.Del("aigw:config:endpoints:smart")
	mr.Set("aigw:config:smart_routing:smart", string(data))
	mr.HSet("aigw:config:model_versions", "smart", "2")
	source.checkVersion(ctx)
	require.Empty(t, manager.GetEndpoints(ctx, "smart"))
	mr.Del("aigw:config:smart_routing:smart")
	mr.HDel("aigw:config:model_versions", "smart")
	source.checkVersion(ctx)
	require.Empty(t, manager.GetEndpoints(ctx, "smart"), "revoked smart model cannot fall back to stale ordinary YAML")
}

func TestSmartConfigRedisOrdinaryOverrideRevocationDoesNotResurrectStaticSmart(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Set("aigw:config:endpoints:smart", `[{"provider_name":"ordinary"}]`)
	mr.HSet("aigw:config:model_versions", "smart", "7")
	source := NewRedisConfigSource(rdb, 0, zap.NewNop())
	manager := NewConfigManager(smartConfigFixture(), source, zap.NewNop())
	got, err := manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Nil(t, got, "the initial observed Redis definition is ordinary")
	require.True(t, manager.HasModel(ctx, "smart"))
	mr.Del("aigw:config:endpoints:smart")
	mr.HDel("aigw:config:model_versions", "smart")
	source.checkVersion(ctx)
	source.ClearCache()
	got, err = manager.GetSmartRouting(ctx, "smart")
	require.NoError(t, err)
	require.Nil(t, got, "deleting an ordinary override must not resurrect the static smart definition")
	require.False(t, manager.HasModel(ctx, "smart"))
	require.False(t, manager.AllKnownModels()["smart"])
	_, aliasExists := manager.GetAlias("auto")
	require.False(t, aliasExists)
}

func TestSmartConfigRedisOrdinaryOnlyOverridePreservesLegacyStaticFallback(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Set("aigw:config:endpoints:gpt-4", `[{"provider_name":"redis-normal"}]`)
	mr.HSet("aigw:config:model_versions", "gpt-4", "1")
	source := NewRedisConfigSource(rdb, 0, zap.NewNop())
	manager := NewConfigManager(newTestYAMLConfig(), source, zap.NewNop())
	require.Equal(t, "redis-normal", manager.GetEndpoints(ctx, "gpt-4")[0].ProviderName)
	mr.Del("aigw:config:endpoints:gpt-4")
	mr.HDel("aigw:config:model_versions", "gpt-4")
	source.checkVersion(ctx)
	source.ClearCache()
	require.True(t, manager.HasModel(ctx, "gpt-4"))
	require.Equal(t, "openai", manager.GetEndpoints(ctx, "gpt-4")[0].ProviderName)
}

func TestSmartConfigYAMLRejectsNonIntegerNumbersBeforeConversion(t *testing.T) {
	const template = `models:
  smart:
    model_type: smart
    request_types: [chat_completion]
    smart_routing:
      version: %s
      judge_model: judge
      judge_timeout_ms: %s
      judge_max_input_bytes: %s
      judge_max_output_tokens: %s
      ranges:
        - {min: %s, max: %s, model: small}
        - {min: %s, max: 100, model: large}
`
	tests := []struct {
		name   string
		values [7]string
	}{
		{"fractional boundary", [7]string{"1", "5000", "65536", "256", "0", "40.5", "40.5"}},
		{"negative fractional minimum", [7]string{"1", "5000", "65536", "256", "-0.5", "40", "40"}},
		{"fractional version", [7]string{"1.5", "5000", "65536", "256", "0", "40", "40"}},
		{"fractional timeout", [7]string{"1", "5000.5", "65536", "256", "0", "40", "40"}},
		{"negative fractional timeout", [7]string{"1", "-0.5", "65536", "256", "0", "40", "40"}},
		{"fractional input limit", [7]string{"1", "5000", "65536.5", "256", "0", "40", "40"}},
		{"fractional output limit", [7]string{"1", "5000", "65536", "256.5", "0", "40", "40"}},
		{"float typed boundary", [7]string{"1", "5000", "65536", "256", "0", "40.0", "40.0"}},
		{"quoted integer boundary", [7]string{"1", "5000", "65536", "256", "0", `"40"`, `"40"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			x := tt.values
			raw := fmt.Sprintf(template, x[0], x[1], x[2], x[3], x[4], x[5], x[6])
			require.NoError(t, v.ReadConfig(strings.NewReader(raw)))
			_, err := Load(v)
			require.Error(t, err, "YAML must not truncate or coerce smart-routing numeric fields")
			rawJSON := fmt.Sprintf(`{"version":%s,"judge_model":"judge","judge_timeout_ms":%s,"judge_max_input_bytes":%s,"judge_max_output_tokens":%s,"ranges":[{"min":%s,"max":%s,"model":"small"},{"min":%s,"max":100,"model":"large"}]}`, x[0], x[1], x[2], x[3], x[4], x[5], x[6])
			var runtimeDocument core.SmartRoutingConfig
			require.Error(t, json.Unmarshal([]byte(rawJSON), &runtimeDocument), "YAML rejection must match the HTTP/Redis JSON document contract")
		})
	}
}

func TestSmartConfigYAMLMatchesJSONWithIntegerNumbers(t *testing.T) {
	raw := `models:
  smart:
    model_type: smart
    request_types: [chat_completion]
    smart_routing:
      version: 3
      judge_model: judge
      judge_timeout_ms: 5000
      judge_max_input_bytes: 65536
      judge_max_output_tokens: 256
      ranges:
        - {min: 0, max: 40, model: small}
        - {min: 40, max: 100, model: large}
`
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(raw)))
	yamlConfig, err := Load(v)
	require.NoError(t, err)
	require.NoError(t, Validate(yamlConfig))
	var jsonConfig GatewayConfig
	require.NoError(t, json.Unmarshal([]byte(`{"models":{"smart":{"model_type":"smart","request_types":["chat_completion"],"smart_routing":{"version":3,"judge_model":"judge","judge_timeout_ms":5000,"judge_max_input_bytes":65536,"judge_max_output_tokens":256,"ranges":[{"min":0,"max":40,"model":"small"},{"min":40,"max":100,"model":"large"}]}}}}`), &jsonConfig))
	require.NoError(t, Validate(&jsonConfig))
	require.Equal(t, jsonConfig.Models["smart"].SmartRouting, yamlConfig.Models["smart"].SmartRouting)
}
