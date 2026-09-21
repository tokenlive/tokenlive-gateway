package service

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"go.uber.org/zap"
)

func TestModelServiceRecognizesEndpointFreeSmartWithoutWeakeningTenantAuthorization(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Set("aigw:config:smart_routing:smart", `{"version":1,"judge_model":"judge","ranges":[{"min":0,"max":50,"model":"small"},{"min":50,"max":100,"model":"large"}]}`)
	mr.HSet("aigw:config:model_versions", "smart", "1")
	ms := newTestModelService(t, rdb)
	valid, err := ms.ValidateModel(ctx, "smart", "", "u1")
	require.NoError(t, err)
	require.True(t, valid, "a smart model does not need its own endpoint")
	valid, err = ms.ValidateModel(ctx, "smart", "unrestricted", "u1")
	require.NoError(t, err)
	require.True(t, valid)
	mr.SAdd("aigw:tenant:restricted:models", "small")
	valid, err = ms.ValidateModel(ctx, "smart", "restricted", "u1")
	require.NoError(t, err)
	require.False(t, valid, "smart routing must not bypass the tenant allowlist")
	mr.SAdd("aigw:tenant:restricted:models", "smart")
	valid, err = ms.ValidateModel(ctx, "smart", "restricted", "u1")
	require.NoError(t, err)
	require.True(t, valid)
	mr.Del("aigw:config:smart_routing:smart")
	mr.HDel("aigw:config:model_versions", "smart")
	valid, err = ms.ValidateModel(ctx, "smart", "", "u1")
	require.NoError(t, err)
	require.False(t, valid)
}

func TestModelServiceInjectedSnapshotSmartAliasAndRevocation(t *testing.T) {
	cfg := &config.GatewayConfig{
		Models: map[string]config.ModelConfig{
			"smart": {ModelType: "smart", RequestTypes: []string{"chat_completion"}, SmartRouting: &core.SmartRoutingConfig{
				Version: 1, JudgeModel: "judge",
				Ranges: []core.SmartRoutingRange{{Min: 0, Max: 50, Model: "small"}, {Min: 50, Max: 100, Model: "large"}},
			}},
		},
		Aliases: map[string]string{"auto": "smart"},
	}
	manager := config.NewConfigManager(cfg, nil, zap.NewNop())
	ms := newTestModelService(t, nil)
	ms.SetConfigManager(manager)
	valid, err := ms.ValidateModel(context.Background(), "AUTO", "", "u1")
	require.NoError(t, err)
	require.True(t, valid)
	manager.UpdateYAMLConfig(&config.GatewayConfig{})
	valid, err = ms.ValidateModel(context.Background(), "smart", "", "u1")
	require.NoError(t, err)
	require.False(t, valid, "embedded/HTTP refresh must revoke the removed model")
}

func TestModelServiceLegacyFallbackUpdateStillRecognizesOrdinaryModels(t *testing.T) {
	ms := newTestModelService(t, nil)
	ms.UpdateFallbackModels(map[string]bool{"new-normal": true})
	valid, err := ms.ValidateModel(context.Background(), "new-normal", "", "u1")
	require.NoError(t, err)
	require.True(t, valid)
	ms.UpdateFallbackModels(map[string]bool{})
	valid, err = ms.ValidateModel(context.Background(), "new-normal", "", "u1")
	require.NoError(t, err)
	require.False(t, valid)
}
