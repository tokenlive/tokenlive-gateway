package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"go.uber.org/zap"
)

func TestAliasService_InMemoryResolve(t *testing.T) {
	logger := &log.Logger{Logger: zap.NewNop()}
	gwCfg := &config.GatewayConfig{
		Models: map[string]config.ModelConfig{
			"glm-5.3": {
				Endpoints: []config.EndpointConfig{
					{Code: "ep-glm", Provider: "zhipu", RealModel: "glm-5.3"},
				},
			},
			"gpt-5.6": {
				Endpoints: []config.EndpointConfig{
					{Code: "ep-gpt", Provider: "openai", RealModel: "gpt-5.6"},
				},
			},
		},
		Aliases: map[string]string{
			"claude-opus-5.3": "glm-5.3",
			"claude-sonnet":   "gpt-5.6",
		},
	}
	cfgMgr := config.NewConfigManager(gwCfg, nil, zap.NewNop())
	aliasSvc := NewAliasService(nil, logger, cfgMgr)

	ctx := context.Background()

	// 1. Exact alias match
	resolved, err := aliasSvc.Resolve(ctx, "claude-opus-5.3")
	require.NoError(t, err)
	require.Equal(t, "glm-5.3", resolved)

	// 2. Case-insensitive alias match
	resolved, err = aliasSvc.Resolve(ctx, "Claude-Opus-5.3")
	require.NoError(t, err)
	require.Equal(t, "glm-5.3", resolved)

	// 3. Case-insensitive real model normalization
	resolved, err = aliasSvc.Resolve(ctx, "GLM-5.3")
	require.NoError(t, err)
	require.Equal(t, "glm-5.3", resolved)

	// 4. Non-alias model returns original
	resolved, err = aliasSvc.Resolve(ctx, "unknown-model-xyz")
	require.NoError(t, err)
	require.Equal(t, "unknown-model-xyz", resolved)

	// 5. GetAliases for model
	aliases, err := aliasSvc.GetAliases(ctx, "glm-5.3")
	require.NoError(t, err)
	require.Contains(t, aliases, "claude-opus-5.3")

	// 6. PurgeCache & Update config hot-reload
	gwCfg2 := &config.GatewayConfig{
		Models: gwCfg.Models,
		Aliases: map[string]string{
			"claude-opus-5.3": "gpt-5.6",
		},
	}
	cfgMgr.UpdateYAMLConfig(gwCfg2)
	aliasSvc.PurgeCache()

	resolved, err = aliasSvc.Resolve(ctx, "claude-opus-5.3")
	require.NoError(t, err)
	require.Equal(t, "gpt-5.6", resolved)
}
