package bootstrap

import (
	"context"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDynamicEndpointAdapterPreservesPriority(t *testing.T) {
	cfg := &config.GatewayConfig{
		Models: map[string]config.ModelConfig{
			"test-model": {
				RequestTypes: []string{"chat_completion"},
				Endpoints: []config.EndpointConfig{
					{ID: "primary", Provider: "test-provider", URL: "http://primary", Priority: 1},
					{ID: "secondary", Provider: "test-provider", URL: "http://secondary", Priority: 2},
				},
			},
		},
		Providers: map[string]config.ProviderConfig{
			"test-provider": {Protocol: "openai"},
		},
	}
	manager := config.NewConfigManager(cfg, nil, zap.NewNop())

	endpoints := (&dynamicEndpointAdapter{mgr: manager}).GetEndpoints(context.Background(), "test-model")

	require.Len(t, endpoints, 2)
	assert.Equal(t, 1, endpoints[0].Priority)
	assert.Equal(t, 2, endpoints[1].Priority)
}

func TestDynamicEndpointAdapterPreservesCapacity(t *testing.T) {
	cfg := &config.GatewayConfig{
		Models: map[string]config.ModelConfig{
			"test-model": {
				RequestTypes:    []string{"chat_completion"},
				ContextLength:   128000,
				MaxOutputTokens: 8192,
				Endpoints: []config.EndpointConfig{
					{ID: "ep-1", Provider: "test-provider", URL: "http://ep1"},
					{ID: "ep-2", Provider: "test-provider", URL: "http://ep2", ContextLength: 32768, MaxOutputTokens: 4096},
				},
			},
		},
		Providers: map[string]config.ProviderConfig{
			"test-provider": {Protocol: "openai"},
		},
	}
	manager := config.NewConfigManager(cfg, nil, zap.NewNop())

	endpoints := (&dynamicEndpointAdapter{mgr: manager}).GetEndpoints(context.Background(), "test-model")

	require.Len(t, endpoints, 2)
	assert.EqualValues(t, 128000, endpoints[0].ContextLength)
	assert.EqualValues(t, 8192, endpoints[0].MaxOutputTokens)
	assert.EqualValues(t, 32768, endpoints[1].ContextLength)
	assert.EqualValues(t, 4096, endpoints[1].MaxOutputTokens)
}
