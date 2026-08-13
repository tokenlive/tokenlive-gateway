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
