package bootstrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestGatewayEngineCleanupClosesPublisherAdmission(t *testing.T) {
	v := versionReportConfig(t)
	engine, _, cleanup, err := NewGatewayEngine(v, &log.Logger{Logger: zap.NewNop()}, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	cleanup()
	cleanup()
	require.ErrorIs(t, engine.Publisher().Publish(context.Background(), nil), context.Canceled)
}

func TestGatewayEngineInitFailureRollsBackWithoutPollingOrClosingBorrowedRedis(t *testing.T) {
	v := versionReportConfig(t)
	v.Set("pipelines.chat_completion", &core.PipelineConfig{
		Name: "chat_completion", InboundFilters: []string{"missing"},
		Invoker: core.InvokerConfig{Type: "cluster"},
	})
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	provider := config.NewHTTPGatewayProvider(server.URL, "token", false)
	engine, _, cleanup, err := NewGatewayEngine(v, &log.Logger{Logger: zap.NewNop()}, nil, nil, nil, rdb, nil, provider, nil)
	require.ErrorContains(t, err, "engine init")
	require.Nil(t, engine)
	require.Nil(t, cleanup)
	require.NoError(t, rdb.Ping(context.Background()).Err())
	require.Never(t, func() bool { return calls.Load() != 0 }, 30*time.Millisecond, time.Millisecond)
}

func TestBuildDefaultsDoNotShareMutableFilterSlices(t *testing.T) {
	first, _, _, _ := BuildFromRelationalConfig(&config.GatewayConfig{}, true)
	second, _, _, _ := BuildFromRelationalConfig(&config.GatewayConfig{}, true)
	first.Pipelines["chat_completion"].InboundFilters[0] = "changed"
	first.Pipelines["embedding"].OutboundFilters[0] = "changed"
	require.Equal(t, "auth", first.Pipelines["embedding"].InboundFilters[0], "pipelines within one build must not share filter slices")
	require.Equal(t, "auth", second.Pipelines["chat_completion"].InboundFilters[0])
	require.Equal(t, "token_settlement", second.Pipelines["embedding"].OutboundFilters[0])
}

func TestBuildDefaultsPreserveExplicitPipelineAndAuthPolicy(t *testing.T) {
	explicit := &core.PipelineConfig{
		Name: "custom-chat", RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
		InboundFilters: []string{"validate"}, OutboundFilters: []string{"access_log"},
		Invoker: core.InvokerConfig{Type: "cluster"},
	}
	for _, auth := range []bool{false, true} {
		cfg, _, _, _ := BuildFromRelationalConfig(&config.GatewayConfig{
			Pipelines: map[string]*core.PipelineConfig{"chat_completion": explicit},
		}, auth)
		require.Same(t, explicit, cfg.Pipelines["chat_completion"])
		for _, name := range []string{"embedding", "image_generation", "messages", "responses"} {
			p := cfg.Pipelines[name]
			require.Equal(t, auth, slices.Contains(p.InboundFilters, "auth"))
			require.Contains(t, p.InboundFilters, "rate_limit")
			if name == "image_generation" {
				require.NotContains(t, p.OutboundFilters, "token_settlement")
				require.Empty(t, p.CriticalOutboundFilters)
			} else {
				require.Equal(t, []string{"token_settlement", "sticky_session"}, p.CriticalOutboundFilters)
			}
		}
	}
}

func TestBuildFromRelationalConfigAddsImageGenerationPipeline(t *testing.T) {
	engineConfig, _, _, _ := BuildFromRelationalConfig(&config.GatewayConfig{}, true)

	pipeline, ok := engineConfig.Pipelines["image_generation"]
	require.True(t, ok)
	assert.Equal(t, []core.RequestType{core.RequestTypeImageGeneration}, pipeline.RequestTypes)
	assert.Contains(t, pipeline.InboundFilters, "auth")
	assert.NotContains(t, pipeline.OutboundFilters, "token_settlement")
}

func TestBuildFromRelationalConfigAdvertisesOpenAIImageCapability(t *testing.T) {
	cfg := &config.GatewayConfig{
		Models: map[string]config.ModelConfig{
			"grok-imagine-image-2.0": {
				RequestTypes: []string{"image_generation"},
				Endpoints: []config.EndpointConfig{
					{Provider: "xai", URL: "https://api.x.ai/v1"},
				},
			},
		},
		Providers: map[string]config.ProviderConfig{
			"xai": {Protocol: "openai"},
		},
	}

	_, _, providers, _ := BuildFromRelationalConfig(cfg, true)
	require.Len(t, providers, 1)
	assert.True(t, slices.Contains(providers[0].RequestTypes, core.RequestTypeImageGeneration))
}

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
