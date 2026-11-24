package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/tokenlive/tokenlive-gateway/internal/bootstrap"
	"github.com/tokenlive/tokenlive-gateway/internal/handler"
	"github.com/tokenlive/tokenlive-gateway/internal/router"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

func TestGeminiGeneration(t *testing.T) {
	t.Setenv("ADMIN_SERVER_URL", "")
	t.Setenv("GATEWAY_SYNC_TOKEN", "")
	const (
		model       = "gemini-4-neon"
		tenant      = "test-tenant"
		apiKey      = "tl_live_gemini_generation_test"
		upstreamKey = "upstream-gemini-key"
		body        = `{"contents":[{"parts":[{"text":"hello"}]}]}`
		completion  = `{"candidates":[{"content":{"parts":[{"text":"pong"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, upstreamKey, r.Header.Get("x-goog-api-key"))
		payload, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, body, string(payload))

		switch r.URL.Path {
		case "/upstream/version/models/neon:streamGenerateContent":
			assert.Equal(t, "sse", r.URL.Query().Get("alt"))
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: "+completion+"\n\n")
		case "/upstream/version/models/neon:generateContent":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, completion)
		default:
			t.Errorf("unexpected upstream path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(upstream.Close)

	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	const pepper = "gemini-generation-test-pepper"
	redisServer.HSet(store.RedisKeyApiKeyHash(config.HashAPIKey(apiKey, pepper)),
		"user_id", "test-user", "tenant", tenant, "status", "1", "credits", "-1", "expires_at", "0")
	_, err := redisServer.SAdd("aigw:tenant:"+tenant+":models", model)
	require.NoError(t, err)
	_, err = redisServer.SAdd("aigw:tenant:"+tenant+":model:"+model+":endpoints", "allowed")
	require.NoError(t, err)
	redisServer.HSet("aigw:policies:model:"+model, "*:billing", `{"input_price":1,"output_price":1}`)
	endpoint := config.ResolvedEndpoint{
		ID:               "allowed",
		URL:              upstream.URL + "/upstream/version",
		RealModel:        "neon",
		ProviderName:     "google",
		ProviderProtocol: "gemini",
		RequestTypes:     []string{"gemini_generate_content"},
		APIKey:           upstreamKey,
		Priority:         1,
		Weight:           1,
		Timeout:          2000,
	}
	disallowed := endpoint
	disallowed.ID = "disallowed"
	disallowed.RealModel = "disallowed"
	disallowed.Priority = 0
	endpoints, err := json.Marshal([]config.ResolvedEndpoint{disallowed, endpoint})
	require.NoError(t, err)
	require.NoError(t, redisServer.Set(store.RedisKeyConfigEndpoints(model), string(endpoints)))

	cfg := viper.New()
	cfg.Set("gateway.config_source", "redis")
	cfg.Set("gateway.state_store", "memory")
	cfg.Set("llm.enable_auth", true)
	cfg.Set("models", map[string]any{})
	logger := &log.Logger{Logger: zap.NewNop()}
	provider := config.NewRedisGatewayProviderWithAPIKeyPepper(client, pepper)
	keys := service.NewApiKeyService(provider, logger)
	models := service.NewModelService(client, logger, cfg)
	manager, err := bootstrap.NewGatewayConfigManager(cfg, logger, client)
	require.NoError(t, err)
	engine, _, cleanup, err := bootstrap.NewGatewayEngine(cfg, logger, models, keys, manager, client, nil, provider, nil)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	server := NewHTTPServer(router.RouterDeps{
		Logger:        logger,
		Config:        cfg,
		ApiKeyService: keys,
		LLMHandler:    handler.NewLLMHandler(engine, models, manager, service.NewAliasService(client, logger, manager)),
	})

	for _, tc := range []struct {
		name    string
		path    string
		header  string
		payload string
		content string
		want    string
	}{
		{"v1 stream", "/v1/models/" + model + ":streamGenerateContent?alt=sse",
			apiKey, body, "text/event-stream", "data: " + completion + "\n\n"},
		{"v1 nonstream", "/v1/models/" + model + ":generateContent?key=" + apiKey,
			"", `{"model":"models/` + model + `",` + body[1:], "application/json", completion},
		{"v1beta stream", "/v1beta/models/" + model + ":streamGenerateContent?alt=sse",
			apiKey, body, "text/event-stream", "data: " + completion + "\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.payload))
			r.Header.Set("Content-Type", "application/json")
			if tc.header != "" {
				r.Header.Set("x-goog-api-key", tc.header)
			}
			w := httptest.NewRecorder()
			server.ServeHTTP(w, r)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Contains(t, w.Header().Get("Content-Type"), tc.content)
			require.Equal(t, tc.want, w.Body.String())
		})
	}

	missingKey := httptest.NewRecorder()
	server.ServeHTTP(missingKey, httptest.NewRequest(http.MethodPost, "/v1/models/"+model+":generateContent", strings.NewReader(body)))
	require.Equal(t, http.StatusUnauthorized, missingKey.Code)

	unsupported := httptest.NewRequest(http.MethodPost, "/v1/models/"+model+":countTokens", strings.NewReader(body))
	unsupported.Header.Set("x-goog-api-key", apiKey)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, unsupported)
	require.Equal(t, http.StatusNotFound, w.Code)
}
