package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/tokenlive/tokenlive-gateway/internal/handler"
	"github.com/tokenlive/tokenlive-gateway/internal/router"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

func TestGeminiListModels(t *testing.T) {
	oldMode, oldWriter := gin.Mode(), gin.DefaultWriter
	var accessLog bytes.Buffer
	gin.SetMode(gin.TestMode)
	gin.DefaultWriter = &accessLog
	t.Cleanup(func() {
		gin.SetMode(oldMode)
		gin.DefaultWriter = oldWriter
	})

	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	const apiKey = "tl_live_gemini_list_test"
	const pepper = "gemini-list-test-pepper"
	redisServer.HSet(store.RedisKeyApiKeyHash(config.HashAPIKey(apiKey, pepper)),
		"user_id", "user-1", "tenant", "tenant-1", "status", "1", "quota", "-1", "expires_at", "0")
	_, err := redisServer.SAdd("aigw:tenant:tenant-1:models", "gemini-flash", "gpt-4")
	require.NoError(t, err)
	_, err = redisServer.SAdd(store.RedisKeyModelAliases("gemini-flash"), "gemini-alias")
	require.NoError(t, err)

	gemini := config.ModelConfig{
		RequestTypes:    []string{"gemini_generate_content"},
		ContextLength:   1000000,
		MaxOutputTokens: 8192,
		Endpoints: []config.EndpointConfig{
			{Provider: "openai", URL: "https://example.invalid", ContextLength: 1234},
			{Provider: "google", URL: "https://example.invalid"},
		},
	}
	observedCore, logs := observer.New(zap.InfoLevel)
	logger := &log.Logger{Logger: zap.New(observedCore)}
	manager := config.NewConfigManager(&config.GatewayConfig{
		Providers: map[string]config.ProviderConfig{
			"google": {Protocol: "gemini"},
			"openai": {Protocol: "openai"},
		},
		Models: map[string]config.ModelConfig{
			"gemini-flash":        gemini,
			"gemini-other-tenant": gemini,
			"gpt-4": {
				RequestTypes: []string{"chat_completion"},
				Endpoints:    []config.EndpointConfig{{Provider: "openai", URL: "https://example.invalid"}},
			},
		},
	}, nil, logger.Logger)
	cfg := viper.New()
	server := NewHTTPServer(router.RouterDeps{
		Logger:        logger,
		Config:        cfg,
		ApiKeyService: service.NewApiKeyService(config.NewRedisGatewayProviderWithAPIKeyPepper(client, pepper), logger),
		LLMHandler: handler.NewLLMHandler(nil, service.NewModelService(client, logger, cfg),
			manager, service.NewAliasService(client, logger, manager)),
	})

	request := func(path, header, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if header != "" {
			r.Header.Set(header, key)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		return w
	}
	requireModels := func(w *httptest.ResponseRecorder, ids ...string) string {
		t.Helper()
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var resp struct {
			Models []struct {
				Name        string   `json:"name"`
				InputLimit  int64    `json:"inputTokenLimit"`
				OutputLimit int64    `json:"outputTokenLimit"`
				Methods     []string `json:"supportedGenerationMethods"`
			} `json:"models"`
			NextPageToken string `json:"nextPageToken"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		require.Len(t, resp.Models, len(ids), w.Body.String())
		for i, id := range ids {
			require.Equal(t, "models/"+id, resp.Models[i].Name)
			require.EqualValues(t, 1000000, resp.Models[i].InputLimit)
			require.EqualValues(t, 8192, resp.Models[i].OutputLimit)
			require.Contains(t, resp.Models[i].Methods, "generateContent")
		}
		return resp.NextPageToken
	}

	require.Empty(t, requireModels(request("/v1/models?pageSize=1000", "x-goog-api-key", apiKey), "gemini-alias", "gemini-flash"))
	require.Empty(t, requireModels(request("/v1/models", "x-goog-api-key", apiKey), "gemini-alias", "gemini-flash"))
	require.Empty(t, requireModels(request("/v1/models?key="+apiKey, "", ""), "gemini-alias", "gemini-flash"))
	require.Empty(t, requireModels(request("/v1beta/models", "Authorization", "Bearer "+apiKey), "gemini-alias", "gemini-flash"))

	token := requireModels(request("/v1/models?pageSize=1", "Authorization", "Bearer "+apiKey), "gemini-alias")
	require.NotEmpty(t, token)
	require.Empty(t, requireModels(request("/v1/models?pageSize=1&pageToken="+url.QueryEscape(token),
		"Authorization", "Bearer "+apiKey), "gemini-flash"))
	require.Equal(t, http.StatusBadRequest, request("/v1/models?pageSize=-1", "x-goog-api-key", apiKey).Code)
	require.Equal(t, http.StatusBadRequest, request("/v1/models?pageToken=!", "x-goog-api-key", apiKey).Code)
	require.Equal(t, http.StatusUnauthorized, request("/v1/models?pageSize=1000", "", "").Code)
	require.Equal(t, http.StatusUnauthorized, request("/v1/models?pageSize=1000", "x-goog-api-key", "invalid-key").Code)

	openAI := request("/v1/models", "Authorization", "Bearer "+apiKey)
	require.Equal(t, http.StatusOK, openAI.Code)
	require.Contains(t, openAI.Body.String(), `"object":"list"`)
	require.Contains(t, openAI.Body.String(), `"id":"gpt-4"`)
	require.NotContains(t, openAI.Body.String(), "gemini-other-tenant")

	_, err = redisServer.SRem("aigw:tenant:tenant-1:models", "gemini-flash")
	require.NoError(t, err)
	empty := request("/v1beta/models", "x-goog-api-key", apiKey)
	require.Equal(t, http.StatusOK, empty.Code)
	require.JSONEq(t, `{"models":[]}`, empty.Body.String())

	require.NotContains(t, accessLog.String(), apiKey)
	for _, entry := range logs.All() {
		fields, err := json.Marshal(entry.ContextMap())
		require.NoError(t, err)
		require.NotContains(t, string(fields), apiKey)
	}
}
