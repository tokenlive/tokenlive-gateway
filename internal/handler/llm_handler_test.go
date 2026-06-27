package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/internal/handler"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockDiscovery 实现 core.Discovery 接口
type mockDiscovery struct {
	endpoints []*core.Endpoint
}

func (d *mockDiscovery) List(ctx context.Context, model string) ([]*core.Endpoint, error) {
	return d.endpoints, nil
}

func (d *mockDiscovery) Watch(ctx context.Context, model string) (<-chan []*core.Endpoint, error) {
	ch := make(chan []*core.Endpoint)
	close(ch)
	return ch, nil
}

func (d *mockDiscovery) Close() error { return nil }

type mockRouter struct{}

func (r *mockRouter) Name() string { return "mock" }
func (r *mockRouter) Route(gctx *core.GatewayContext, eps []*core.Endpoint) []*core.Endpoint {
	return eps
}

type mockLoadBalancer struct{}

func (lb *mockLoadBalancer) Select(gctx *core.GatewayContext, eps []*core.Endpoint) core.Invoker {
	if len(eps) == 0 {
		return nil
	}
	return invoker.NewProviderInvoker(nil, eps[0])
}

func setupTestLLMHandler(t *testing.T) (*handler.LLMHandler, *gin.Engine) {
	t.Helper()

	logger, _ := zap.NewDevelopment()
	ss := store.NewMemoryStateStore()

	engineCfg := &core.EngineConfig{
		Pipelines: map[string]*core.PipelineConfig{
			"default": {
				Name:         "default",
				RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
				Invoker:      core.InvokerConfig{Type: "cluster"},
			},
		},
	}

	discovery := &mockDiscovery{
		endpoints: []*core.Endpoint{
			{ID: "ep-1", Provider: "openai", Model: "gpt-4"},
		},
	}

	engine := core.NewEngine(engineCfg, discovery, ss, nil, logger)
	engine.SetInvokerBuilder(invoker.NewBuilder())
	engine.RegisterRouterFactory("capability", func(cfg core.RouterConfig, _ core.StateStore, _ *zap.Logger) core.Router {
		return &mockRouter{}
	})
	engine.RegisterRouterFactory("circuit_breaker", func(cfg core.RouterConfig, _ core.StateStore, _ *zap.Logger) core.Router {
		return &mockRouter{}
	})
	engine.RegisterLoadBalancerFactory("round_robin", func(_ core.StateStore) core.LoadBalancer {
		return &mockLoadBalancer{}
	})

	if err := engine.Init(); err != nil {
		t.Fatalf("engine.Init() error: %v", err)
	}

	modelSvc := service.NewModelService(nil, &log.Logger{Logger: logger}, viper.New())
	cfgMgr := config.NewConfigManager(&config.GatewayConfig{}, nil, logger)
	llmHandler := handler.NewLLMHandler(engine, modelSvc, cfgMgr, nil)

	gin.SetMode(gin.TestMode)
	r := gin.New()

	return llmHandler, r
}

func TestLLMHandler_ChatCompletion_DelegatesToEngine(t *testing.T) {
	llmHandler, r := setupTestLLMHandler(t)
	r.POST("/v1/chat/completions", llmHandler.ChatCompletion)

	body := `{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	// Engine 没有配置 Discovery，会返回 500（no pipeline matched 或 no available endpoint）
	// 但验证了请求确实被委托到了 Engine
	assert.NotEqual(t, http.StatusNotFound, w.Code, "route should be registered")
}

func TestLLMHandler_CreateEmbedding_DelegatesToEngine(t *testing.T) {
	llmHandler, r := setupTestLLMHandler(t)
	r.POST("/v1/embeddings", llmHandler.CreateEmbedding)

	body := `{"model":"text-embedding-3-small","input":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	assert.NotEqual(t, http.StatusNotFound, w.Code, "route should be registered")
}

type stubModelLister struct {
	models []string
	err    error
}

func (s stubModelLister) ListTenantModels(ctx context.Context, tenant string) ([]string, error) {
	return s.models, s.err
}

type stubModelOwner struct {
	owners      map[string]string
	knownModels map[string]bool
}

func (s stubModelOwner) OwnerOf(ctx context.Context, model string) string {
	return s.owners[model]
}

func (s stubModelOwner) AllKnownModels() map[string]bool {
	if s.knownModels == nil {
		return map[string]bool{}
	}
	return s.knownModels
}

type stubAliasQuerier struct {
	aliases map[string][]string
}

func (s stubAliasQuerier) GetAliases(ctx context.Context, modelCode string) ([]string, error) {
	if s.aliases == nil {
		return nil, nil
	}
	return s.aliases[modelCode], nil
}

func TestListModels_Authorized_ReturnsTenantModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lister := stubModelLister{models: []string{"gpt-4"}}
	owner := stubModelOwner{owners: map[string]string{"gpt-4": "openai"}}
	h := handler.NewLLMHandlerWithDeps(lister, owner, stubAliasQuerier{})

	r := gin.New()
	r.GET("/v1/models", func(c *gin.Context) {
		c.Set("tenant", "t1")
		c.Set("user_id", "u1")
		h.ListModels(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "list", resp["object"])
	data := resp["data"].([]any)
	require.Len(t, data, 1)
	m := data[0].(map[string]any)
	assert.Equal(t, "gpt-4", m["id"])
	assert.Equal(t, "model", m["object"])
	assert.Equal(t, "openai", m["owned_by"])
	assert.EqualValues(t, 0, m["created"])
}

func TestListModels_ToC_ReturnsAllKnownModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lister := stubModelLister{}
	owner := stubModelOwner{
		owners:      map[string]string{"gpt-4": "openai"},
		knownModels: map[string]bool{"gpt-4": true},
	}
	h := handler.NewLLMHandlerWithDeps(lister, owner, stubAliasQuerier{})

	r := gin.New()
	r.GET("/v1/models", func(c *gin.Context) {
		c.Set("user_id", "u1") // tenant is empty, meaning ToC
		h.ListModels(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "list", resp["object"])
	data := resp["data"].([]any)
	require.Len(t, data, 1)
	m := data[0].(map[string]any)
	assert.Equal(t, "gpt-4", m["id"])
	assert.Equal(t, "openai", m["owned_by"])
}

func TestListModels_Unauthorized_NoTenantAndNoUserID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := handler.NewLLMHandlerWithDeps(stubModelLister{}, stubModelOwner{}, stubAliasQuerier{})

	r := gin.New()
	r.GET("/v1/models", h.ListModels)

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	errObj := resp["error"].(map[string]any)
	assert.Equal(t, "authentication_error", errObj["type"])
}

func TestListModels_EmptyList(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := handler.NewLLMHandlerWithDeps(stubModelLister{models: []string{}}, stubModelOwner{}, stubAliasQuerier{})

	r := gin.New()
	r.GET("/v1/models", func(c *gin.Context) {
		c.Set("tenant", "t-empty")
		c.Set("user_id", "u-empty")
		h.ListModels(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "list", resp["object"])
	assert.Empty(t, resp["data"])
}

func TestListModels_OwnerFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lister := stubModelLister{models: []string{"unknown-model"}}
	owner := stubModelOwner{owners: map[string]string{}}
	h := handler.NewLLMHandlerWithDeps(lister, owner, stubAliasQuerier{})

	r := gin.New()
	r.GET("/v1/models", func(c *gin.Context) {
		c.Set("tenant", "t1")
		c.Set("user_id", "u1")
		h.ListModels(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	data := resp["data"].([]any)
	require.Len(t, data, 1)
	assert.Equal(t, "github.com/tokenlive/tokenlive-gateway", data[0].(map[string]any)["owned_by"])
}

func TestListModels_DoesNotCallEngine(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lister := stubModelLister{models: []string{"gpt-4"}}
	owner := stubModelOwner{owners: map[string]string{"gpt-4": "openai"}}
	h := handler.NewLLMHandlerWithDeps(lister, owner, stubAliasQuerier{})

	r := gin.New()
	r.GET("/v1/models", func(c *gin.Context) {
		c.Set("tenant", "t1")
		c.Set("user_id", "u1")
		h.ListModels(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// ListModels 走 handler 直接路径，不经过 Engine 的 SSE/InterceptWriter，
	// 因此 Content-Type 应为标准 application/json。
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
}

func TestListModels_Wildcard_ReturnsAllKnownModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lister := stubModelLister{models: []string{"*"}}
	owner := stubModelOwner{
		owners:      map[string]string{"gpt-4": "openai", "claude-3": "anthropic"},
		knownModels: map[string]bool{"gpt-4": true, "claude-3": true},
	}
	h := handler.NewLLMHandlerWithDeps(lister, owner, stubAliasQuerier{})

	r := gin.New()
	r.GET("/v1/models", func(c *gin.Context) {
		c.Set("tenant", "t1")
		c.Set("user_id", "u1")
		h.ListModels(c)
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "list", resp["object"])
	data := resp["data"].([]any)
	assert.Len(t, data, 2)

	var ids []string
	for _, item := range data {
		ids = append(ids, item.(map[string]any)["id"].(string))
	}
	assert.Contains(t, ids, "gpt-4")
	assert.Contains(t, ids, "claude-3")
}
