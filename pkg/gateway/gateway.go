// Package gateway provides a product-level embeddable facade for tokenlive-gateway.
// External hosts (e.g. tokenlive-standalone) can build the Engine and register LLM
// routes on a caller-owned Gin engine without starting cmd/server.
package gateway

import (
	"fmt"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/internal/bootstrap"
	"github.com/tokenlive/tokenlive-gateway/internal/handler"
	"github.com/tokenlive/tokenlive-gateway/internal/repository"
	"github.com/tokenlive/tokenlive-gateway/internal/router"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"

	_ "github.com/tokenlive/tokenlive-gateway/pkg/llm/providers"
)

// Gateway is an embeddable LLM gateway instance (Engine + Gin route registration).
// It does not listen on a port; the host owns HTTP lifecycle.
type Gateway struct {
	Engine   *core.Engine
	Provider config.GatewayProvider
	Config   *config.ConfigManager

	v      *viper.Viper
	logger *log.Logger
	rdb    *redis.Client

	modelService   *service.ModelService
	apiKeyService  *service.ApiKeyService
	policyService  *service.PolicyService
	aliasService   *service.AliasService
	llmHandler     *handler.LLMHandler
	enableAuth     bool
}

// Options configures optional dependencies for New.
type Options struct {
	// Provider overrides config_source-based provider selection.
	// Used by tokenlive-standalone to inject an EmbeddedGatewayProvider.
	Provider config.GatewayProvider

	// Redis, if non-nil, skips creating a client from conf.
	Redis *redis.Client

	// ClickHouse, if set (including explicit nil via SkipClickHouse), overrides conf-based CH.
	ClickHouse     clickhouse.Conn
	SkipClickHouse bool
}

// New builds a Gateway from viper config without starting HTTP or job servers.
// cleanup closes Engine background work and optional Redis/ClickHouse owned by New.
func New(conf *viper.Viper, logger *log.Logger, opts *Options) (*Gateway, func(), error) {
	if conf == nil {
		return nil, nil, fmt.Errorf("gateway: conf is nil")
	}
	if logger == nil {
		return nil, nil, fmt.Errorf("gateway: logger is nil")
	}
	if opts == nil {
		opts = &Options{}
	}

	var cleanups []func()
	cleanupAll := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	var rdb *redis.Client
	if opts.Redis != nil {
		rdb = opts.Redis
	} else {
		redisCfg := repository.LoadRedisConfig(conf)
		client, redisCleanup, err := repository.NewRedis(redisCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("gateway: redis: %w", err)
		}
		rdb = client
		if redisCleanup != nil {
			cleanups = append(cleanups, redisCleanup)
		}
	}

	var provider config.GatewayProvider
	var err error
	if opts.Provider != nil {
		provider = opts.Provider
	} else {
		provider, err = bootstrap.ProvideGatewayProvider(conf, rdb)
		if err != nil {
			cleanupAll()
			return nil, nil, err
		}
	}

	configMgr, err := bootstrap.NewGatewayConfigManager(conf, logger, rdb)
	if err != nil {
		cleanupAll()
		return nil, nil, err
	}

	modelService := service.NewModelService(rdb, logger, conf)
	apiKeyService := service.NewApiKeyService(provider, logger)
	aliasService := service.NewAliasService(rdb, logger, configMgr)

	var chConn clickhouse.Conn
	if opts.SkipClickHouse {
		chConn = nil
	} else if opts.ClickHouse != nil {
		chConn = opts.ClickHouse
	} else {
		conn, chCleanup, chErr := repository.NewClickHouse(conf, logger)
		if chErr != nil {
			cleanupAll()
			return nil, nil, fmt.Errorf("gateway: clickhouse: %w", chErr)
		}
		chConn = conn
		if chCleanup != nil {
			cleanups = append(cleanups, chCleanup)
		}
	}

	engine, policySvc, engCleanup, err := bootstrap.NewGatewayEngine(
		conf, logger, modelService, apiKeyService, configMgr, rdb, chConn, provider,
	)
	if err != nil {
		cleanupAll()
		return nil, nil, err
	}
	if engCleanup != nil {
		cleanups = append(cleanups, engCleanup)
	}

	llmHandler := handler.NewLLMHandler(engine, modelService, configMgr, aliasService)

	enableAuth := conf.GetBool("llm.enable_auth")
	gw := &Gateway{
		Engine:        engine,
		Provider:      provider,
		Config:        configMgr,
		v:             conf,
		logger:        logger,
		rdb:           rdb,
		modelService:  modelService,
		apiKeyService: apiKeyService,
		policyService: policySvc,
		aliasService:  aliasService,
		llmHandler:    llmHandler,
		enableAuth:    enableAuth,
	}

	return gw, cleanupAll, nil
}

// RegisterGin mounts LLM (+ Gemini) routes on the given Gin engine.
// Registers groups /v1 and /v1beta (same as cmd/server).
func (g *Gateway) RegisterGin(r *gin.Engine) {
	if g == nil || r == nil {
		return
	}
	deps := router.RouterDeps{
		Logger:        g.logger,
		Config:        g.v,
		LLMHandler:    g.llmHandler,
		ApiKeyService: g.apiKeyService,
	}
	v1 := r.Group("/v1")
	router.InitLLMRouter(deps, v1)
	v1beta := r.Group("/v1beta")
	router.InitGeminiRouter(deps, v1beta)
}

// Handler returns an http.Handler that serves only LLM routes on a private Gin engine.
func (g *Gateway) Handler() http.Handler {
	r := gin.New()
	r.Use(gin.Recovery())
	g.RegisterGin(r)
	return r
}

// UpdateEngineConfig hot-reloads Engine pipelines (models/endpoints/providers).
func (g *Gateway) UpdateEngineConfig(cfg *core.EngineConfig) error {
	if g == nil || g.Engine == nil {
		return fmt.Errorf("gateway: not initialized")
	}
	return g.Engine.UpdateConfig(cfg)
}

// PurgeAPIKeyCache clears the in-process API key cache.
func (g *Gateway) PurgeAPIKeyCache() {
	if g != nil && g.apiKeyService != nil {
		g.apiKeyService.PurgeCache()
	}
}

// UpdateYAMLConfig replaces the YAML layer of ConfigManager (local/static models).
func (g *Gateway) UpdateYAMLConfig(gwCfg *config.GatewayConfig) {
	if g != nil && g.Config != nil {
		g.Config.UpdateYAMLConfig(gwCfg)
	}
}

// PurgePolicyCache clears the in-process policy cache.
func (g *Gateway) PurgePolicyCache() {
	if g != nil && g.policyService != nil {
		g.policyService.PurgeCache()
	}
}

// PurgeAliasCache clears the in-process alias cache.
func (g *Gateway) PurgeAliasCache() {
	if g != nil && g.aliasService != nil {
		g.aliasService.PurgeCache()
	}
}

// ApplyGatewayConfig hot-reloads Engine pipelines + ConfigManager from a full GatewayConfig
// (same path as HTTP config poller). Used by tokenlive-standalone ConfigHub.
func (g *Gateway) ApplyGatewayConfig(gwCfg *config.GatewayConfig) error {
	if g == nil || g.Engine == nil {
		return fmt.Errorf("gateway: not initialized")
	}
	if gwCfg == nil {
		return fmt.Errorf("gateway: config is nil")
	}
	engineConfig, providerImpls, _, _ := bootstrap.BuildFromRelationalConfig(gwCfg, g.enableAuth)
	if err := g.Engine.UpdateConfig(engineConfig); err != nil {
		return fmt.Errorf("engine update config: %w", err)
	}
	g.Engine.SetProviders(providerImpls)
	if g.Config != nil {
		g.Config.UpdateYAMLConfig(gwCfg)
		if g.modelService != nil {
			g.modelService.UpdateFallbackModels(config.KnownModels(gwCfg))
		}
	}
	return nil
}
