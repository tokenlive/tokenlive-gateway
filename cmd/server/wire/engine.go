package wire

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
	"github.com/tokenlive/tokenlive-gateway/pkg/lbs"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/routers"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"

	"github.com/tokenlive/tokenlive-gateway/internal/service"

	// 空导入触发 provider init() 注册
	_ "github.com/tokenlive/tokenlive-gateway/pkg/llm/providers"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// NewGatewayDataStores 创建 StateStore 和 CompensationQueue。
// 根据 state_store 配置或 Redis 状态确定是使用 Redis 实现还是内存实现。
func NewGatewayDataStores(v *viper.Viper, rdb *redis.Client) (core.StateStore, compensation.Queue, error) {
	stateStoreMode := v.GetString("gateway.state_store")
	if stateStoreMode == "" {
		if rdb != nil {
			stateStoreMode = "redis"
		} else {
			stateStoreMode = "memory"
		}
	}

	if stateStoreMode == "memory" || rdb == nil {
		return store.NewMemoryStateStore(), nil, nil
	}

	stateStore := store.NewRedisStateStore(rdb, nil)
	compQueue, err := compensation.NewRedisQueue(rdb, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("compensation queue: %w", err)
	}

	return stateStore, compQueue, nil
}

// NewGatewayConfigManager 从 viper 创建分层配置管理器（独立 provider，便于注入到
// 多个组件，例如 LLMHandler 与 GatewayEngine 共享同一份 ConfigManager）。
// 若没有 models 配置返回错误。
func NewGatewayConfigManager(
	v *viper.Viper,
	logger *log.Logger,
	rdb *redis.Client,
) (*config.ConfigManager, error) {
	if !v.IsSet("models") {
		return nil, fmt.Errorf("no models config found")
	}

	gwCfg, err := config.Load(v)
	if err != nil {
		return nil, fmt.Errorf("load gateway config: %w", err)
	}
	if err := config.Validate(gwCfg); err != nil {
		return nil, fmt.Errorf("validate gateway config: %w", err)
	}

	var redisSrc *config.RedisConfigSource
	configSource := v.GetString("gateway.config_source")
	if configSource == "" && rdb != nil {
		configSource = "redis"
	}
	if configSource == "redis" && rdb != nil {
		pollInterval := v.GetDuration("config_poll_interval")
		redisSrc = config.NewRedisConfigSource(rdb, pollInterval, logger.Logger)
	}

	return config.NewConfigManager(gwCfg, redisSrc, logger.Logger), nil
}

// ProvideGatewayProvider 根据配置创建统一的 GatewayProvider
func ProvideGatewayProvider(v *viper.Viper, rdb *redis.Client) (config.GatewayProvider, error) {
	configSource := v.GetString("gateway.config_source")
	// 自动探测以向下兼容
	if configSource == "" {
		if rdb != nil {
			configSource = "redis"
		} else {
			configSource = "local"
		}
	}

	switch configSource {
	case "redis":
		if rdb == nil {
			return nil, fmt.Errorf("config_source is set to 'redis', but redis is not configured")
		}
		apiKeyPepper := v.GetString("llm.api_key_pepper")
		return config.NewRedisGatewayProviderWithAPIKeyPepper(rdb, apiKeyPepper), nil
	case "http":
		adminURL := v.GetString("gateway.admin_url")
		if adminURL == "" {
			adminURL = os.Getenv("ADMIN_SERVER_URL")
		}
		syncToken := v.GetString("gateway.sync_token")
		if syncToken == "" {
			syncToken = os.Getenv("GATEWAY_SYNC_TOKEN")
		}
		if adminURL == "" {
			return nil, fmt.Errorf("config_source is set to 'http', but gateway.admin_url is empty")
		}
		tlsSkipVerify := v.GetBool("gateway.admin_tls_skip_verify")
		return config.NewHTTPGatewayProvider(adminURL, syncToken, tlsSkipVerify), nil
	default:
		// 兜底为 RedisProvider (rdb 可以为 nil)
		apiKeyPepper := v.GetString("llm.api_key_pepper")
		return config.NewRedisGatewayProviderWithAPIKeyPepper(rdb, apiKeyPepper), nil
	}
}

// NewGatewayEngine 创建 Engine（直接从 viper 读取配置，使用共享的 Redis client，支持 ClickHouse 审计落库）
func NewGatewayEngine(
	v *viper.Viper,
	logger *log.Logger,
	modelService *service.ModelService,
	apiKeyService *service.ApiKeyService,
	configMgr *config.ConfigManager,
	rdb *redis.Client,
	chConn clickhouse.Conn,
	provider config.GatewayProvider,
) (*core.Engine, func(), error) {

	otelCleanup, otelErr := telemetry.InitOTelMetrics(v, logger.Logger)
	if otelErr != nil {
		return nil, nil, fmt.Errorf("init otel metrics: %w", otelErr)
	}

	// 创建 MetricsRegistry（显式依赖注入）
	metricsRegistry, regErr := telemetry.NewMetricsRegistry(otel.GetMeterProvider())
	if regErr != nil {
		otelCleanup()
		return nil, nil, fmt.Errorf("create metrics registry: %w", regErr)
	}

	// 读取 auth 配置（可选）
	validKeys := readAuthKeys(v)
	enableAuth := v.GetBool("llm.enable_auth") || len(validKeys) > 0

	configSource := v.GetString("gateway.config_source")
	stateStoreMode := v.GetString("gateway.state_store")

	// 自动探测以向下兼容
	if configSource == "" {
		if rdb != nil {
			configSource = "redis"
		} else {
			configSource = "local"
		}
	}
	if stateStoreMode == "" {
		if rdb != nil {
			stateStoreMode = "redis"
		} else {
			stateStoreMode = "memory"
		}
	}

	// 快速失败校验 (Fail-Fast)
	if configSource == "redis" && rdb == nil {
		return nil, nil, fmt.Errorf("config_source is set to 'redis', but redis is not configured or failed to connect")
	}
	if stateStoreMode == "redis" && rdb == nil {
		return nil, nil, fmt.Errorf("state_store is set to 'redis', but redis is not configured or failed to connect")
	}

	// 详细输出数据源及策略获取的配置信息
	adminURL := v.GetString("gateway.admin_url")
	if adminURL == "" {
		adminURL = os.Getenv("ADMIN_SERVER_URL")
	}
	syncToken := v.GetString("gateway.sync_token")
	if syncToken == "" {
		syncToken = os.Getenv("GATEWAY_SYNC_TOKEN")
	}
	redisAddr := v.GetString("redis.addr")
	if redisAddr == "" && rdb != nil {
		redisAddr = "configured"
	}

	logger.Logger.Info("gateway policy and config source initialized",
		zap.String("config_source", configSource),
		zap.String("state_store", stateStoreMode),
		zap.String("admin_url", adminURL),
		zap.String("redis_addr", redisAddr),
		zap.Bool("redis_connected", rdb != nil),
	)

	var gwCfg *config.GatewayConfig
	var err error
	var engineConfig *core.EngineConfig
	var staticDiscovery *core.StaticDiscovery
	var providerImpls map[string]core.Provider

	if v.IsSet("models") {
		// model-centric 格式：models（含 endpoints） + providers
		gwCfg, err = config.Load(v)
		if err != nil {
			return nil, nil, fmt.Errorf("load gateway config: %w", err)
		}
		if err := config.Validate(gwCfg); err != nil {
			return nil, nil, fmt.Errorf("validate gateway config: %w", err)
		}

		// 构建 EngineConfig 和 Provider 实例
		engineConfig, providerImpls, _, _ = buildFromRelationalConfig(gwCfg, enableAuth)

		// 注册 endpoints
		staticDiscovery = core.NewStaticDiscovery()
		resolved := config.Resolve(gwCfg)
		if err := registerEndpointsFromResolvedEndpoints(staticDiscovery, resolved); err != nil {
			return nil, nil, fmt.Errorf("register static endpoints: %w", err)
		}
	} else {
		return nil, nil, fmt.Errorf("no models config found")
	}
	var gwDiscovery core.Discovery
	var serviceDiscovery core.Discovery = staticDiscovery
	if configMgr != nil {
		dynamicDisc := core.NewDynamicDiscovery()
		dynamicDisc.SetDynamicProvider(&dynamicEndpointAdapter{mgr: configMgr})
		serviceDiscovery = core.NewCompositeDiscovery([]core.Discovery{dynamicDisc, staticDiscovery})
	}
	registry := core.NewProviderRegistry(providerImpls)
	gwDiscovery = core.NewAssemblingDiscovery(serviceDiscovery, registry)

	// 创建状态存储和补偿队列（使用共享 client）
	stateStore, compQueue, err := NewGatewayDataStores(v, rdb)
	if err != nil {
		return nil, nil, fmt.Errorf("create data stores: %w", err)
	}

	// 读取本地兜底 policies 配置（冷启动容灾）
	var localPolicies []*policy.Policy
	if v.IsSet("policies") {
		_ = v.UnmarshalKey("policies", &localPolicies)
	}

	// 读取可配置优先级链条
	var priorityChain []string
	if v.IsSet("policy.priority_chain") {
		_ = v.UnmarshalKey("policy.priority_chain", &priorityChain)
	}
	if len(priorityChain) == 0 {
		priorityChain = []string{"global", "tenant", "user", "model", "tenant_model", "user_model"}
	}

	// 创建 PolicyService，直接传入统一的 GatewayProvider
	policyService := service.NewPolicyService(provider, localPolicies, priorityChain, logger)

	// 创建 Engine，将 policyService 作为 core.PolicyProvider 接口注入
	engine := core.NewEngine(engineConfig, gwDiscovery, stateStore, policyService, logger.Logger)

	// 注入熔断器共享 Redis 客户端和指标
	if rdb != nil {
		engine.CircuitBreakerManager().SetRDB(rdb)
	}
	// 注入熔断器指标发射器
	cbMetrics := core.NewCircuitBreakerMetrics(metricsRegistry.CircuitBreakerState)
	engine.CircuitBreakerManager().SetMetrics(cbMetrics)

	// 注入可选组件
	if compQueue != nil {
		engine.SetCompQueue(compQueue)
	}
	engine.SetProviders(providerImpls)
	engine.SetStaticDiscovery(staticDiscovery)
	engine.SetInvokerBuilder(invoker.NewBuilder())

	// 注入别名解析服务
	aliasService := service.NewAliasService(rdb, logger)
	engine.SetAliasService(aliasService)

	// 启动 Redis 版本轮询（新格式配置）
	if configMgr != nil {
		go configMgr.StartRedisPolling(engine.Context())
	}

	// 注册 Router 工厂
	engine.RegisterRouterFactory("capability", func(cfg core.RouterConfig, _ core.StateStore, _ *zap.Logger) core.Router {
		return &routers.CapabilityRouter{}
	})
	engine.RegisterRouterFactory("tenant_endpoint", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewTenantEndpointRouter(rdb, l)
	})
	engine.RegisterRouterFactory("circuit_breaker", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewCircuitBreakerRouter(engine.CircuitBreakerManager(), v.GetBool("llm.enable_active_health_check"), l)
	})
	engine.RegisterRouterFactory("priority", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewPriorityRouter(l)
	})
	engine.RegisterRouterFactory("tag", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewTagRouter(l)
	})

	// 注册 LoadBalancer 工厂
	engine.RegisterLoadBalancerFactory("round_robin", func(_ core.StateStore) core.LoadBalancer {
		return lbs.NewRoundRobin()
	})
	engine.RegisterLoadBalancerFactory("weighted_rr", func(_ core.StateStore) core.LoadBalancer {
		return lbs.NewWeightedRoundRobinLoadBalancer()
	})
	engine.RegisterLoadBalancerFactory("random", func(_ core.StateStore) core.LoadBalancer {
		return lbs.NewRandomLoadBalancer()
	})
	engine.RegisterLoadBalancerFactory("weighted_random", func(_ core.StateStore) core.LoadBalancer {
		return lbs.NewWeightedRandomLoadBalancer()
	})
	engine.RegisterLoadBalancerFactory("least_connections", func(_ core.StateStore) core.LoadBalancer {
		return lbs.NewLeastConnectionsLoadBalancer()
	})
	engine.RegisterLoadBalancerFactory("least_latency", func(ss core.StateStore) core.LoadBalancer {
		return lbs.NewLeastLatencyLoadBalancer(ss)
	})
	engine.RegisterLoadBalancerFactory("cost", func(_ core.StateStore) core.LoadBalancer {
		return lbs.NewCostLoadBalancer()
	})
	engine.RegisterLoadBalancerFactory("composite", func(ss core.StateStore) core.LoadBalancer {
		return lbs.NewCompositeLoadBalancer(ss, 0.5, 0.5)
	})
	engine.RegisterLoadBalancerFactory("sticky", func(ss core.StateStore) core.LoadBalancer {
		return lbs.NewStickyLoadBalancer(ss, lbs.NewRoundRobin(), func(gctx *core.GatewayContext) string {
			return gctx.SessionID
		}, 5*time.Minute)
	})
	engine.RegisterLoadBalancerFactory("endpoint_affinity", func(ss core.StateStore) core.LoadBalancer {
		return lbs.NewEndpointAffinityLoadBalancer(ss)
	})

	// 注册 InboundFilter
	engine.RegisterFilter("auth", inbound.NewAuthFilter())
	engine.RegisterFilter("session_reader", inbound.NewSessionReaderFilter("X-Session-ID"))
	engine.RegisterFilter("credits_check", inbound.NewCreditsCheckFilter(apiKeyService))
	engine.RegisterFilter("tagging", inbound.NewTaggingFilter())
	rateLimitFilter := inbound.NewRateLimitFilter(stateStore)
	engine.RegisterFilter("rate_limit", rateLimitFilter)
	engine.RegisterFilter("validate", inbound.NewValidateFilter(modelService))

	// 注册 OutboundFilter
	engine.RegisterFilter("token_settlement", outbound.NewTokenSettlementFilter(stateStore, apiKeyService, logger.Logger))
	engine.RegisterFilter("sticky_session", outbound.NewStickySessionFilter(stateStore, 5*time.Minute))
	engine.RegisterFilter("metrics", outbound.NewMetricsFilter(
		metricsRegistry,
		&outbound.DefaultMetricsExtractor{},
		logger.Logger,
	))
	engine.RegisterFilter("access_log", outbound.NewAccessLogFilter(logger.Logger, rdb, compQueue, chConn, v))

	engine.RegisterFilter("status_collector", outbound.NewStatusCollectorFilter(rdb, engine.CircuitBreakerManager(), adminURL, syncToken, logger.Logger))

	// 注册 Event Publisher 过滤器
	var eventsCfg events.PublisherConfig
	if v.IsSet("events") {
		_ = v.UnmarshalKey("events", &eventsCfg)
	}
	eventPublisher := events.NewPublisher(eventsCfg, rdb, adminURL, syncToken)
	eventPubFilter := outbound.NewEventPublishFilter(eventPublisher, logger.Logger)
	eventPubFilter.SetDiscovery(engine.Discovery())
	engine.RegisterFilter("event_publisher", eventPubFilter)

	// 注入事件发布器到 Engine，供 InvokerDependencyResolver.Publisher() 使用
	engine.SetPublisher(eventPublisher)

	// 熔断器状态变更时直接发布事件（Closed→Open）
	engine.CircuitBreakerManager().SetEventHandler(func(evt core.CBEvent) {
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			provider := evt.ProviderName

			// 如果是服务级熔断，且缓存的 provider 为空，尝试从 key（provider:model）中拆分
			if provider == "" && strings.Contains(evt.Key, ":") {
				parts := strings.Split(evt.Key, ":")
				if len(parts) > 0 {
					provider = parts[0]
				}
			}

			transitionStr := ""
			if evt.OldState != "" && evt.NewState != "" {
				transitionStr = fmt.Sprintf("[%s->%s] ", evt.OldState, evt.NewState)
			}

			evtOps := &events.OpsEvent{
				EventType:    events.EventTypeCircuitBreak,
				TenantCode:   evt.TenantCode,
				ModelCode:    evt.ModelCode,
				ProviderName: provider,
				PolicyID:     evt.PolicyID,
				PolicyName:   evt.PolicyName,
				Threshold:    evt.Threshold,
				CurrentValue: evt.CurrentValue,
				RequestID:    evt.RequestID,
				TraceID:      evt.TraceID,
				Message:      transitionStr + "circuit breaker opened: " + evt.Key,
				Timestamp:    time.Now().Unix(),
			}
			// 如果是实例级熔断，我们将 EndpointID 设为 key
			if !strings.Contains(evt.Key, ":") {
				evtOps.EndpointID = evt.Key
				evtOps.EndpointCode = evt.EndpointCode
				// 尝试从 StaticDiscovery 查找 endpoint code
				if evtOps.EndpointCode == "" {
					if ep := findEndpointByID(staticDiscovery, evt.Key); ep != nil {
						evtOps.EndpointCode = ep.Code
					}
				}
			}

			if err := eventPublisher.Publish(bgCtx, evtOps); err != nil {
				logger.Logger.Warn("circuit breaker event publish failed", zap.String("key", evt.Key), zap.Error(err))
			}
		}()
	})

	// 初始化引擎
	if err := engine.Init(); err != nil {
		otelCleanup()
		if eventPublisher != nil {
			_ = eventPublisher.Close()
		}
		return nil, nil, fmt.Errorf("engine init: %w", err)
	}

	// 启动后台任务
	engine.StartHealthCheck(engine.Context(), 30*time.Second, v.GetBool("llm.enable_active_health_check"))
	engine.StartCircuitBreakerProbe(engine.Context(), 5*time.Second)

	// 启动 HTTP 轮询同步（如果是 HTTPGatewayProvider）
	if httpProv, ok := provider.(*config.HTTPGatewayProvider); ok {
		pollInterval := v.GetDuration("config_poll_interval")
		if pollInterval <= 0 {
			pollInterval = 10 * time.Second
		}
		poller := config.NewHTTPConfigPoller(httpProv, pollInterval, logger.Logger)

		go poller.Start(engine.Context(),
			// 1) 路由配置更新回调
			func(ctx context.Context, gwCfg *config.GatewayConfig) error {
				engineConfig, providerImpls, _, _ := buildFromRelationalConfig(gwCfg, enableAuth)
				if err := engine.UpdateConfig(engineConfig); err != nil {
					return fmt.Errorf("engine update config: %w", err)
				}
				engine.SetProviders(providerImpls)
				configMgr.UpdateYAMLConfig(gwCfg)
				return nil
			},
			// 2) 策略配置更新回调
			func(ctx context.Context) error {
				policyService.PurgeCache()
				return nil
			},
			// 3) API Key 更新回调
			func(ctx context.Context) error {
				apiKeyService.PurgeCache()
				return nil
			},
		)
	}

	var compWorker *compensation.Worker
	chEnabled := v.GetBool("access_log.clickhouse.enabled")
	if chEnabled && compQueue != nil && chConn != nil && rdb != nil {

		if redisQ, ok := compQueue.(*compensation.RedisQueue); ok {
			compWorker = compensation.NewWorker(rdb, redisQ, logger.Logger)
			compWorker.RegisterCompensator("access_log", outbound.NewAccessLogCompensator(chConn, logger.Logger))
			go compWorker.Run(engine.Context())
		}
	}

	// cleanup 函数
	cleanup := func() {
		if compWorker != nil {
			compWorker.Close()
		}
		otelCleanup()
		if err := engine.Close(); err != nil {
			logger.Logger.Error("engine close error", zap.Error(err))
		}
		if eventPublisher != nil {
			_ = eventPublisher.Close()
		}
	}

	return engine, cleanup, nil

}

// buildFromRelationalConfig 从 model-centric 配置构建 EngineConfig 和 Provider 实例
func buildFromRelationalConfig(
	gwCfg *config.GatewayConfig,
	hasAuth bool,
) (*core.EngineConfig, map[string]core.Provider, []core.ProviderConfig, map[string]bool) {
	engineConfig := &core.EngineConfig{
		Pipelines: make(map[string]*core.PipelineConfig),
		Providers: make(map[string]*core.ProviderConfig),
	}

	resolved := config.Resolve(gwCfg)
	knownModels := config.KnownModels(gwCfg)

	// 按 provider 分组收集 models
	providerModels := make(map[string][]string)
	for modelCode, eps := range resolved {
		for _, re := range eps {
			providerModels[re.ProviderName] = append(providerModels[re.ProviderName], modelCode)
		}
	}

	// 构建 ProviderConfig 列表（去重）
	providerConfigMap := make(map[string]*core.ProviderConfig)
	for _, eps := range resolved {
		for _, re := range eps {
			if _, exists := providerConfigMap[re.ProviderName]; !exists {
				caps := []core.RequestType{
					core.RequestTypeChatCompletion,
					core.RequestTypeEmbedding,
				}
				if re.ProviderProtocol == "openai" {
					caps = append(caps, core.RequestTypeResponses)
				} else if re.ProviderProtocol == "anthropic" {
					caps = []core.RequestType{core.RequestTypeMessages}
				}
				providerConfigMap[re.ProviderName] = &core.ProviderConfig{
					Name:         re.ProviderName,
					Type:         re.ProviderProtocol,
					Models:       providerModels[re.ProviderName],
					RequestTypes: caps,
				}
			}
		}
	}
	var providerConfigs []core.ProviderConfig
	for _, pc := range providerConfigMap {
		providerConfigs = append(providerConfigs, *pc)
	}

	// 创建 Provider 实例
	providerImpls := make(map[string]core.Provider)
	for providerName := range providerConfigMap {
		var firstRE config.ResolvedEndpoint
		found := false
		for _, eps := range resolved {
			for _, re := range eps {
				if re.ProviderName == providerName {
					firstRE = re
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		p, err := llm.NewProvider(firstRE.ProviderProtocol, llm.ProviderConfig{
			Name:    providerName,
			BaseURL: firstRE.URL,
			APIKey:  firstRE.APIKey,
			Models:  providerModels[providerName],
		})
		if err != nil {
			continue
		}
		providerImpls[providerName] = p
	}

	// 优先读取配置文件中定义的 pipelines
	for name, pcfg := range gwCfg.Pipelines {
		engineConfig.Pipelines[name] = pcfg
	}

	// 1. 创建通用的 chat_completion pipeline
	inboundFilters := []string{"tagging", "session_reader", "quota_check", "validate"}
	if hasAuth {
		inboundFilters = append([]string{"auth"}, inboundFilters...)
	}

	if _, exists := engineConfig.Pipelines["chat_completion"]; !exists {
		engineConfig.Pipelines["chat_completion"] = &core.PipelineConfig{
			Name:         "chat_completion",
			RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
			Invoker: core.InvokerConfig{
				Type: "cluster",
			},
			InboundFilters:          inboundFilters,
			OutboundFilters:         []string{"token_settlement", "sticky_session", "metrics", "status_collector", "access_log", "event_publisher"},
			CriticalOutboundFilters: []string{"token_settlement", "sticky_session"},
		}
	}

	// 2. 创建通用的 embedding pipeline
	if _, exists := engineConfig.Pipelines["embedding"]; !exists {
		engineConfig.Pipelines["embedding"] = &core.PipelineConfig{
			Name:         "embedding",
			RequestTypes: []core.RequestType{core.RequestTypeEmbedding},
			Invoker: core.InvokerConfig{
				Type: "cluster",
			},
			InboundFilters:          inboundFilters,
			OutboundFilters:         []string{"token_settlement", "sticky_session", "metrics", "status_collector", "access_log", "event_publisher"},
			CriticalOutboundFilters: []string{"token_settlement", "sticky_session"},
		}
	}

	// 3. 创建通用的 messages pipeline (Anthropic 原生协议)
	if _, exists := engineConfig.Pipelines["messages"]; !exists {
		engineConfig.Pipelines["messages"] = &core.PipelineConfig{
			Name:         "messages",
			RequestTypes: []core.RequestType{core.RequestTypeMessages},
			Invoker: core.InvokerConfig{
				Type: "cluster",
			},
			InboundFilters:          inboundFilters,
			OutboundFilters:         []string{"token_settlement", "sticky_session", "metrics", "status_collector", "access_log", "event_publisher"},
			CriticalOutboundFilters: []string{"token_settlement", "sticky_session"},
		}
	}

	// 5. 创建通用的 responses pipeline
	if _, exists := engineConfig.Pipelines["responses"]; !exists {
		engineConfig.Pipelines["responses"] = &core.PipelineConfig{
			Name:         "responses",
			RequestTypes: []core.RequestType{core.RequestTypeResponses},
			Invoker: core.InvokerConfig{
				Type: "cluster",
			},
			InboundFilters:          inboundFilters,
			OutboundFilters:         []string{"token_settlement", "sticky_session", "metrics", "status_collector", "access_log", "event_publisher"},
			CriticalOutboundFilters: []string{"token_settlement", "sticky_session"},
		}
	}

	for _, pc := range providerConfigMap {
		engineConfig.Providers[pc.Name] = pc
	}

	return engineConfig, providerImpls, providerConfigs, knownModels
}

// registerEndpointsFromResolvedEndpoints 从 resolved endpoints 注册端点到 StaticDiscovery
func registerEndpointsFromResolvedEndpoints(sd *core.StaticDiscovery, resolved map[string][]config.ResolvedEndpoint) error {
	for modelName, eps := range resolved {
		endpoints := make([]*core.Endpoint, 0, len(eps))
		for i, re := range eps {
			if len(re.RequestTypes) == 0 {
				return fmt.Errorf("static endpoint for model %s (provider %s) has no requestTypes configured", modelName, re.ProviderName)
			}
			var requestTypes []core.RequestType
			for _, capStr := range re.RequestTypes {
				requestTypes = append(requestTypes, core.RequestType(capStr))
			}
			epID := re.ID
			if epID == "" {
				epID = fmt.Sprintf("%s-%s-%d", re.ProviderName, modelName, i)
			}
			endpoint := &core.Endpoint{
				ID:                 epID,
				Code:               re.Code,
				URL:                re.URL,
				Provider:           re.ProviderName,
				ProviderProtocol:   re.ProviderProtocol,
				APIKey:             re.APIKey,
				Model:              modelName,
				UpstreamModel:      re.RealModel,
				Weight:             re.Weight,
				Priority:           re.Priority,
				Metadata:           re.Metadata,
				Headers:            re.Headers,
				InputPrice:         re.InputPrice,
				OutputPrice:        re.OutputPrice,
				CachedPrice:        re.CachedPrice,
				CacheCreationPrice: re.CacheCreationPrice,
				Healthy:            true,
				RequestTypes:       requestTypes,
			}
			endpoints = append(endpoints, endpoint)
		}
		sd.RegisterService(modelName, endpoints)
	}
	return nil
}

// defaultTimeout 默认超时
const defaultTimeout = 60 * time.Second

// readAuthKeys 从配置读取 API Key 映射
func readAuthKeys(v *viper.Viper) map[string]string {
	keys := make(map[string]string)
	if v.IsSet("gateway.auth.valid_keys") {
		_ = v.UnmarshalKey("gateway.auth.valid_keys", &keys)
	} else if v.IsSet("llm.auth.valid_keys") {
		_ = v.UnmarshalKey("llm.auth.valid_keys", &keys)
	}
	return keys
}

// findEndpointByID 从 StaticDiscovery 中查找指定 ID 的端点
func findEndpointByID(sd *core.StaticDiscovery, endpointID string) *core.Endpoint {
	if sd == nil {
		return nil
	}
	for _, ep := range sd.GetAllEndpoints() {
		if ep.ID == endpointID {
			return ep
		}
	}
	return nil
}

type dynamicEndpointAdapter struct {
	mgr *config.ConfigManager
}

func (a *dynamicEndpointAdapter) GetEndpoints(ctx context.Context, model string) []core.DynamicEndpoint {
	eps := a.mgr.GetEndpoints(ctx, model)
	res := make([]core.DynamicEndpoint, len(eps))
	for i, ep := range eps {
		res[i] = core.DynamicEndpoint{
			ID:                 ep.ID,
			Code:               ep.Code,
			ProviderName:       ep.ProviderName,
			ProviderProtocol:   ep.ProviderProtocol,
			URL:                ep.URL,
			APIKey:             ep.APIKey,
			RealModel:          ep.RealModel,
			Weight:             ep.Weight,
			Headers:            ep.Headers,
			Metadata:           ep.Metadata,
			RequestTypes:       ep.RequestTypes,
			InputPrice:         ep.InputPrice,
			OutputPrice:        ep.OutputPrice,
			CachedPrice:        ep.CachedPrice,
			CacheCreationPrice: ep.CacheCreationPrice,
		}
	}
	return res
}
