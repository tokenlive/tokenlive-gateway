package bootstrap

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"
	"github.com/tokenlive/tokenlive-gateway/pkg/versionreport"

	"github.com/tokenlive/tokenlive-gateway/internal/service"

	// Blank import registers providers via init().
	_ "github.com/tokenlive/tokenlive-gateway/pkg/llm/providers"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// One runtime identity per Gateway process, independent of engine rebuilds.
var versionReportInstanceID = uuid.NewString()

// NewGatewayDataStores creates StateStore and CompensationQueue.
// Chooses Redis or in-memory based on state_store config / Redis availability.
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

// NewGatewayConfigManager builds a layered ConfigManager from viper
// (shared provider for LLMHandler and GatewayEngine).
// Returns error if models config is missing.
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

// ProvideGatewayProvider creates the unified GatewayProvider from config.
func ProvideGatewayProvider(v *viper.Viper, rdb *redis.Client) (config.GatewayProvider, error) {
	configSource := v.GetString("gateway.config_source")
	// Auto-detect for backward compatibility.
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
		// Fall back to RedisProvider (rdb may be nil).
		apiKeyPepper := v.GetString("llm.api_key_pepper")
		return config.NewRedisGatewayProviderWithAPIKeyPepper(rdb, apiKeyPepper), nil
	}
}

// NewGatewayEngine creates the Engine from viper, shared Redis client, and optional ClickHouse audit sink.
// The returned PolicyService is the same instance wired into the Engine (for cache purge on hot reload).
func NewGatewayEngine(
	v *viper.Viper,
	logger *log.Logger,
	modelService *service.ModelService,
	apiKeyService *service.ApiKeyService,
	configMgr *config.ConfigManager,
	rdb *redis.Client,
	chConn clickhouse.Conn,
	provider config.GatewayProvider,
	metricsSink outbound.MetricsSink,
) (*core.Engine, *service.PolicyService, func(), error) {

	otelCleanup, otelErr := telemetry.InitOTelMetrics(v, logger.Logger)
	if otelErr != nil {
		return nil, nil, nil, fmt.Errorf("init otel metrics: %w", otelErr)
	}

	lifetime := &gatewayLifetime{logger: logger.Logger, otelCleanup: otelCleanup}
	built := false
	defer func() {
		if !built {
			lifetime.close()
		}
	}()

	// Explicit DI for MetricsRegistry.
	metricsRegistry, regErr := telemetry.NewMetricsRegistry(otel.GetMeterProvider())
	if regErr != nil {
		return nil, nil, nil, fmt.Errorf("create metrics registry: %w", regErr)
	}

	// Optional auth config.
	validKeys := readAuthKeys(v)
	enableAuth := v.GetBool("llm.enable_auth") || len(validKeys) > 0

	configSource := v.GetString("gateway.config_source")
	stateStoreMode := v.GetString("gateway.state_store")

	// Auto-detect for backward compatibility.
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

	// Fail-fast validation.
	if configSource == "redis" && rdb == nil {
		return nil, nil, nil, fmt.Errorf("config_source is set to 'redis', but redis is not configured or failed to connect")
	}
	if stateStoreMode == "redis" && rdb == nil {
		return nil, nil, nil, fmt.Errorf("state_store is set to 'redis', but redis is not configured or failed to connect")
	}

	// Log policy/config source details.
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
		// Model-centric format: models (with endpoints) + providers.
		gwCfg, err = config.Load(v)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("load gateway config: %w", err)
		}
		if err := config.Validate(gwCfg); err != nil {
			return nil, nil, nil, fmt.Errorf("validate gateway config: %w", err)
		}

		engineConfig, providerImpls, _, _ = BuildFromRelationalConfig(gwCfg, enableAuth)

		staticDiscovery = core.NewStaticDiscovery()
		resolved := config.Resolve(gwCfg)
		if err := registerEndpointsFromResolvedEndpoints(staticDiscovery, resolved); err != nil {
			return nil, nil, nil, fmt.Errorf("register static endpoints: %w", err)
		}
	} else {
		return nil, nil, nil, fmt.Errorf("no models config found")
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
	lifetime.discovery = gwDiscovery

	stateStore, compQueue, err := NewGatewayDataStores(v, rdb)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create data stores: %w", err)
	}

	lifetime.stateStore, lifetime.queue = stateStore, compQueue

	// Local fallback policies for cold-start resilience.
	var localPolicies []*policy.Policy
	if v.IsSet("policies") {
		_ = v.UnmarshalKey("policies", &localPolicies)
	}

	// Configurable policy priority chain.
	var priorityChain []string
	if v.IsSet("policy.priority_chain") {
		_ = v.UnmarshalKey("policy.priority_chain", &priorityChain)
	}
	if len(priorityChain) == 0 {
		priorityChain = []string{"global", "tenant", "user", "model", "tenant_model", "user_model"}
	}

	policyService := service.NewPolicyService(provider, localPolicies, priorityChain, logger)

	// Inject policyService as core.PolicyProvider.
	engine := core.NewEngine(engineConfig, gwDiscovery, stateStore, policyService, logger.Logger)

	lifetime.engine = engine
	lifetime.tasks = newGatewayRuntime(engine.Context())
	setupEngineRegistry(engine, lifetime, registryDependencies{
		config: v, logger: logger, models: modelService, apiKeys: apiKeyService,
		configManager: configMgr, policies: policyService, stateStore: stateStore,
		queue: compQueue, providers: providerImpls, discovery: staticDiscovery,
		redis: rdb, clickhouse: chConn, metricsSink: metricsSink, metrics: metricsRegistry,
		configSource: configSource, stateStoreMode: stateStoreMode, adminURL: adminURL, syncToken: syncToken,
	})

	if err := engine.Init(); err != nil {
		return nil, nil, nil, fmt.Errorf("engine init: %w", err)
	}
	lifetime.initialized = true

	// Explicit polling starts only after Init succeeds.
	if configMgr != nil {
		lifetime.tasks.start(configMgr.StartRedisPolling)
	}

	// Background tasks.
	engine.StartHealthCheck(engine.Context(), 30*time.Second, v.GetBool("llm.enable_active_health_check"))
	engine.StartCircuitBreakerProbe(engine.Context(), 5*time.Second)

	// HTTP poll sync when using HTTPGatewayProvider.
	if httpProv, ok := provider.(*config.HTTPGatewayProvider); ok {
		pollInterval := v.GetDuration("config_poll_interval")
		if pollInterval <= 0 {
			pollInterval = 10 * time.Second
		}
		poller := config.NewHTTPConfigPoller(httpProv, pollInterval, logger.Logger)

		lifetime.tasks.start(func(ctx context.Context) {
			poller.Start(ctx,
				// 1) Routing config update callback.
				func(ctx context.Context, gwCfg *config.GatewayConfig) error {
					engineConfig, providerImpls, _, _ := BuildFromRelationalConfig(gwCfg, enableAuth)
					if err := engine.UpdateConfig(engineConfig); err != nil {
						return fmt.Errorf("engine update config: %w", err)
					}
					engine.SetProviders(providerImpls)
					if configMgr != nil {
						configMgr.UpdateYAMLConfig(gwCfg)
					}
					return nil
				},
				// 2) Policy config update callback.
				func(ctx context.Context) error {
					policyService.PurgeCache()
					return nil
				},
				// 3) API Key update callback.
				func(ctx context.Context) error {
					if apiKeyService != nil {
						apiKeyService.PurgeCache()
					}
					return nil
				},
			)
		})
	}

	// Redis pub/sub for live cache refresh (RedisGatewayProvider, etc.).
	if rdb != nil {
		lifetime.tasks.start(func(ctx context.Context) {
			var retryDelay = 1 * time.Second
			var maxRetryDelay = 30 * time.Second
			var retryCount = 0
			var maxRetries = 5

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				pubsub := rdb.Subscribe(ctx, "aigw:channel:policy_update", "aigw:channel:apikey_update")
				stopPubsub := context.AfterFunc(ctx, func() { _ = pubsub.Close() })

				// First receive confirms whether Pub/Sub is supported.
				_, err := pubsub.Receive(ctx)
				if err != nil {
					stopPubsub()
					_ = pubsub.Close()
					errMsg := err.Error()
					if strings.Contains(errMsg, "unknown command") || strings.Contains(errMsg, "not allowed") || strings.Contains(errMsg, "ERR unknown") {
						logger.Logger.Warn("Redis Pub/Sub is not supported by current Redis server (unknown command). Falling back to local cache TTL expiration.", zap.Error(err))
						return // Unsupported command: exit permanently, no retry.
					}

					retryCount++
					if retryCount > maxRetries {
						logger.Logger.Warn("Redis Pub/Sub subscription failed permanently after maximum retries. Falling back to local cache TTL expiration.", zap.Error(err))
						return
					}

					logger.Logger.Warn("Redis Pub/Sub subscription failed, retrying...", zap.Int("attempt", retryCount), zap.Duration("delay", retryDelay), zap.Error(err))
					select {
					case <-ctx.Done():
						return
					case <-time.After(retryDelay):
					}
					retryDelay *= 2
					if retryDelay > maxRetryDelay {
						retryDelay = maxRetryDelay
					}
					continue
				}

				// Subscribed; reset retry backoff.
				retryCount = 0
				retryDelay = 1 * time.Second
				logger.Logger.Info("Successfully subscribed to Redis policy & apikey update channels")

				ch := pubsub.Channel()
				var loopErr error
				for {
					select {
					case <-ctx.Done():
						stopPubsub()
						_ = pubsub.Close()
						return
					case msg, ok := <-ch:
						if !ok {
							loopErr = fmt.Errorf("pubsub channel closed")
							break
						}
						switch msg.Channel {
						case "aigw:channel:policy_update":
							policyService.PurgeCache()
							logger.Logger.Info("Redis policy update signal received, local cache purged")
						case "aigw:channel:apikey_update":
							if apiKeyService != nil {
								apiKeyService.PurgeCache()
							}
							logger.Logger.Info("Redis API Key update signal received, local cache purged")
						}
					}
					if loopErr != nil {
						break
					}
				}
				stopPubsub()
				_ = pubsub.Close()

				// Context cancelled: exit cleanly.
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Re-enter outer loop to resubscribe.
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		})
	}

	var compWorker *compensation.Worker
	chEnabled := v.GetBool("access_log.clickhouse.enabled")
	if chEnabled && compQueue != nil && chConn != nil && rdb != nil {

		if redisQ, ok := compQueue.(*compensation.RedisQueue); ok {
			compWorker = compensation.NewWorker(rdb, redisQ, logger.Logger)
			compWorker.RegisterCompensator("access_log", outbound.NewAccessLogCompensator(chConn, logger.Logger))
			lifetime.worker = compWorker
			go compWorker.Run(lifetime.tasks.ctx)
		}
	}

	lifetime.stopReporter = startVersionReporter(engine.Context(), v, rdb, configSource, adminURL, syncToken)
	built = true
	return engine, policyService, lifetime.close, nil

}

// startVersionReporter is called only after a successful engine build.
// Embedded hosts are the standalone edition and must never register themselves
// as professional Gateway nodes, even when Redis or an Admin URL is available.
func startVersionReporter(ctx context.Context, v *viper.Viper, rdb *redis.Client, configSource, adminURL, token string) func() {
	if configSource == "embedded" {
		return func() {}
	}
	var client *http.Client
	closeTransport := func() {}
	if rdb == nil {
		// Match the existing Admin sync trust policy without sharing a client
		// or transport. Certificate verification remains enabled by default.
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: v.GetBool("gateway.admin_tls_skip_verify"),
			},
		}
		client = &http.Client{Transport: transport}
		closeTransport = transport.CloseIdleConnections
	}
	sender := versionreport.SelectSender(rdb, client, adminURL, token)
	if sender == nil {
		closeTransport()
		return func() {}
	}
	namespace := os.Getenv("GATEWAY_VERSION_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}
	version, buildKind := v.GetString("runtime.version"), v.GetString("runtime.build_kind")
	if version == "" {
		version = "dev"
	}
	if buildKind == "" {
		buildKind = "dev"
	}
	node := versionreport.Node{
		SchemaVersion: 1,
		Namespace:     namespace,
		InstanceID:    versionReportInstanceID,
		Version:       version,
		BuildKind:     buildKind,
	}
	reportCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer closeTransport()
		versionreport.Run(reportCtx, sender, node, time.After)
	}()
	return func() {
		cancel()
		<-done
	}
}

// BuildFromRelationalConfig builds EngineConfig and Provider instances from model-centric config.
// Used by HTTP poller and embed hosts (tokenlive-standalone ConfigHub) for hot reload.
func BuildFromRelationalConfig(
	gwCfg *config.GatewayConfig,
	hasAuth bool,
) (*core.EngineConfig, map[string]core.Provider, []core.ProviderConfig, map[string]bool) {
	engineConfig := &core.EngineConfig{
		Pipelines: make(map[string]*core.PipelineConfig),
		Providers: make(map[string]*core.ProviderConfig),
	}

	resolved := config.Resolve(gwCfg)
	knownModels := config.KnownModels(gwCfg)

	// Group models by provider.
	providerModels := make(map[string][]string)
	for modelCode, eps := range resolved {
		for _, re := range eps {
			providerModels[re.ProviderName] = append(providerModels[re.ProviderName], modelCode)
		}
	}

	// Build deduplicated ProviderConfig list.
	providerConfigMap := make(map[string]*core.ProviderConfig)
	for _, eps := range resolved {
		for _, re := range eps {
			if _, exists := providerConfigMap[re.ProviderName]; !exists {
				caps := []core.RequestType{
					core.RequestTypeChatCompletion,
					core.RequestTypeEmbedding,
				}
				switch re.ProviderProtocol {
				case "openai":
					caps = append(caps, core.RequestTypeImageGeneration, core.RequestTypeResponses)
				case "anthropic":
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

	// Create Provider instances.
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

	// Prefer pipelines defined in config file.
	for name, pcfg := range gwCfg.Pipelines {
		engineConfig.Pipelines[name] = pcfg
	}

	addDefaultPipelines(engineConfig.Pipelines, hasAuth)

	for _, pc := range providerConfigMap {
		engineConfig.Providers[pc.Name] = pc
	}

	return engineConfig, providerImpls, providerConfigs, knownModels
}

// registerEndpointsFromResolvedEndpoints registers resolved endpoints onto StaticDiscovery.
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
				ProviderCode:       re.ProviderCode,
				ProviderProtocol:   re.ProviderProtocol,
				APIKey:             re.APIKey,
				AuthType:           re.AuthType,
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
				ContextLength:      re.ContextLength,
				MaxOutputTokens:    re.MaxOutputTokens,
				Healthy:            true,
				RequestTypes:       requestTypes,
			}
			endpoints = append(endpoints, endpoint)
		}
		sd.RegisterService(modelName, endpoints)
	}
	return nil
}

// defaultTimeout is the default request timeout.
const defaultTimeout = 60 * time.Second

// readAuthKeys reads API key map from config.
func readAuthKeys(v *viper.Viper) map[string]string {
	keys := make(map[string]string)
	if v.IsSet("gateway.auth.valid_keys") {
		_ = v.UnmarshalKey("gateway.auth.valid_keys", &keys)
	} else if v.IsSet("llm.auth.valid_keys") {
		_ = v.UnmarshalKey("llm.auth.valid_keys", &keys)
	}
	return keys
}

// findEndpointByID looks up an endpoint by ID in StaticDiscovery.
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
			ProviderCode:       ep.ProviderCode,
			ProviderProtocol:   ep.ProviderProtocol,
			URL:                ep.URL,
			APIKey:             ep.APIKey,
			RealModel:          ep.RealModel,
			Weight:             ep.Weight,
			Priority:           ep.Priority,
			Headers:            ep.Headers,
			Metadata:           ep.Metadata,
			RequestTypes:       ep.RequestTypes,
			InputPrice:         ep.InputPrice,
			OutputPrice:        ep.OutputPrice,
			CachedPrice:        ep.CachedPrice,
			CacheCreationPrice: ep.CacheCreationPrice,
			ContextLength:      ep.ContextLength,
			MaxOutputTokens:    ep.MaxOutputTokens,
		}
	}
	return res
}
