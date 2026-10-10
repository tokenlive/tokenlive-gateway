package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
	"github.com/tokenlive/tokenlive-gateway/pkg/lbs"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/routers"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"
	"go.uber.org/zap"
)

type registryDependencies struct {
	config         *viper.Viper
	logger         *log.Logger
	models         *service.ModelService
	apiKeys        *service.ApiKeyService
	configManager  *config.ConfigManager
	policies       *service.PolicyService
	stateStore     core.StateStore
	queue          compensation.Queue
	providers      map[string]core.Provider
	discovery      *core.StaticDiscovery
	redis          *redis.Client
	clickhouse     clickhouse.Conn
	metricsSink    outbound.MetricsSink
	metrics        *telemetry.MetricsRegistry
	configSource   string
	stateStoreMode string
	adminURL       string
	syncToken      string
}

// 注册仅属于当前 Engine 的实例，不重复注册 provider init 安装的全局 factory。
func setupEngineRegistry(engine *core.Engine, lifetime *gatewayLifetime, deps registryDependencies) {
	cbMetrics := core.NewCircuitBreakerMetrics(deps.metrics.CircuitBreakerState)
	engine.CircuitBreakerManager().SetMetrics(cbMetrics)

	if deps.queue != nil {
		engine.SetCompQueue(deps.queue)
	}
	engine.SetProviders(deps.providers)
	engine.SetStaticDiscovery(deps.discovery)
	engine.SetInvokerBuilder(invoker.NewBuilder())

	aliasService := service.NewAliasService(deps.redis, deps.logger, deps.configManager)
	engine.SetAliasService(aliasService)

	// Register Router factories.
	engine.RegisterRouterFactory("capability", func(cfg core.RouterConfig, _ core.StateStore, _ *zap.Logger) core.Router {
		return &routers.CapabilityRouter{}
	})
	engine.RegisterRouterFactory("tenant_endpoint", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewTenantEndpointRouter(deps.redis, l)
	})
	engine.RegisterRouterFactory("circuit_breaker", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewCircuitBreakerRouter(engine.CircuitBreakerManager(), deps.config.GetBool("llm.enable_active_health_check"), l)
	})
	engine.RegisterRouterFactory("priority", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewPriorityRouter(l)
	})
	engine.RegisterRouterFactory("tag", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
		return routers.NewTagRouter(l)
	})

	// Register LoadBalancer factories.
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

	// Register InboundFilters.
	engine.RegisterFilter("auth", inbound.NewAuthFilter())
	engine.RegisterFilter("session_reader", inbound.NewSessionReaderFilter("X-Session-ID"))
	engine.RegisterFilter("credits_check", inbound.NewCreditsCheckFilter(deps.apiKeys))
	engine.RegisterFilter("tagging", inbound.NewTaggingFilter())
	rateLimitFilter := inbound.NewRateLimitFilter(deps.stateStore)
	engine.RegisterFilter("rate_limit", rateLimitFilter)
	engine.RegisterFilter("validate", inbound.NewValidateFilter(deps.models))

	// Register OutboundFilters.
	tokenSettlementFilter := outbound.NewTokenSettlementFilter(deps.stateStore, deps.apiKeys, deps.logger.Logger)
	engine.RegisterFilter("token_settlement", tokenSettlementFilter)
	if deps.models != nil && deps.configManager != nil {
		deps.models.SetConfigManager(deps.configManager)
		engine.SetSmartRouter(invoker.NewSmartRouterWithAccounting(deps.configManager, deps.models, deps.policies, engine, rateLimitFilter, tokenSettlementFilter))
	}
	engine.RegisterFilter("sticky_session", outbound.NewStickySessionFilter(deps.stateStore, 5*time.Minute))
	engine.RegisterFilter("metrics", outbound.NewMetricsFilter(
		deps.metrics,
		&outbound.DefaultMetricsExtractor{},
		deps.logger.Logger,
	))
	lifetime.accessLog = outbound.NewAccessLogFilter(deps.logger.Logger, deps.redis, deps.queue, deps.clickhouse, deps.config)
	engine.RegisterFilter("access_log", lifetime.accessLog)

	statusCollector := outbound.NewStatusCollectorFilter(deps.redis, engine.CircuitBreakerManager(), deps.adminURL, deps.syncToken, deps.logger.Logger)
	lifetime.status = statusCollector
	statusCollector.SetIncludeClientDisconnect(deps.configSource == "embedded" && deps.stateStoreMode == "memory")
	if deps.metricsSink != nil {
		statusCollector.SetMetricsSink(deps.metricsSink)
	}
	engine.RegisterFilter("status_collector", statusCollector)

	// Register Event Publisher filter.
	var eventsCfg events.PublisherConfig
	if deps.config.IsSet("events") {
		_ = deps.config.UnmarshalKey("events", &eventsCfg)
	}
	eventPublisher := newRuntimePublisher(lifetime.tasks, events.NewPublisher(eventsCfg, deps.redis, deps.adminURL, deps.syncToken))
	lifetime.publisher = eventPublisher
	eventPubFilter := outbound.NewEventPublishFilter(eventPublisher, deps.logger.Logger)
	eventPubFilter.SetDiscovery(engine.Discovery())
	engine.RegisterFilter("event_publisher", eventPubFilter)

	// For InvokerDependencyResolver.Publisher().
	engine.SetPublisher(eventPublisher)

	// 熔断事件与轮询、订阅共用后台准入，关闭后不再接收新发布任务。
	engine.CircuitBreakerManager().SetEventHandler(func(evt core.CBEvent) {
		lifetime.tasks.start(func(ctx context.Context) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := eventPublisher.Publish(ctx, circuitBreakerOpsEvent(evt, deps.discovery)); err != nil {
				deps.logger.Logger.Warn("circuit breaker event publish failed", zap.String("key", evt.Key), zap.Error(err))
			}
		})
	})
}

func circuitBreakerOpsEvent(evt core.CBEvent, discovery *core.StaticDiscovery) *events.OpsEvent {
	provider := evt.ProviderName
	if provider == "" && strings.Contains(evt.Key, ":") {
		provider = strings.Split(evt.Key, ":")[0]
	}
	transition := ""
	if evt.OldState != "" && evt.NewState != "" {
		transition = fmt.Sprintf("[%s->%s] ", evt.OldState, evt.NewState)
	}
	result := &events.OpsEvent{
		EventType: events.EventTypeCircuitBreak, TenantCode: evt.TenantCode,
		ModelCode: evt.ModelCode, ProviderName: provider, PolicyID: evt.PolicyID,
		PolicyName: evt.PolicyName, Threshold: evt.Threshold, CurrentValue: evt.CurrentValue,
		RequestID: evt.RequestID, TraceID: evt.TraceID,
		Message: transition + "circuit breaker opened: " + evt.Key, Timestamp: time.Now().Unix(),
	}
	if !strings.Contains(evt.Key, ":") {
		result.EndpointID, result.EndpointCode = evt.Key, evt.EndpointCode
		if result.EndpointCode == "" {
			if ep := findEndpointByID(discovery, evt.Key); ep != nil {
				result.EndpointCode = ep.Code
			}
		}
	}
	return result
}
