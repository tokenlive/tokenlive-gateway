package routers

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// TenantEndpointRouter 租户模型端点白名单路由过滤器
type TenantEndpointRouter struct {
	rdb    *redis.Client
	logger *zap.Logger
	cache  *store.ExpirableCache[string, []string]
}

func NewTenantEndpointRouter(rdb *redis.Client, logger *zap.Logger) *TenantEndpointRouter {
	// 初始化缓存，正向缓存 30秒 TTL（1000最大容量），负向缓存 10秒 TTL（500最大容量）
	cache := store.NewExpirableCache[string, []string](
		1000, 30*time.Second,
		500, 10*time.Second,
	)
	return &TenantEndpointRouter{
		rdb:    rdb,
		logger: logger,
		cache:  cache,
	}
}

func (r *TenantEndpointRouter) Name() string { return "tenant_endpoint" }

func (r *TenantEndpointRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	// 非租户请求直接跳过
	if gctx.Tenant == "" {
		return endpoints
	}

	ctx := gctx.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// 从 Redis 获取该租户在该模型下被允许的端点白名单集合
	// 键格式与 admin 侧保持一致: aigw:tenant:{tenantCode}:model:{modelCode}:endpoints
	redisKey := store.RedisKeyTenantEndpoints(gctx.Tenant, gctx.Model)

	// 1. 优先从本地二级缓存读取
	if allowedEndpoints, errMsg, ok := r.cache.Get(redisKey); ok {
		if errMsg != "" {
			// 负向缓存命中，打印调试日志并采取降级放通策略
			r.logger.Debug("tenant endpoint routing: local invalid cache hit, fallback to open",
				zap.String("tenant", gctx.Tenant),
				zap.String("model", gctx.Model),
				zap.String("error", errMsg),
			)
			return endpoints
		}
		// 正向缓存命中，执行过滤逻辑
		return r.filterEndpoints(endpoints, allowedEndpoints, gctx.Tenant, gctx.Model)
	}

	allowedEndpoints, err := r.rdb.SMembers(ctx, redisKey).Result()
	if err != nil {
		// Redis 查询失败默认降级策略为"放通"（Fail-Open），避免阻断核心流量
		r.logger.Error("failed to query tenant endpoint whitelist, fallback to open",
			zap.String("tenant", gctx.Tenant),
			zap.String("model", gctx.Model),
			zap.Error(err),
		)
		// 写入负向缓存，防止 Redis 宕机时，大量高频请求导致频繁穿透发生雪崩（缓存10秒）
		r.cache.AddInvalid(redisKey, err.Error())
		return endpoints
	}

	// 2. 写入本地正向缓存，缓存30秒
	r.cache.AddValid(redisKey, allowedEndpoints)

	return r.filterEndpoints(endpoints, allowedEndpoints, gctx.Tenant, gctx.Model)
}

func (r *TenantEndpointRouter) filterEndpoints(endpoints []*core.Endpoint, allowedEndpoints []string, tenant, model string) []*core.Endpoint {
	// 降级策略: 空 = 全放通
	if len(allowedEndpoints) == 0 {
		return endpoints
	}

	// 构建用于极速匹配的 Set
	allowedSet := make(map[string]bool, len(allowedEndpoints))
	for _, id := range allowedEndpoints {
		allowedSet[id] = true
	}

	// 过滤 Endpoint（按 Endpoint ID 匹配）
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if allowedSet[ep.ID] {
			result = append(result, ep)
		} else {
			r.logger.Debug("tenant endpoint routing: endpoint skipped due to endpoint restriction",
				zap.String("tenant", tenant),
				zap.String("model", model),
				zap.String("endpoint_id", ep.ID),
				zap.String("provider", ep.Provider),
			)
		}
	}

	return result
}
