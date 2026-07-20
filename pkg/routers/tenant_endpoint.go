package routers

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// TenantEndpointRouter filters by tenant model endpoint whitelist.
type TenantEndpointRouter struct {
	rdb    *redis.Client
	logger *zap.Logger
	cache  *store.ExpirableCache[string, []string]
}

func NewTenantEndpointRouter(rdb *redis.Client, logger *zap.Logger) *TenantEndpointRouter {
	// Positive cache 30s TTL (cap 1000); negative 10s (cap 500).
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
	// Skip non-tenant requests.
	if gctx.Tenant == "" {
		return endpoints
	}

	ctx := gctx.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// Load tenant+model endpoint whitelist from Redis.
	// Key: aigw:tenant:{tenantCode}:model:{modelCode}:endpoints
	redisKey := store.RedisKeyTenantEndpoints(gctx.Tenant, gctx.Model)

	// 1. Prefer local L2 cache.
	if allowedEndpoints, errMsg, ok := r.cache.Get(redisKey); ok {
		if errMsg != "" {
			// Negative cache hit: fail-open.
			r.logger.Debug("tenant endpoint routing: local invalid cache hit, fallback to open",
				zap.String("tenant", gctx.Tenant),
				zap.String("model", gctx.Model),
				zap.String("error", errMsg),
			)
			return endpoints
		}
		// Positive cache hit: filter.
		return r.filterEndpoints(endpoints, allowedEndpoints, gctx.Tenant, gctx.Model)
	}

	allowedEndpoints, err := r.rdb.SMembers(ctx, redisKey).Result()
	if err != nil {
		// Redis error: fail-open to avoid blocking traffic.
		r.logger.Error("failed to query tenant endpoint whitelist, fallback to open",
			zap.String("tenant", gctx.Tenant),
			zap.String("model", gctx.Model),
			zap.Error(err),
		)
		// Negative cache 10s to prevent Redis stampede.
		r.cache.AddInvalid(redisKey, err.Error())
		return endpoints
	}

	// 2. Write positive cache (30s).
	r.cache.AddValid(redisKey, allowedEndpoints)

	return r.filterEndpoints(endpoints, allowedEndpoints, gctx.Tenant, gctx.Model)
}

func (r *TenantEndpointRouter) filterEndpoints(endpoints []*core.Endpoint, allowedEndpoints []string, tenant, model string) []*core.Endpoint {
	// Empty whitelist: pass all.
	if len(allowedEndpoints) == 0 {
		return endpoints
	}

	// Build set for O(1) match.
	allowedSet := make(map[string]bool, len(allowedEndpoints))
	for _, id := range allowedEndpoints {
		allowedSet[id] = true
	}

	// Filter by endpoint ID.
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
