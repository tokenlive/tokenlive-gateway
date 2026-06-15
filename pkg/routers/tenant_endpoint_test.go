package routers

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func getTestProjectRoot() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(b)))
}

func setupTenantRouterTestRedis(t *testing.T) (*redis.Client, *zap.Logger) {
	v := config.NewConfig(filepath.Join(getTestProjectRoot(), "config", "local.yml"))

	rdb := redis.NewClient(&redis.Options{
		Addr:     v.GetString("data.redis.addr"),
		Password: v.GetString("data.redis.password"),
		DB:       v.GetInt("data.redis.db"),
	})

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		t.Skipf("skipping test as Redis is not reachable: %v", err)
	}

	return rdb, logger
}

func TestTenantEndpointRouter_Route(t *testing.T) {
	rdb, logger := setupTenantRouterTestRedis(t)
	defer rdb.Close()

	router := NewTenantEndpointRouter(rdb, logger)
	ctx := context.Background()

	ep1 := &core.Endpoint{ID: "ep1", Provider: "openai-official", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep2", Provider: "openai-custom", Model: "gpt-4"}
	ep3 := &core.Endpoint{ID: "ep3", Provider: "anthropic-official", Model: "gpt-4"}
	endpoints := []*core.Endpoint{ep1, ep2, ep3}

	// 1. 非租户请求 (gctx.Tenant == "") -> 不做任何过滤
	t.Run("non-tenant request passes all", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:    ctx,
			Tenant: "",
			Model:  "gpt-4",
		}
		result := router.Route(gctx, endpoints)
		assert.Len(t, result, 3)
	})

	// 2. 租户请求，但 Redis 白名单不存在 (Empty = 全放通) -> 不做任何过滤
	t.Run("empty whitelist allows all", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:    ctx,
			Tenant: "company-temp-empty",
			Model:  "gpt-4",
		}
		redisKey := "aigw:tenant:company-temp-empty:model:gpt-4:endpoints"
		rdb.Del(ctx, redisKey)

		result := router.Route(gctx, endpoints)
		assert.Len(t, result, 3)
	})

	// 3. 租户请求，Redis 拥有白名单配置 -> 只保留允许的 Endpoint ID
	t.Run("whitelist filters matching endpoints", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:    ctx,
			Tenant: "company-a",
			Model:  "gpt-4",
		}
		redisKey := "aigw:tenant:company-a:model:gpt-4:endpoints"

		// 写入允许的端点 ID
		err := rdb.SAdd(ctx, redisKey, "ep1", "ep2").Err()
		require.NoError(t, err)
		defer rdb.Del(ctx, redisKey)

		result := router.Route(gctx, endpoints)
		assert.Len(t, result, 2)
		assert.Equal(t, "ep1", result[0].ID)
		assert.Equal(t, "ep2", result[1].ID)
	})

	// 4. 验证本地缓存命中
	t.Run("local cache hit prevents querying redis again immediately", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:    ctx,
			Tenant: "company-cache-test",
			Model:  "gpt-4",
		}
		redisKey := "aigw:tenant:company-cache-test:model:gpt-4:endpoints"

		// 写入允许的端点 ID
		err := rdb.SAdd(ctx, redisKey, "ep1").Err()
		require.NoError(t, err)
		defer rdb.Del(ctx, redisKey)

		// 第一次调用 Route，将结果加入缓存
		result1 := router.Route(gctx, endpoints)
		assert.Len(t, result1, 1)
		assert.Equal(t, "ep1", result1[0].ID)

		// 修改 Redis 里的白名单，把 "ep1" 删掉，加上 "ep2"
		rdb.Del(ctx, redisKey)
		err = rdb.SAdd(ctx, redisKey, "ep2").Err()
		require.NoError(t, err)

		// 第二次调用 Route。因为本地缓存了 30s，所以应该依然返回 result1 对应的 "ep1"，而不是修改后的 "ep2"
		result2 := router.Route(gctx, endpoints)
		assert.Len(t, result2, 1)
		assert.Equal(t, "ep1", result2[0].ID) // 证明命中了缓存，没有读取最新的 Redis 数据

		// 清理掉路由里的缓存（手动失效），或者因为测试用的 router 后面用不到了，我们也可以手动删除 Key 以便测试其它用例
		router.cache.Remove(redisKey)

		// 移除缓存后再查，应该拿到最新修改的 "ep2"
		result3 := router.Route(gctx, endpoints)
		assert.Len(t, result3, 1)
		assert.Equal(t, "ep2", result3[0].ID)
	})

	// 5. 验证负向缓存写入及避免雪崩
	t.Run("negative cache prevents redundant redis query on failure", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:    ctx,
			Tenant: "company-failure-test",
			Model:  "gpt-4",
		}
		redisKey := "aigw:tenant:company-failure-test:model:gpt-4:endpoints"
		rdb.Del(ctx, redisKey)

		// 制造一个会引发错误的 key，或者为了让 SMembers 报错，我们可以写入一个 String 类型的 key
		// 这样 SMembers 就会返回 WRONGTYPE 错误。
		err := rdb.Set(ctx, redisKey, "not-a-set", 0).Err()
		require.NoError(t, err)
		defer rdb.Del(ctx, redisKey)

		// 第一次调用，由于 Redis SMembers 报错，会发生 Fail-Open，返回所有 endpoints
		result1 := router.Route(gctx, endpoints)
		assert.Len(t, result1, 3)

		// 并且它应该把错误写入了负向缓存中。
		// 我们将 Redis 里的 key 删除掉
		rdb.Del(ctx, redisKey)

		// 此时如果再次调用 Route，它不应该去读 Redis（因为如果去读 Redis，由于 key 被删了，白名单变空，返回值应该是全通）
		// 但因为负向缓存命中了，它应该直接打印 Debug 日志，不发请求到 Redis 并依然 Fail-Open。
		// 我们可以通过验证缓存中确实有负向记录来证明：
		_, errMsg, ok := router.cache.Get(redisKey)
		assert.True(t, ok)
		assert.Contains(t, errMsg, "WRONGTYPE")

		// 同样，清理测试产生的缓存
		router.cache.Remove(redisKey)
	})
}
