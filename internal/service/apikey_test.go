package service

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func getProjectRoot() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(b)))
}

func setupTestRedis(t *testing.T) (*redis.Client, *log.Logger) {
	v := config.NewConfig(filepath.Join(getProjectRoot(), "config", "local.yml"))

	rdb := redis.NewClient(&redis.Options{
		Addr:     v.GetString("data.redis.addr"),
		Password: v.GetString("data.redis.password"),
		DB:       v.GetInt("data.redis.db"),
	})

	logger := log.NewLog(v)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		t.Skipf("skipping test as Redis is not reachable: %v", err)
	}

	return rdb, logger
}

func TestApiKeyService_ValidateAndCache(t *testing.T) {
	rdb, logger := setupTestRedis(t)
	defer rdb.Close()

	svc := NewApiKeyService(config.NewRedisGatewayProvider(rdb), logger)
	ctx := context.Background()

	testKey := "sk-test-mock-enterprise-apikey-888"
	redisKey := "aigw:apikey:" + testKey

	// 1. 写入测试 Key 到 Redis
	_, err := rdb.HSet(ctx, redisKey, map[string]interface{}{
		"user_id":    "tenant_user_009",
		"status":     1,
		"quota":      5000,
		"rate_limit": `{"qps":10}`,
		"expires_at": 0,
	}).Result()
	if err != nil {
		t.Fatalf("failed to set test key in redis: %v", err)
	}
	defer rdb.Del(ctx, redisKey)

	// 2. 第一次查询：命中 Redis
	t.Run("first lookup (Redis query)", func(t *testing.T) {
		info, err := svc.ValidateKey(ctx, testKey)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if info.UserID != "tenant_user_009" || info.Status != 1 || info.Quota != 5000 {
			t.Errorf("unexpected fields: %+v", info)
		}
	})

	// 3. 第二次查询：应该命中 Local 二级缓存
	t.Run("second lookup (Local Cache hit)", func(t *testing.T) {
		// 先在 Redis 里修改 status（如果是直连，会查出新 status；如果是 local cache 命中，会依旧返回老值）
		rdb.HSet(ctx, redisKey, "status", 2)

		info, err := svc.ValidateKey(ctx, testKey)
		if err != nil {
			t.Fatalf("expected local cache hit, got error: %v", err)
		}
		// status 应该依然为 1，因为本地缓存还未失效
		if info.Status != 1 {
			t.Errorf("expected local cache hit (status=1), but got status=%d", info.Status)
		}

		// 恢复原状
		rdb.HSet(ctx, redisKey, "status", 1)
	})

	// 4. 测试 VerifyKey
	t.Run("VerifyKey wrapper method", func(t *testing.T) {
		userID, tenant, workspaceID, userTenant, err := svc.VerifyKey(ctx, testKey)
		if err != nil {
			t.Fatalf("VerifyKey failed: %v", err)
		}
		if userID != "tenant_user_009" {
			t.Errorf("expected User ID 'tenant_user_009', got '%s'", userID)
		}
		if tenant != "" {
			t.Errorf("expected empty Tenant, got '%s'", tenant)
		}
		if workspaceID != "" {
			t.Errorf("expected empty WorkspaceID, got '%s'", workspaceID)
		}
		if userTenant != "" {
			t.Errorf("expected empty UserTenant, got '%s'", userTenant)
		}
	})

	// 4.1 测试 Portal API Key (同时包含 user_id, tenant 且包含 workspace_id)
	t.Run("Portal API Key format with user, tenant and workspace_id", func(t *testing.T) {
		portalKey := "sk-portal-mock-key-111"
		portalRedisKey := "aigw:apikey:" + portalKey
		_, err := rdb.HSet(ctx, portalRedisKey, map[string]interface{}{
			"user_id":      "usr-portal-user",
			"tenant":       "company-a",
			"workspace_id": "ws-portal-space",
			"status":       1,
			"quota":        -1,
			"expires_at":   0,
		}).Result()
		if err != nil {
			t.Fatalf("failed to set portal key in redis: %v", err)
		}
		defer rdb.Del(ctx, portalRedisKey)

		info, err := svc.ValidateKey(ctx, portalKey)
		if err != nil {
			t.Fatalf("ValidateKey failed for portal key: %v", err)
		}
		if info.UserID != "usr-portal-user" || info.Tenant != "company-a" || info.WorkspaceID != "ws-portal-space" {
			t.Errorf("unexpected field mapping: %+v", info)
		}
	})

	// 5. 测试 Key 被禁用
	t.Run("Key Disabled check", func(t *testing.T) {
		disabledKey := "sk-disabled-test-key-777"
		dRedisKey := "aigw:apikey:" + disabledKey
		rdb.HSet(ctx, dRedisKey, map[string]interface{}{
			"user_id": "disabled_usr",
			"status":  2, // 禁用
		})
		defer rdb.Del(ctx, dRedisKey)

		// 第一次查，应该立刻感知到禁用
		svc.cache.Remove(disabledKey)
		_, err := svc.ValidateKey(ctx, disabledKey)
		if err == nil {
			t.Error("expected error for disabled key, got nil")
		} else {
			logger.Logger.Info("disabled key error successfully caught", zap.Error(err))
		}
	})

	// 6. 测试 Key 过期
	t.Run("Key Expired check", func(t *testing.T) {
		expiredKey := "sk-expired-test-key-666"
		eRedisKey := "aigw:apikey:" + expiredKey
		rdb.HSet(ctx, eRedisKey, map[string]interface{}{
			"user_id":    "expired_usr",
			"status":     1,
			"expires_at": time.Now().Unix() - 100, // 已过期
		})
		defer rdb.Del(ctx, eRedisKey)

		svc.cache.Remove(expiredKey)
		_, err := svc.ValidateKey(ctx, expiredKey)
		if err == nil {
			t.Error("expected error for expired key, got nil")
		} else {
			logger.Logger.Info("expired key error successfully caught", zap.Error(err))
		}
	})

	// 7. 负向缓存测试
	t.Run("Negative Caching prevent cache penetration", func(t *testing.T) {
		nonExistKey := "sk-random-non-existent-key-99999"

		// 第一次查不存在的 Key，Redis 查不到返回错误
		_, err1 := svc.ValidateKey(ctx, nonExistKey)
		if err1 == nil {
			t.Fatal("expected error for non existent key, got nil")
		}

		// 故意清空或关闭测试连接来证明根本没有再次打到 Redis，而是立刻命中了本地负向缓存
		// 在这里，我们可以把 rdb 换成一个错误链接（比如关闭链接）
		rdb.Close()
		_, err2 := svc.ValidateKey(ctx, nonExistKey)
		if err2 == nil {
			t.Fatal("expected cached error from negative cache, got nil")
		}
		if err2.Error() != "invalid API key" {
			t.Errorf("expected negative cached 'invalid API key' error, got %v", err2)
		}
	})
}
