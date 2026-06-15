package service

import (
	"context"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

func TestModelService_ValidateModel(t *testing.T) {
	// 1. 初始化 viper 配置，设置已知模型（新配置格式）
	v := viper.New()
	v.Set("models", map[string]interface{}{
		"gpt-4":         map[string]interface{}{"real_model": "gpt-4"},
		"claude-3-opus": map[string]interface{}{"real_model": "claude-3-opus"},
	})

	// 2. 初始化 miniredis
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	logger := log.NewLog(v)
	ms := NewModelService(rdb, logger, v)

	ctx := context.Background()

	t.Run("ToC mode (tenant is empty) - should check YAML and Redis Config", func(t *testing.T) {
		// gpt-4 存在于 YAML
		valid, err := ms.ValidateModel(ctx, "gpt-4", "", "user-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected gpt-4 to be valid from yaml fallback")
		}

		// unknown-model 不存在于 YAML
		valid, err = ms.ValidateModel(ctx, "unknown-model", "", "user-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if valid {
			t.Error("expected unknown-model to be invalid")
		}

		// 模拟写入 Redis Config 动态端点
		mr.Set("aigw:config:endpoints:deepseek-v3.2", "some-config")
		valid, err = ms.ValidateModel(ctx, "deepseek-v3.2", "", "user-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected deepseek-v3.2 to be valid from Redis Config")
		}
	})

	t.Run("rdb is nil - should fallback to YAML", func(t *testing.T) {
		msNilRdb := NewModelService(nil, logger, v)
		valid, err := msNilRdb.ValidateModel(ctx, "gpt-4", "tenant-1", "user-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected gpt-4 to be valid from yaml fallback when rdb is nil")
		}
	})

	t.Run("key does not exist in redis - should fallback to YAML", func(t *testing.T) {
		// tenant-non-exist 不存在于 Redis，应走 YAML 兜底
		valid, err := ms.ValidateModel(ctx, "gpt-4", "tenant-non-exist", "user-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected gpt-4 to be valid via fallback")
		}

		valid, err = ms.ValidateModel(ctx, "non-exist-model", "tenant-non-exist", "user-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if valid {
			t.Error("expected non-exist-model to be invalid via fallback")
		}

		// 动态模型（不在 YAML 中，但在 Redis Config 中存在）
		mr.Set("aigw:config:endpoints:qwen-turbo", "some-value")
		valid, err = ms.ValidateModel(ctx, "qwen-turbo", "tenant-non-exist", "user-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected qwen-turbo to be valid via dynamic config endpoint check")
		}
	})

	t.Run("key exists in redis - allowed model", func(t *testing.T) {
		tenantID := "tenant-123"
		redisKey := "aigw:tenant:" + tenantID + ":models"

		// 往 redis 集合里加模型权限
		mr.SAdd(redisKey, "gpt-4", "gpt-3.5-turbo")

		// gpt-4 应该被允许
		valid, err := ms.ValidateModel(ctx, "gpt-4", tenantID, "user-123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !valid {
			t.Error("expected gpt-4 to be valid for tenant-123")
		}
	})

	t.Run("key exists in redis - denied model", func(t *testing.T) {
		tenantID := "tenant-123"

		// claude-3-opus 虽在 YAML 里，但没在 tenant-123 的 redis key 里，应被拒绝且不进行 YAML 兜底
		valid, err := ms.ValidateModel(ctx, "claude-3-opus", tenantID, "user-123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if valid {
			t.Error("expected claude-3-opus to be invalid since key exists but not member")
		}
	})
}

// newTestModelService 构造一个用于 ListUserModels / ListTenantModels 测试的 ModelService。
func newTestModelService(t *testing.T, rdb *redis.Client) *ModelService {
	t.Helper()
	v := viper.New()
	return NewModelService(rdb, log.NewLog(v), v)
}

func TestListUserModels_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	userID := "u1"
	mr.SAdd("aigw:user:"+userID+":models", "gpt-4", "claude-3-opus")

	ms := newTestModelService(t, rdb)

	got, err := ms.ListUserModels(context.Background(), userID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 models, got %d (%v)", len(got), got)
	}
	seen := map[string]bool{}
	for _, m := range got {
		seen[m] = true
	}
	if !seen["gpt-4"] || !seen["claude-3-opus"] {
		t.Errorf("expected gpt-4 and claude-3-opus in result, got %v", got)
	}
}

func TestListUserModels_KeyMissing(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ms := newTestModelService(t, rdb)

	got, err := ms.ListUserModels(context.Background(), "user-without-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice, got %v", got)
	}
}

func TestListUserModels_EmptyUserID(t *testing.T) {
	ms := newTestModelService(t, nil)

	got, err := ms.ListUserModels(context.Background(), "")
	if err == nil {
		t.Fatalf("expected error for empty userID, got nil (result=%v)", got)
	}
}

func TestListUserModels_RedisNil(t *testing.T) {
	ms := newTestModelService(t, nil)

	got, err := ms.ListUserModels(context.Background(), "u1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice when rdb is nil, got %v", got)
	}
}

func TestListUserModels_RedisError(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ms := newTestModelService(t, rdb)

	// 关闭 miniredis 服务端，使后续 SMembers 调用产生网络错误
	mr.Close()

	got, err := ms.ListUserModels(context.Background(), "u1")
	if err != nil {
		t.Fatalf("expected no error (redis error must be swallowed), got %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice on redis error, got %v", got)
	}
}

func TestListTenantModels_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	tenant := "t1"
	mr.SAdd("aigw:tenant:"+tenant+":models", "gpt-4", "claude-3-opus")

	ms := newTestModelService(t, rdb)

	got, err := ms.ListTenantModels(context.Background(), tenant)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 models, got %d (%v)", len(got), got)
	}
	seen := map[string]bool{}
	for _, m := range got {
		seen[m] = true
	}
	if !seen["gpt-4"] || !seen["claude-3-opus"] {
		t.Errorf("expected gpt-4 and claude-3-opus in result, got %v", got)
	}
}

func TestListTenantModels_KeyMissing(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ms := newTestModelService(t, rdb)

	got, err := ms.ListTenantModels(context.Background(), "tenant-without-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "*" {
		t.Errorf("expected wildcard ['*'], got %v", got)
	}
}

func TestListTenantModels_EmptyTenant(t *testing.T) {
	ms := newTestModelService(t, nil)

	got, err := ms.ListTenantModels(context.Background(), "")
	if err == nil {
		t.Fatalf("expected error for empty tenant, got nil (result=%v)", got)
	}
}

func TestListTenantModels_RedisNil(t *testing.T) {
	ms := newTestModelService(t, nil)

	got, err := ms.ListTenantModels(context.Background(), "t1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice when rdb is nil, got %v", got)
	}
}

func TestListTenantModels_RedisError(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	ms := newTestModelService(t, rdb)

	// 关闭 miniredis 服务端，使后续 SMembers 调用产生网络错误
	mr.Close()

	got, err := ms.ListTenantModels(context.Background(), "t1")
	if err != nil {
		t.Fatalf("expected no error (redis error must be swallowed), got %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice on redis error, got %v", got)
	}
}
