package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func TestPolicyService_GetPolicy_LocalFallback(t *testing.T) {
	rdb, logger := setupTestRedis(t)
	defer rdb.Close()

	// 1. 准备本地兜底 YAML 列表
	localPolicies := []*policy.Policy{
		{
			Permissions: []string{"yaml-fallback"},
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "qps-limit",
					Type: "request",
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 5, TimeWindowInMs: 1000},
					},
				},
			},
		},
	}

	svc := NewPolicyService(rdb, localPolicies, nil, logger)
	ctx := context.Background()

	rdb.Del(ctx, "aigw:policies:global", "aigw:policies:user:u1", "aigw:policies:model:m1")

	p, err := svc.GetPolicy(ctx, "", "u1", "m1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(p.Permissions) == 0 || p.Permissions[0] != "yaml-fallback" {
		t.Errorf("expected permission 'yaml-fallback', got %v", p.Permissions)
	}
	if len(p.LimitPolicies) == 0 || p.LimitPolicies[0].SlidingWindows[0].Threshold != 5 {
		t.Errorf("expected QPS 5, got %v", p.LimitPolicies)
	}
}

func TestPolicyService_GetPolicy_RedisResolutionAndMerge(t *testing.T) {
	rdb, logger := setupTestRedis(t)
	defer rdb.Close()

	svc := NewPolicyService(rdb, nil, nil, logger)
	ctx := context.Background()

	userID := "user_test_merge"
	modelName := "gpt-4"

	// Level 0: Global * + *
	p0 := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "global-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 10, TimeWindowInMs: 1000},
				},
			},
		},
	}
	// Level 1: User-wide * + user
	p1 := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "user-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 20, TimeWindowInMs: 60000},
				},
			},
		},
	}
	// Level 2: Model-wide model + *
	p2 := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "model-limit",
				Type: "token",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 30, TimeWindowInMs: 60000},
				},
			},
		},
	}
	// Level 3: Specialized user + model
	p3 := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "global-limit", // Override level 0
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 40, TimeWindowInMs: 1000},
				},
			},
		},
	}

	p0Bytes, _ := json.Marshal(p0)
	p1Bytes, _ := json.Marshal(p1)
	p2Bytes, _ := json.Marshal(p2)
	p3Bytes, _ := json.Marshal(p3)

	globalKey := "aigw:policies:global"
	userKey := "aigw:policies:user:" + userID
	modelKey := "aigw:policies:model:" + modelName

	defer rdb.Del(ctx, globalKey, userKey, modelKey)

	rdb.HSet(ctx, globalKey, "*", p0Bytes)
	rdb.HSet(ctx, userKey, "*", p1Bytes)
	rdb.HSet(ctx, modelKey, "*", p2Bytes)
	rdb.HSet(ctx, userKey, modelName, p3Bytes)

	merged, err := svc.GetPolicy(ctx, "", userID, modelName)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证合并后的策略集
	limitMap := make(map[string]int64)
	for _, lp := range merged.LimitPolicies {
		if len(lp.SlidingWindows) > 0 {
			limitMap[lp.Name] = lp.SlidingWindows[0].Threshold
		}
	}

	if limitMap["global-limit"] != 40 {
		t.Errorf("expected global-limit threshold 40, got %d", limitMap["global-limit"])
	}
	if limitMap["user-limit"] != 20 {
		t.Errorf("expected user-limit threshold 20, got %d", limitMap["user-limit"])
	}
	if limitMap["model-limit"] != 30 {
		t.Errorf("expected model-limit threshold 30, got %d", limitMap["model-limit"])
	}
}

func TestPolicyService_Caching(t *testing.T) {
	rdb, logger := setupTestRedis(t)
	defer rdb.Close()

	svc := NewPolicyService(rdb, nil, nil, logger)
	ctx := context.Background()

	userID := "user_caching"
	modelName := "gpt-4-cache"

	p0 := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "global-limit",
				Type: "request",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 10, TimeWindowInMs: 1000},
				},
			},
		},
	}
	p0Bytes, _ := json.Marshal(p0)

	globalKey := "aigw:policies:global"
	defer rdb.Del(ctx, globalKey)
	rdb.HSet(ctx, globalKey, "*", p0Bytes)

	_, err := svc.GetPolicy(ctx, "", userID, modelName)
	if err != nil {
		t.Fatalf("first GetPolicy failed: %v", err)
	}

	p0.LimitPolicies[0].SlidingWindows[0].Threshold = 99
	p0BytesNew, _ := json.Marshal(p0)
	rdb.HSet(ctx, globalKey, "*", p0BytesNew)

	p2, err := svc.GetPolicy(ctx, "", userID, modelName)
	if err != nil {
		t.Fatalf("second GetPolicy failed: %v", err)
	}

	if p2.LimitPolicies[0].SlidingWindows[0].Threshold != 10 {
		t.Errorf("expected cache hit with QPS=10, got QPS=%d", p2.LimitPolicies[0].SlidingWindows[0].Threshold)
	}

	cacheKey := ":" + userID + ":" + modelName
	svc.validCache.Remove(cacheKey)

	p3, err := svc.GetPolicy(ctx, "", userID, modelName)
	if err != nil {
		t.Fatalf("third GetPolicy failed: %v", err)
	}

	if p3.LimitPolicies[0].SlidingWindows[0].Threshold != 99 {
		t.Errorf("expected fresh fetch QPS=99, got QPS=%d", p3.LimitPolicies[0].SlidingWindows[0].Threshold)
	}
}

func TestPolicyService_NegativeCaching(t *testing.T) {
	rdb, logger := setupTestRedis(t)
	defer rdb.Close()

	svc := NewPolicyService(rdb, nil, nil, logger)
	ctx := context.Background()

	userID := "non_exist_user"
	modelName := "non_exist_model"

	globalKey := "aigw:policies:global"
	rdb.Del(ctx, globalKey)

	_, err1 := svc.GetPolicy(ctx, "", userID, modelName)
	if err1 == nil {
		t.Fatal("expected error, got nil")
	}

	rdb.Close()

	_, err2 := svc.GetPolicy(ctx, "", userID, modelName)
	if err2 == nil {
		t.Fatal("expected cached error from negative cache, got nil")
	}

	if err2.Error() != err1.Error() {
		t.Errorf("expected error '%s' from negative cache, got '%s'", err1.Error(), err2.Error())
	}
}

func TestPolicyService_LocalFallbackOnEmptyRedis(t *testing.T) {
	rdb, logger := setupTestRedis(t)
	defer rdb.Close()

	localPolicies := []*policy.Policy{
		{
			Permissions: []string{"local-only"},
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "qps-limit",
					Type: "request",
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 7, TimeWindowInMs: 1000},
					},
				},
			},
		},
	}

	svc := NewPolicyService(rdb, localPolicies, nil, logger)
	ctx := context.Background()

	rdb.Del(ctx, "aigw:policies:global")

	p, err := svc.GetPolicy(ctx, "", "u", "m")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(p.Permissions) == 0 || p.Permissions[0] != "local-only" {
		t.Errorf("expected permission 'local-only', got %v", p.Permissions)
	}
}

func TestPolicyService_GetPolicy_PermissionsAutoFill(t *testing.T) {
	rdb, logger := setupTestRedis(t)
	defer rdb.Close()

	svc := NewPolicyService(rdb, nil, nil, logger)
	ctx := context.Background()

	tenant := "test-tenant-jd"
	modelName := "gpt-4"

	// 模拟 Redis 中的策略：只包含 tagging 策略，permissions 字段为 nil
	p := &policy.Policy{
		TaggingPolicies: []*policy.TaggingPolicy{
			{
				Name: "JD-VIP",
			},
		},
	}
	pBytes, _ := json.Marshal(p)

	modelKey := "aigw:policies:model:" + modelName
	tenantModelsKey := "aigw:tenant:" + tenant + ":models"

	rdb.Del(ctx, modelKey, tenantModelsKey)
	defer rdb.Del(ctx, modelKey, tenantModelsKey)

	rdb.HSet(ctx, modelKey, "*", pBytes)
	rdb.SAdd(ctx, tenantModelsKey, "gpt-4", "claude-opus-4.7")

	merged, err := svc.GetPolicy(ctx, tenant, "", modelName)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证 Permissions 是否自动填充
	if len(merged.Permissions) != 2 {
		t.Errorf("expected 2 permissions, got %d: %v", len(merged.Permissions), merged.Permissions)
	}
	hasGPT4 := false
	hasClaude := false
	for _, perm := range merged.Permissions {
		if perm == "gpt-4" {
			hasGPT4 = true
		}
		if perm == "claude-opus-4.7" {
			hasClaude = true
		}
	}
	if !hasGPT4 || !hasClaude {
		t.Errorf("expected permissions to contain gpt-4 and claude-opus-4.7, got %v", merged.Permissions)
	}
}
