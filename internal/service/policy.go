package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/redis/go-redis/v9"
)

// PolicyService 负责按租户/用户维度懒加载、二级缓存管理，并调用 policy.PolicyMatcher 进行内存匹配合并
type PolicyService struct {
	rdb           *redis.Client
	logger        *log.Logger
	localPolicies []*policy.Policy                       // 本地 YAML 兜底规则（冷启动容灾）
	priorityChain []string                               // 自定义合并优先级链条
	validCache    *expirable.LRU[string, *policy.Policy] // 30秒过期二级正向缓存
	invalidCache  *expirable.LRU[string, string]         // 10秒过期二级负向缓存
}

func NewPolicyService(rdb *redis.Client, localPolicies []*policy.Policy, priorityChain []string, logger *log.Logger) *PolicyService {
	if localPolicies == nil {
		localPolicies = []*policy.Policy{}
	}
	if len(priorityChain) == 0 {
		priorityChain = []string{"global", "tenant", "user", "model", "tenant_model", "user_model"}
	}

	validCache := expirable.NewLRU[string, *policy.Policy](
		10000,
		func(key string, value *policy.Policy) {},
		30*time.Second,
	)

	invalidCache := expirable.NewLRU[string, string](
		5000,
		func(key string, value string) {},
		10*time.Second,
	)

	return &PolicyService{
		rdb:           rdb,
		logger:        logger,
		localPolicies: localPolicies,
		priorityChain: priorityChain,
		validCache:    validCache,
		invalidCache:  invalidCache,
	}
}

// GetPolicy 懒加载获取并合并出最终策略（满足 policy.PolicyProvider 接口契约）
func (s *PolicyService) GetPolicy(ctx context.Context, tenantCode, userID, model string) (*policy.Policy, error) {
	cacheKey := tenantCode + ":" + userID + ":" + model

	// 1. 查本地负向缓存，防止非法穿透
	if errMsg, ok := s.invalidCache.Get(cacheKey); ok {
		return nil, errors.New(errMsg)
	}

	// 2. 查本地二级缓存
	if p, ok := s.validCache.Get(cacheKey); ok {
		return p, nil
	}

	var p0, p1, p2, p3, p4, p5 *policy.Policy

	// 3. 动态回源：如果 Redis 启用，则从中聚合检索
	if s.rdb != nil {
		// (a) 如果有 userID，一次性查出 p5 (user_model) 和 p2 (user)
		if userID != "" {
			userHashKey := "aigw:policies:user:" + userID
			userFields, err := s.rdb.HGetAll(ctx, userHashKey).Result()
			if err == nil && len(userFields) > 0 {
				// 解析 Level 5: user_model
				if p5Str, ok := userFields[model]; ok {
					var temp policy.Policy
					if json.Unmarshal([]byte(p5Str), &temp) == nil {
						p5 = &temp
					}
				}
				// 解析 Level 2: user
				if p2Str, ok := userFields["*"]; ok {
					var temp policy.Policy
					if json.Unmarshal([]byte(p2Str), &temp) == nil {
						p2 = &temp
					}
				}
			}
		}

		// (b) 如果有 tenantCode，一次性查出 p4 (tenant_model) 和 p1 (tenant)
		if tenantCode != "" {
			tenantHashKey := "aigw:policies:tenant:" + tenantCode
			tenantFields, err := s.rdb.HGetAll(ctx, tenantHashKey).Result()
			if err == nil && len(tenantFields) > 0 {
				// 解析 Level 4: tenant_model
				if p4Str, ok := tenantFields[model]; ok {
					var temp policy.Policy
					if json.Unmarshal([]byte(p4Str), &temp) == nil {
						p4 = &temp
					}
				}
				// 解析 Level 1: tenant
				if p1Str, ok := tenantFields["*"]; ok {
					var temp policy.Policy
					if json.Unmarshal([]byte(p1Str), &temp) == nil {
						p1 = &temp
					}
				}
			}
		}

		// (c) 获取模型的公共通配规则（p3: model）
		modelHashKey := "aigw:policies:model:" + model
		p3Str, err := s.rdb.HGet(ctx, modelHashKey, "*").Result()
		if err == nil && p3Str != "" {
			var temp policy.Policy
			if json.Unmarshal([]byte(p3Str), &temp) == nil {
				p3 = &temp
			}
		}

		// (d) 获取全局最终通配规则（p0: global）
		p0Str, err := s.rdb.HGet(ctx, "aigw:policies:global", "*").Result()
		if err == nil && p0Str != "" {
			var temp policy.Policy
			if json.Unmarshal([]byte(p0Str), &temp) == nil {
				p0 = &temp
			}
		}
	}

	// 4. 自定义优先级合并覆盖
	policyMap := map[string]*policy.Policy{
		"global":       p0,
		"tenant":       p1,
		"user":         p2,
		"model":        p3,
		"tenant_model": p4,
		"user_model":   p5,
	}

	var candidates []*policy.Policy
	for _, dim := range s.priorityChain {
		if p, ok := policyMap[dim]; ok && p != nil {
			candidates = append(candidates, p)
		}
	}

	// 如果没有在 Redis 里找到任何策略，则将本地静态列表全量作为候选进行 Match
	if len(candidates) == 0 {
		candidates = s.localPolicies
	}

	// 5. 调用单例 matcher 进行多维规则合并（自底向上覆盖）
	merged, err := policy.DefaultPolicyMatcher.Match(tenantCode, userID, model, candidates)
	if err != nil {
		// 写入负向缓存，防止穿透
		s.invalidCache.Add(cacheKey, err.Error())
		return nil, err
	}

	// 5.5. 补充授权模型列表到 Permissions 中，防止 AuthFilter 误阻断
	if len(merged.Permissions) == 0 {
		if s.rdb != nil {
			var models []string
			if userID != "" {
				userKey := "aigw:user:" + userID + ":models"
				models, _ = s.rdb.SMembers(ctx, userKey).Result()
			}
			if len(models) == 0 && tenantCode != "" {
				tenantKey := "aigw:tenant:" + tenantCode + ":models"
				models, _ = s.rdb.SMembers(ctx, tenantKey).Result()
			}
			if len(models) > 0 {
				merged.Permissions = models
			}
		}
		// 如果依然为空（如 ToC 模式，或者 Redis 不可用 / 未配置该租户模型集），则兜底为 "*"
		if len(merged.Permissions) == 0 {
			merged.Permissions = []string{"*"}
		}
	}

	// 6. 缓存结果到正向缓存中
	s.validCache.Add(cacheKey, merged)

	return merged, nil
}
