package service

import (
	"context"
	"errors"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// PolicyService 负责按租户/用户维度懒加载、二级缓存管理，并调用 policy.PolicyMatcher 进行内存匹配合并
type PolicyService struct {
	provider      config.GatewayProvider
	logger        *log.Logger
	localPolicies []*policy.Policy                       // 本地 YAML 兜底规则（冷启动容灾）
	priorityChain []string                               // 自定义合并优先级链条
	validCache    *expirable.LRU[string, *policy.Policy] // 30秒过期二级正向缓存
	invalidCache  *expirable.LRU[string, string]         // 10秒过期二级负向缓存
}

func NewPolicyService(provider config.GatewayProvider, localPolicies []*policy.Policy, priorityChain []string, logger *log.Logger) *PolicyService {
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
		provider:      provider,
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

	// 3. 动态回源：通过统一的 provider 获取匹配的策略列表
	items, err := s.provider.GetPolicies(ctx, model, userID, tenantCode)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		if item.Value == nil {
			continue
		}
		switch {
		case userID != "" && item.Scope == "user:"+userID && item.Model == model:
			p5 = item.Value
		case userID != "" && item.Scope == "user:"+userID && item.Model == "*":
			p2 = item.Value
		case tenantCode != "" && item.Scope == "tenant:"+tenantCode && item.Model == model:
			p4 = item.Value
		case tenantCode != "" && item.Scope == "tenant:"+tenantCode && item.Model == "*":
			p1 = item.Value
		case (item.Scope == "model:"+model && item.Model == "*") || (item.Scope == "global" && item.Model == model):
			p3 = item.Value
		case item.Scope == "global" && item.Model == "*":
			p0 = item.Value
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

	// 如果没有在数据源里找到任何策略，则将本地静态列表全量作为候选进行 Match
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
		var models []string
		if userID != "" {
			models, _ = s.provider.GetUserModels(ctx, userID)
		}
		if len(models) == 0 && tenantCode != "" {
			models, _ = s.provider.GetTenantModels(ctx, tenantCode)
		}
		if len(models) > 0 {
			merged.Permissions = models
		}
		// 如果依然为空（如 ToC 模式，或者数据源不可用 / 未配置该租户模型集），则兜底为 "*"
		if len(merged.Permissions) == 0 {
			merged.Permissions = []string{"*"}
		}
	}

	// 5.8. 如果合并后没有配置 FallbackPolicy，尝试使用静态的全局 Fallbacks 配置
	if merged.InvocationPolicy == nil {
		merged.InvocationPolicy = &policy.InvocationPolicy{}
	}
	if merged.InvocationPolicy.FallbackPolicy == nil {
		if cfg, err := s.provider.GetConfig(ctx, model); err == nil && cfg != nil {
			if fbs, ok := cfg.Fallbacks[model]; ok && len(fbs) > 0 {
				merged.InvocationPolicy.FallbackPolicy = &policy.FallbackPolicy{
					Targets: fbs,
				}
			}
		}
	}

	// 6. 缓存结果到正向缓存中
	s.validCache.Add(cacheKey, merged)

	return merged, nil
}

// PurgeCache 清空本地 LRU 缓存以立即使新配置生效
func (s *PolicyService) PurgeCache() {
	s.validCache.Purge()
	s.invalidCache.Purge()
}
