package outbound

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"

	"go.uber.org/zap"
)

// QuotaDeductor 配额扣减器接口（用于解耦和测试）
type QuotaDeductor interface {
	DeductQuota(ctx context.Context, apiKey string, tokens int64) (int64, error)
}

// TokenSettlementFilter 在请求完成后结算预估 token 与实际 token 的差额
type TokenSettlementFilter struct {
	stateStore    core.StateStore
	quotaDeductor QuotaDeductor
	logger        *zap.Logger
}

// NewTokenSettlementFilter 创建 TokenSettlementFilter
func NewTokenSettlementFilter(ss core.StateStore, qd QuotaDeductor, logger *zap.Logger) *TokenSettlementFilter {
	return &TokenSettlementFilter{
		stateStore:    ss,
		quotaDeductor: qd,
		logger:        logger,
	}
}

func (f *TokenSettlementFilter) Name() string                        { return "token_settlement" }
func (f *TokenSettlementFilter) Order() int                          { return 10 }
func (f *TokenSettlementFilter) Criticality() core.FilterCriticality { return core.Critical }

func (f *TokenSettlementFilter) OnResponse(gctx *core.GatewayContext) error {
	// ===== 配额扣减逻辑 =====
	// 只对个人用户（UserID != ""）且请求成功的场景进行配额扣减
	if gctx.UserID != "" && gctx.Err == nil && f.quotaDeductor != nil {
		// 计算总 Token 数（输入 + 输出）
		totalTokens := int64(gctx.InputTokens + gctx.OutputTokens)

		if totalTokens > 0 {
			// 扣减配额
			newQuota, err := f.quotaDeductor.DeductQuota(gctx.Ctx, gctx.APIKey, totalTokens)
			if err != nil {
				// 配额扣减失败，记录错误并返回（触发补偿机制）
				if f.logger != nil {
					f.logger.Error("failed to deduct quota",
						zap.String("user_id", gctx.UserID),
						zap.String("api_key", gctx.APIKey[:8]+"..."),
						zap.Int64("tokens", totalTokens),
						zap.Error(err))
				}
				return err
			}

			// 记录配额扣减成功
			if f.logger != nil {
				f.logger.Debug("quota deducted successfully",
					zap.String("user_id", gctx.UserID),
					zap.Int64("tokens", totalTokens),
					zap.Int64("new_quota", newQuota))
			}
		}
	}

	// ===== 原有的 Token 结算逻辑 =====
	// 针对流式传输异常中断（缺失 usage）启用字数估算降级结算
	if gctx.IsStream && gctx.OutputTokens == 0 && gctx.TransmittedChars > 0 {
		ratio := 0.6 // 默认值
		if gctx.Policy != nil {
			for _, lp := range gctx.Policy.LimitPolicies {
				if lp.Estimator != nil && lp.Estimator.Type == "length_ratio" && lp.Estimator.Ratio > 0 {
					ratio = lp.Estimator.Ratio
					break
				}
			}
		}
		gctx.OutputTokens = int(float64(gctx.TransmittedChars) * ratio)
		if gctx.OutputTokens <= 0 {
			gctx.OutputTokens = 1
		}
		if gctx.Tags == nil {
			gctx.Tags = make(map[string]string)
		}
		gctx.Tags["completion_token_estimated"] = "true"
	}

	// 异步更新 EMA 估算值
	if gctx.OutputTokens > 0 {
		tenantKey := gctx.Tenant
		if tenantKey == "" {
			tenantKey = gctx.UserID
		}
		model := gctx.Model
		completion := int64(gctx.OutputTokens)
		go func() {
			ctx := context.Background()
			// 1. 更新租户级特定模型的 EMA
			if tenantKey != "" {
				_, _ = f.stateStore.UpdateEMA(ctx, "tenant:"+tenantKey+":"+model, completion, 0.1)
			}
			// 2. 更新模型全局级的 EMA
			_, _ = f.stateStore.UpdateEMA(ctx, "model:global:"+model, completion, 0.1)
		}()
	}

	// 统一定义并解析本请求的最终费率 (元/百万 Tokens)
	var (
		inputPrice         = 2.0 // 默认兜底价格 (元/百万 Tokens)
		outputPrice        = 2.0
		cachedPrice        = 2.0
		cacheCreationPrice = 2.0
	)

	// 1. 回退继承模型级别策略
	if gctx.Policy != nil && gctx.Policy.Billing != nil {
		inputPrice = gctx.Policy.Billing.InputPrice
		outputPrice = gctx.Policy.Billing.OutputPrice
		if gctx.Policy.Billing.CachedPrice > 0 {
			cachedPrice = gctx.Policy.Billing.CachedPrice
		} else {
			cachedPrice = inputPrice
		}
		if gctx.Policy.Billing.CacheCreationPrice > 0 {
			cacheCreationPrice = gctx.Policy.Billing.CacheCreationPrice
		} else {
			cacheCreationPrice = inputPrice
		}
	}

	// 2. 覆盖 Endpoint 级别配置价格 (最高优先级)
	if gctx.SelectedEndpoint != nil {
		if gctx.SelectedEndpoint.InputPrice != nil {
			inputPrice = *gctx.SelectedEndpoint.InputPrice
		}
		if gctx.SelectedEndpoint.OutputPrice != nil {
			outputPrice = *gctx.SelectedEndpoint.OutputPrice
		}
		if gctx.SelectedEndpoint.CachedPrice != nil {
			cachedPrice = *gctx.SelectedEndpoint.CachedPrice
		}
		if gctx.SelectedEndpoint.CacheCreationPrice != nil {
			cacheCreationPrice = *gctx.SelectedEndpoint.CacheCreationPrice
		}
	}

	// 计算实际费用并赋给 gctx.Cost (包含缓存命中的支持)
	if gctx.Err == nil {
		if gctx.CachedTokens+gctx.CacheCreationTokens > gctx.InputTokens {
			gctx.CachedTokens = gctx.InputTokens
			gctx.CacheCreationTokens = 0
		}

		cachedTokens := gctx.CachedTokens
		cacheCreationTokens := gctx.CacheCreationTokens
		nonCachedPromptTokens := gctx.InputTokens - cachedTokens - cacheCreationTokens

		gctx.Cost = (float64(nonCachedPromptTokens)*inputPrice +
			float64(cachedTokens)*cachedPrice +
			float64(cacheCreationTokens)*cacheCreationPrice +
			float64(gctx.OutputTokens)*outputPrice) / 1_000_000.0
	}

	policy := gctx.Policy
	if policy == nil || len(policy.LimitPolicies) == 0 {
		return nil
	}

	for _, lp := range policy.LimitPolicies {
		if lp.Type != "token" && lp.Type != "cost" {
			continue
		}
		// 自治条件判断
		if !inbound.MatchLimitPolicyConditions(gctx, lp) {
			continue // 条件不匹配，跳过本条限流策略的结算
		}

		var diff int64

		if lp.Type == "token" {
			actual := int64(gctx.InputTokens + gctx.OutputTokens)
			estimated := limiter.EstimateInputTokens(gctx, lp) + limiter.EstimateOutputTokens(context.Background(), f.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
			diff = actual - estimated
		} else if lp.Type == "cost" {
			// 分别估算 input/output token 数，使用各自单价计算预估费用 (厘)
			estimatedInputTokens := limiter.EstimateInputTokens(gctx, lp)
			estimatedOutputTokens := limiter.EstimateOutputTokens(context.Background(), f.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
			estimatedCost := int64((float64(estimatedInputTokens)*inputPrice +
				float64(estimatedOutputTokens)*outputPrice) / 1000.0)

			// 实际费用：考虑缓存命中及创建的单价 (厘)
			cachedTokens := gctx.CachedTokens
			cacheCreationTokens := gctx.CacheCreationTokens
			if cachedTokens+cacheCreationTokens > gctx.InputTokens {
				cachedTokens = gctx.InputTokens
				cacheCreationTokens = 0
			}
			nonCachedPromptTokens := gctx.InputTokens - cachedTokens - cacheCreationTokens

			actualCost := int64((float64(nonCachedPromptTokens)*inputPrice +
				float64(cachedTokens)*cachedPrice +
				float64(cacheCreationTokens)*cacheCreationPrice +
				float64(gctx.OutputTokens)*outputPrice) / 1000.0)

			diff = actualCost - estimatedCost
		}

		if diff == 0 {
			continue
		}

		id := gctx.UserID
		if id == "" {
			id = gctx.Tenant
		}
		policyKey := lp.ID
		if policyKey == "" {
			policyKey = lp.Name
		}
		limitKey := id + ":" + gctx.Model + ":" + policyKey
		for _, sw := range lp.SlidingWindows {
			window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
			if window <= 0 {
				window = time.Minute
			}
			windowKey := limitKey + ":" + window.String()

			if diff < 0 {
				// 预估多了，退还差额 (注意 diff 是负数，退还的值是正数)
				if err := f.stateStore.RateLimitRefund(context.Background(), windowKey, -diff); err != nil {
					return err
				}
			} else {
				// 预估少了，追加扣减
				if _, err := f.stateStore.RateLimitIncr(context.Background(), windowKey, diff, window); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
