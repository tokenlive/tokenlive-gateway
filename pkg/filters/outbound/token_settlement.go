package outbound

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"

	"go.uber.org/zap"
)

// CreditsDeductor 积分扣减器接口（用于解耦和测试）
type CreditsDeductor interface {
	DeductCredits(ctx context.Context, apiKey string, credits int64) (int64, error)
}

// TokenSettlementFilter 在请求完成后结算预估 token 与实际 token 的差额，并扣减 Credits
type TokenSettlementFilter struct {
	stateStore      core.StateStore
	creditsDeductor CreditsDeductor
	logger          *zap.Logger
}

// NewTokenSettlementFilter 创建 TokenSettlementFilter
func NewTokenSettlementFilter(ss core.StateStore, cd CreditsDeductor, logger *zap.Logger) *TokenSettlementFilter {
	return &TokenSettlementFilter{
		stateStore:      ss,
		creditsDeductor: cd,
		logger:          logger,
	}
}

func (f *TokenSettlementFilter) Name() string                        { return "token_settlement" }
func (f *TokenSettlementFilter) Order() int                          { return 10 }
func (f *TokenSettlementFilter) Criticality() core.FilterCriticality { return core.Critical }

func (f *TokenSettlementFilter) OnResponse(gctx *core.GatewayContext) error {
	// 先解析费率，供配额扣减与费用结算共用
	inputPrice, outputPrice, cachedPrice, cacheCreationPrice := resolvePrices(gctx)

	// ===== Credits 扣减逻辑 =====
	// 只对个人用户（UserID != ""）且请求成功的场景进行额度扣减
	if gctx.UserID != "" && gctx.Err == nil && f.creditsDeductor != nil {
		// 计算实际费用
		costYuan := computeActualCost(gctx, inputPrice, cachedPrice, cacheCreationPrice, outputPrice)
		creditsToDeduct := int64(costYuan*1_000_000.0 + 0.5) // 四舍五入转换为微元

		if creditsToDeduct > 0 {
			// 扣减 Credits
			newCredits, err := f.creditsDeductor.DeductCredits(gctx.Ctx, gctx.APIKey, creditsToDeduct)
			if err != nil {
				// 扣减失败，记录错误并返回（触发补偿机制）
				if f.logger != nil {
					f.logger.Error("failed to deduct credits",
						zap.String("user_id", gctx.UserID),
						zap.String("api_key", gctx.APIKey[:8]+"..."),
						zap.Int64("credits", creditsToDeduct),
						zap.Error(err))
				}
				return err
			}

			// 记录扣减成功
			if f.logger != nil {
				f.logger.Debug("credits deducted successfully",
					zap.String("user_id", gctx.UserID),
					zap.Int64("credits", creditsToDeduct),
					zap.Int64("new_credits", newCredits))
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

		// 缺陷3修复：流式异常中断时 usage（通常在最后一帧）往往缺失，InputTokens 也为 0，
		// 导致 computeActualCost 仅算出输出费用、完全漏算输入费用（网关收入损失）。
		// 这里用与预估相同的口径从 RawBody 估算输入 token 并补齐，并标记为估算。
		// 注意：此场景无法知道缓存命中情况，按全部未命中（原价）估算，偏保守。
		if gctx.InputTokens == 0 && len(gctx.RawBody) > 0 {
			estimatedInput := int64(0)
			if gctx.Policy != nil {
				for _, lp := range gctx.Policy.LimitPolicies {
					if lp.Type == "token" || lp.Type == "cost" {
						estimatedInput = limiter.EstimateInputTokens(gctx, lp)
						break
					}
				}
			}
			if estimatedInput <= 0 {
				estimatedInput = limiter.EstimateInputTokens(gctx, nil)
			}
			if estimatedInput > 0 {
				gctx.InputTokens = int(estimatedInput)
				gctx.Tags["input_token_estimated"] = "true"
			}
		}
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

	// 统一定义并解析本请求的最终费率 (元/百万 Tokens)，已在 OnResponse 开头解析（供配额与费用共用）

	// 计算实际费用并赋给 gctx.Cost (包含缓存命中的支持)
	if gctx.Err == nil {
		gctx.Cost = computeActualCost(gctx, inputPrice, cachedPrice, cacheCreationPrice, outputPrice)
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
			// 缺陷2修复：预估费用需与实际费用口径对齐，避免缓存命中场景下预估系统性偏高。
			// 实际口径：缓存的命中/写入部分按 cachedPrice/cacheCreationPrice 计费。
			// 由于 OnResponse 时上游已返回 cached/cacheCreation（若有），预估应复用这些已知值，
			// 仅对「未知部分」做估算：
			//   - 输入：总输入未知 → 用 EstimateInputTokens 估算（此时它代表预估的总输入），
			//     但其中已知 cached/cacheCreation 部分应按各自单价计费，剩余按 inputPrice。
			//   - 输出：完全未知 → 用 EstimateOutputTokens (EMA) 估算，按 outputPrice。
			estimatedInputTokens := limiter.EstimateInputTokens(gctx, lp)
			estimatedOutputTokens := limiter.EstimateOutputTokens(context.Background(), f.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)

			// 在预估口径下，对 cached/cacheCreation 做与实际一致的拆分（口径统一）
			estCached := int64(gctx.CachedTokens)
			estCacheCreation := int64(gctx.CacheCreationTokens)
			if estCached < 0 {
				estCached = 0
			}
			if estCacheCreation < 0 {
				estCacheCreation = 0
			}
			if estCached+estCacheCreation > estimatedInputTokens {
				// 预估总输入不足以覆盖已知缓存部分，钳制（保守）
				if estCached > estimatedInputTokens {
					estCached = estimatedInputTokens
				}
				estCacheCreation = estimatedInputTokens - estCached
				if estCacheCreation < 0 {
					estCacheCreation = 0
				}
			}
			estNonCached := estimatedInputTokens - estCached - estCacheCreation

			// 预估费用 (厘)，与 actualCost 口径一致
			estimatedCost := int64((float64(estNonCached)*inputPrice +
				float64(estCached)*cachedPrice +
				float64(estCacheCreation)*cacheCreationPrice +
				float64(estimatedOutputTokens)*outputPrice) / 1000.0)

			// 实际费用：考虑缓存命中及创建的单价 (厘)，使用与 gctx.Cost 完全一致的计算口径
			// computeActualCost 返回元；1 元 = 1000 厘，故乘以 1000
			actualCost := int64(computeActualCost(gctx, inputPrice, cachedPrice, cacheCreationPrice, outputPrice) * 1000.0)

			diff = actualCost - estimatedCost
		}

		if diff == 0 {
			continue
		}

		// 必须复用入站预扣的 key 构造逻辑 (limiter.GetLimitKey)，否则两边操作的计数器不一致，
		// 会导致预扣的额度无法被结算/退还，限流形同虚设。
		limitKey := limiter.GetLimitKey(gctx, lp)
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

// computeActualCost 按四段单价计算一次请求的实际费用（单位：元）。
// 缓存读取/写入 token 仅是 InputTokens 的子集，需把命中/写入部分从原价输入中扣除，
// 剩余部分按 inputPrice 计费，避免重复计费。本函数不修改 gctx 状态，
// 仅对缓存 token 做防御性 clamp（保证 nonCached >= 0）。
func computeActualCost(gctx *core.GatewayContext, inputPrice, cachedPrice, cacheCreationPrice, outputPrice float64) float64 {
	cached, cacheCreation, nonCachedPrompt, output := clampAndSplitTokens(gctx)
	return (float64(nonCachedPrompt)*inputPrice +
		float64(cached)*cachedPrice +
		float64(cacheCreation)*cacheCreationPrice +
		float64(output)*outputPrice) / 1_000_000.0
}



// clampAndSplitTokens 是 computeActualCost / computeCostEquivalentTokens 的公共子例程：
// 对缓存 token 做防御性 clamp（不修改 gctx），并返回四段拆分后的 token 数。
// 返回顺序：cached, cacheCreation, nonCachedPrompt, output。
func clampAndSplitTokens(gctx *core.GatewayContext) (cached, cacheCreation, nonCachedPrompt, output int) {
	cached = gctx.CachedTokens
	cacheCreation = gctx.CacheCreationTokens
	if cached < 0 {
		cached = 0
	}
	if cacheCreation < 0 {
		cacheCreation = 0
	}
	// 防御性 clamp：缓存部分之和不得超过总输入；优先保留缓存读取（单价通常更低）
	if cached+cacheCreation > gctx.InputTokens {
		if cached > gctx.InputTokens {
			cached = gctx.InputTokens
		}
		cacheCreation = gctx.InputTokens - cached
		if cacheCreation < 0 {
			cacheCreation = 0
		}
	}
	nonCachedPrompt = gctx.InputTokens - cached - cacheCreation
	output = gctx.OutputTokens
	return
}

// resolvePrices 解析本请求最终的费率 (元/百万 Tokens)。
// 解析顺序（后者优先级更高）：默认兜底 → Policy.Billing（缺失缓存类单价时回退为 inputPrice）→ SelectedEndpoint 按字段覆盖。
func resolvePrices(gctx *core.GatewayContext) (inputPrice, outputPrice, cachedPrice, cacheCreationPrice float64) {
	inputPrice = 2.0 // 默认兜底价格 (元/百万 Tokens)
	outputPrice = 2.0
	cachedPrice = 2.0
	cacheCreationPrice = 2.0

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
	return
}
