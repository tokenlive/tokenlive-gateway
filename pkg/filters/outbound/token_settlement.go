package outbound

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"

	"go.uber.org/zap"
)

// CreditsDeductor deducts user credits (for decoupling and tests).
type CreditsDeductor interface {
	DeductCredits(ctx context.Context, apiKey string, credits int64) (int64, error)
}

// TokenSettlementFilter settles estimated vs actual tokens and deducts credits after a request.
type TokenSettlementFilter struct {
	stateStore      core.StateStore
	creditsDeductor CreditsDeductor
	logger          *zap.Logger
}

// NewTokenSettlementFilter creates a TokenSettlementFilter.
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
	// Resolve rates once for quota and cost settlement
	inputPrice, outputPrice, cachedPrice, cacheCreationPrice := resolvePrices(gctx)

	// Credits: personal users (UserID != "") on successful requests only
	if gctx.UserID != "" && gctx.Err == nil && f.creditsDeductor != nil {
		costYuan := computeActualCost(gctx, inputPrice, cachedPrice, cacheCreationPrice, outputPrice)
		creditsToDeduct := int64(costYuan*1_000_000.0 + 0.5) // round to micro-yuan

		if creditsToDeduct > 0 {
			newCredits, err := f.creditsDeductor.DeductCredits(gctx.Ctx, gctx.APIKey, creditsToDeduct)
			if err != nil {
				// Return error to trigger compensation
				if f.logger != nil {
					f.logger.Error("failed to deduct credits",
						zap.String("user_id", gctx.UserID),
						zap.String("api_key", gctx.APIKey[:8]+"..."),
						zap.Int64("credits", creditsToDeduct),
						zap.Error(err))
				}
				return err
			}

			if f.logger != nil {
				f.logger.Debug("credits deducted successfully",
					zap.String("user_id", gctx.UserID),
					zap.Int64("credits", creditsToDeduct),
					zap.Int64("new_credits", newCredits))
			}
		}
	}

	// Stream aborted without usage: estimate tokens from transmitted chars
	if gctx.IsStream && gctx.OutputTokens == 0 && gctx.TransmittedChars > 0 {
		ratio := 0.6 // default
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

		// Stream abort often drops usage (last SSE frame); InputTokens may be 0 so cost
		// would undercount input. Estimate input from RawBody with the same estimator
		// as pre-charge; cache hits unknown → bill as full-price (conservative).
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

	// Async EMA update for output-token estimates
	if gctx.OutputTokens > 0 {
		tenantKey := gctx.Tenant
		if tenantKey == "" {
			tenantKey = gctx.UserID
		}
		model := gctx.Model
		completion := int64(gctx.OutputTokens)
		go func() {
			ctx := context.Background()
			if tenantKey != "" {
				_, _ = f.stateStore.UpdateEMA(ctx, "tenant:"+tenantKey+":"+model, completion, 0.1)
			}
			_, _ = f.stateStore.UpdateEMA(ctx, "model:global:"+model, completion, 0.1)
		}()
	}

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
		if !inbound.MatchLimitPolicyConditions(gctx, lp) {
			continue
		}

		var diff int64

		if lp.Type == "token" {
			actual := int64(gctx.InputTokens + gctx.OutputTokens)
			estimated := limiter.EstimateInputTokens(gctx, lp) + limiter.EstimateOutputTokens(context.Background(), f.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
			diff = actual - estimated
		} else if lp.Type == "cost" {
			// Align estimated cost with actual: bill known cached/cacheCreation at their
			// rates; estimate only the unknown remainder (input via EstimateInputTokens,
			// output via EMA). Avoids systematically high pre-charge on cache hits.
			estimatedInputTokens := limiter.EstimateInputTokens(gctx, lp)
			estimatedOutputTokens := limiter.EstimateOutputTokens(context.Background(), f.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)

			estCached := int64(gctx.CachedTokens)
			estCacheCreation := int64(gctx.CacheCreationTokens)
			if estCached < 0 {
				estCached = 0
			}
			if estCacheCreation < 0 {
				estCacheCreation = 0
			}
			if estCached+estCacheCreation > estimatedInputTokens {
				// Clamp when estimate cannot cover known cache portions
				if estCached > estimatedInputTokens {
					estCached = estimatedInputTokens
				}
				estCacheCreation = estimatedInputTokens - estCached
				if estCacheCreation < 0 {
					estCacheCreation = 0
				}
			}
			estNonCached := estimatedInputTokens - estCached - estCacheCreation

			// Estimated cost in li (1 CNY = 1000 li), same basis as actualCost
			estimatedCost := int64((float64(estNonCached)*inputPrice +
				float64(estCached)*cachedPrice +
				float64(estCacheCreation)*cacheCreationPrice +
				float64(estimatedOutputTokens)*outputPrice) / 1000.0)

			// computeActualCost returns CNY; convert to li
			actualCost := int64(computeActualCost(gctx, inputPrice, cachedPrice, cacheCreationPrice, outputPrice) * 1000.0)

			diff = actualCost - estimatedCost
		}

		if diff == 0 {
			continue
		}

		// Must reuse inbound key construction (limiter.GetLimitKey) so settle/refund
		// hits the same counters as pre-charge.
		limitKey := limiter.GetLimitKey(gctx, lp)
		for _, sw := range lp.SlidingWindows {
			window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
			if window <= 0 {
				window = time.Minute
			}
			windowKey := limitKey + ":" + window.String()

			if diff < 0 {
				// Over-estimated: refund |diff|
				if err := f.stateStore.RateLimitRefund(context.Background(), windowKey, -diff); err != nil {
					return err
				}
			} else {
				// Under-estimated: extra deduct
				if _, err := f.stateStore.RateLimitIncr(context.Background(), windowKey, diff, window); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// computeActualCost bills one request with four unit prices (CNY).
// Cached read/write tokens are subsets of InputTokens; they are subtracted from
// full-price input to avoid double billing. Does not mutate gctx; clamps cache
// tokens so nonCached >= 0.
func computeActualCost(gctx *core.GatewayContext, inputPrice, cachedPrice, cacheCreationPrice, outputPrice float64) float64 {
	cached, cacheCreation, nonCachedPrompt, output := clampAndSplitTokens(gctx)
	return (float64(nonCachedPrompt)*inputPrice +
		float64(cached)*cachedPrice +
		float64(cacheCreation)*cacheCreationPrice +
		float64(output)*outputPrice) / 1_000_000.0
}

// clampAndSplitTokens defensively clamps cache tokens (no gctx mutation) and
// returns cached, cacheCreation, nonCachedPrompt, output.
func clampAndSplitTokens(gctx *core.GatewayContext) (cached, cacheCreation, nonCachedPrompt, output int) {
	cached = gctx.CachedTokens
	cacheCreation = gctx.CacheCreationTokens
	if cached < 0 {
		cached = 0
	}
	if cacheCreation < 0 {
		cacheCreation = 0
	}
	// Prefer keeping cache-read tokens (usually cheaper) when sum exceeds input
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

// resolvePrices returns CNY per million tokens.
// Priority (later wins): defaults → Policy.Billing (cache prices fall back to
// inputPrice) → SelectedEndpoint field overrides.
func resolvePrices(gctx *core.GatewayContext) (inputPrice, outputPrice, cachedPrice, cacheCreationPrice float64) {
	inputPrice = 2.0 // default CNY / 1M tokens
	outputPrice = 2.0
	cachedPrice = 2.0
	cacheCreationPrice = 2.0

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
