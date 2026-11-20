package outbound

import (
	"context"
	"errors"
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
	settled := finalizeUsage(gctx)
	clientDisconnected := errors.Is(gctx.Err, core.ErrClientDisconnected)
	settlementCtx := gctx.Ctx
	if settlementCtx == nil {
		settlementCtx = context.Background()
	} else if clientDisconnected {
		settlementCtx = context.WithoutCancel(settlementCtx)
	}

	// Credits: settle successful requests and client-aborted streams with finalized usage.
	if gctx.UserID != "" && settled.billable && f.creditsDeductor != nil {
		creditsToDeduct := int64(settled.costYuan*1_000_000.0 + 0.5) // round to micro-yuan

		if creditsToDeduct > 0 {
			newCredits, err := f.creditsDeductor.DeductCredits(settlementCtx, gctx.APIKey, creditsToDeduct)
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

	var err error
	if gctx.TrackLimitReservations {
		err = f.settleReservations(gctx, settled)
	} else {
		err = f.settleLegacyLimits(gctx, settled)
	}
	// Read all quota estimates before publishing this response's output sample.
	f.updateOutputEMA(gctx, settled.tokens.OutputTokens)
	return err
}

func (f *TokenSettlementFilter) updateOutputEMA(gctx *core.GatewayContext, outputTokens int) {
	if outputTokens > 0 {
		tenantKey := gctx.Tenant
		if tenantKey == "" {
			tenantKey = gctx.UserID
		}
		model := gctx.Model
		completion := int64(outputTokens)
		go func() {
			ctx := context.Background()
			if tenantKey != "" {
				_, _ = f.stateStore.UpdateEMA(ctx, "tenant:"+tenantKey+":"+model, completion, 0.1)
			}
			_, _ = f.stateStore.UpdateEMA(ctx, "model:global:"+model, completion, 0.1)
		}()
	}
}

func (f *TokenSettlementFilter) settleLegacyLimits(gctx *core.GatewayContext, settled usageSettlement) error {
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
			actual := int64(settled.tokens.InputTokens + settled.tokens.OutputTokens)
			estimated := limiter.EstimateInputTokens(gctx, lp) + limiter.EstimateOutputTokens(context.Background(), f.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
			diff = actual - estimated
		} else if lp.Type == "cost" {
			// Align estimated cost with actual: bill known cached/cacheCreation at their
			// rates; estimate only the unknown remainder (input via EstimateInputTokens,
			// output via EMA). Avoids systematically high pre-charge on cache hits.
			estimatedInputTokens := limiter.EstimateInputTokens(gctx, lp)
			estimatedOutputTokens := limiter.EstimateOutputTokens(context.Background(), f.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)

			estCached := int64(settled.tokens.CachedTokens)
			estCacheCreation := int64(settled.tokens.CacheCreationTokens)
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
			estimatedCost := int64((float64(estNonCached)*settled.prices.input +
				float64(estCached)*settled.prices.cached +
				float64(estCacheCreation)*settled.prices.creation +
				float64(estimatedOutputTokens)*settled.prices.output) / 1000.0)

			// The finalized cost is CNY; convert to li
			actualCost := int64(settled.costYuan * 1000.0)

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

// SettleLimits reconciles smart-call quotas without charging user credits.
// Each entry records its exact bucket and estimate at precharge time.
func (f *TokenSettlementFilter) SettleLimits(gctx *core.GatewayContext) error {
	return f.settleReservations(gctx, snapshotUsage(gctx))
}

func (f *TokenSettlementFilter) settleReservations(gctx *core.GatewayContext, settled usageSettlement) error {
	ctx := gctx.Ctx
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	for i := range gctx.LimitReservations {
		r := &gctx.LimitReservations[i]
		if r.Settled {
			continue
		}
		var actual int64
		if settled.billable {
			if r.Type == "token" {
				actual = int64(settled.tokens.InputTokens + settled.tokens.OutputTokens)
			} else {
				actual = int64(settled.costYuan * 1000)
			}
		}
		diff := actual - r.Estimated
		var err error
		if diff != 0 {
			if r.Burst {
				// 用量已经发生，不能按准入失败拒绝；正差额允许形成债务。
				_, err = f.stateStore.RateLimitAdjust(ctx, r.Key, diff, r.Rate, r.Capacity, r.Window, time.Now())
			} else if diff < 0 {
				err = f.stateStore.RateLimitRefund(ctx, r.Key, -diff)
			} else {
				_, err = f.stateStore.RateLimitIncr(ctx, r.Key, diff, r.Window)
			}
		}
		if err != nil {
			return err
		}
		r.Settled = true
	}
	return nil
}
