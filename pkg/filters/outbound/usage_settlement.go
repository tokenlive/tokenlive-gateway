package outbound

import (
	"errors"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"
	"github.com/tokenlive/tokenlive-gateway/pkg/usage"
)

// usageSettlement is the single response snapshot consumed by settlement effects.
// Prices and cost are resolved once, after missing stream usage is backfilled.
type usageSettlement struct {
	tokens   usage.Tokens
	prices   settlementPrices
	costYuan float64
	billable bool
}

type settlementPrices struct {
	input, output, cached, creation float64
}

func finalizeUsage(gctx *core.GatewayContext) usageSettlement {
	backfillStreamUsage(gctx)
	settled := snapshotUsage(gctx)
	if settled.billable {
		gctx.Cost = settled.costYuan
	}
	return settled
}

// snapshotUsage also serves quota-only child settlement, which must not estimate
// usage, change Cost, charge personal credits, or train the output EMA.
func snapshotUsage(gctx *core.GatewayContext) usageSettlement {
	var prices settlementPrices
	prices.input, prices.output, prices.cached, prices.creation = resolvePrices(gctx)
	tokens := gctx.Usage()
	return usageSettlement{
		tokens: tokens, prices: prices, costYuan: prices.cost(tokens),
		billable: gctx.Err == nil || errors.Is(gctx.Err, core.ErrClientDisconnected),
	}
}

func backfillStreamUsage(gctx *core.GatewayContext) {
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
}

// cost bills one request with four unit prices (CNY).
// Cached read/write tokens are subsets of InputTokens; they are subtracted from
// full-price input to avoid double billing. Does not mutate the snapshot; clamps cache
// tokens so nonCached >= 0.
func (p settlementPrices) cost(tokens usage.Tokens) float64 {
	cached, cacheCreation, nonCachedPrompt, output := clampAndSplitTokens(tokens)
	return (float64(nonCachedPrompt)*p.input +
		float64(cached)*p.cached +
		float64(cacheCreation)*p.creation +
		float64(output)*p.output) / 1_000_000.0
}

// clampAndSplitTokens defensively clamps cache tokens (no snapshot mutation) and
// returns cached, cacheCreation, nonCachedPrompt, output.
func clampAndSplitTokens(tokens usage.Tokens) (cached, cacheCreation, nonCachedPrompt, output int) {
	cached = tokens.CachedTokens
	cacheCreation = tokens.CacheCreationTokens
	if cached < 0 {
		cached = 0
	}
	if cacheCreation < 0 {
		cacheCreation = 0
	}
	// Prefer keeping cache-read tokens (usually cheaper) when sum exceeds input
	if cached+cacheCreation > tokens.InputTokens {
		if cached > tokens.InputTokens {
			cached = tokens.InputTokens
		}
		cacheCreation = tokens.InputTokens - cached
		if cacheCreation < 0 {
			cacheCreation = 0
		}
	}
	nonCachedPrompt = tokens.InputTokens - cached - cacheCreation
	output = tokens.OutputTokens
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

	// Composite answers are billed at the business entry price. The selected
	// supplier endpoint is retained for observability and child quota settlement.
	composite := gctx.SmartRouting != nil && (gctx.SmartRouting.Mode == "priority" || gctx.SmartRouting.Mode == "rules")
	if gctx.SelectedEndpoint != nil && !composite {
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
