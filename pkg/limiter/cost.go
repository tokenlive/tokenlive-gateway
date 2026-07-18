package limiter

import (
	"context"
	"math"
	"net/http"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// CostLimitExecutor enforces spend limits (unit: li, 1/1000 yuan).
type CostLimitExecutor struct {
	stateStore core.StateStore
}

// NewCostLimitExecutor creates a CostLimitExecutor.
func NewCostLimitExecutor(ss core.StateStore) *CostLimitExecutor {
	return &CostLimitExecutor{stateStore: ss}
}

func (e *CostLimitExecutor) Execute(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	limitKey := GetLimitKey(gctx, lp)

	inputPrice := 3.0 // default yuan per million tokens
	outputPrice := 10.0
	if gctx.Policy != nil && gctx.Policy.Billing != nil {
		inputPrice = gctx.Policy.Billing.InputPrice
		outputPrice = gctx.Policy.Billing.OutputPrice
	}

	// Estimate cost in li from input/output tokens and unit prices.
	estimateInputTokens := EstimateInputTokens(gctx, lp)
	estimateOutputTokens := EstimateOutputTokens(ctx, e.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
	estimateCost := int64((float64(estimateInputTokens)*inputPrice +
		float64(estimateOutputTokens)*outputPrice) / 1000.0)

	for i, sw := range lp.SlidingWindows {
		window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
		if window <= 0 {
			window = time.Minute
		}

		if sw.BurstRatio != nil && *sw.BurstRatio > 0 {
			burstCoeff := *sw.BurstRatio
			capacity := int64(math.Ceil(float64(sw.Threshold) * burstCoeff))
			if capacity < 1 {
				capacity = 1
			}
			allowed, remaining, err := e.stateStore.RateLimitTake(ctx, limitKey+":"+window.String(), estimateCost, sw.Threshold, capacity, window, time.Now())
			if err != nil {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, estimateCost)
				return err
			}
			if !allowed {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, estimateCost)
				gctx.Logger(zap.L()).Warn("Rate limit enforced (cost budget exceeded via token bucket)",
					zap.String("policy_name", lp.Name),
					zap.String("limit_type", lp.Type),
					zap.String("user_id", gctx.UserID),
					zap.String("tenant", gctx.Tenant),
					zap.String("model", gctx.Model),
					zap.String("window_size", window.String()),
					zap.Int64("threshold", sw.Threshold),
					zap.Int64("capacity", capacity),
					zap.Float64("burst_ratio", burstCoeff),
					zap.Int64("remaining", remaining),
				)
				return &HTTPError{
					Code:         http.StatusTooManyRequests,
					Message:      "cost limit exceeded (daily budget blown)",
					Threshold:    float64Ptr(float64(sw.Threshold)),
					CurrentValue: float64Ptr(float64(capacity - remaining)),
				}
			}
		} else {
			current, err := e.stateStore.RateLimitIncr(ctx, limitKey+":"+window.String(), estimateCost, window)
			if err != nil {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, estimateCost)
				return err
			}

			if current > sw.Threshold {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i+1, estimateCost)
				gctx.Logger(zap.L()).Warn("Rate limit enforced (cost budget exceeded)",
					zap.String("policy_name", lp.Name),
					zap.String("limit_type", lp.Type),
					zap.String("user_id", gctx.UserID),
					zap.String("tenant", gctx.Tenant),
					zap.String("model", gctx.Model),
					zap.String("window_size", window.String()),
					zap.Int64("threshold", sw.Threshold),
					zap.Int64("current", current),
					zap.Int64("estimated_request_cost_li", estimateCost),
				)
				return &HTTPError{
					Code:         http.StatusTooManyRequests,
					Message:      "cost limit exceeded (daily budget blown)",
					Threshold:    float64Ptr(float64(sw.Threshold)),
					CurrentValue: float64Ptr(float64(current)),
				}
			}
		}
	}
	return nil
}

func (e *CostLimitExecutor) rollback(ctx context.Context, limitKey string, windows []*policy.SlidingWindow, count int, tokens int64) {
	for j := 0; j < count; j++ {
		sw := windows[j]
		window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
		if window <= 0 {
			window = time.Minute
		}
		if sw.BurstRatio != nil && *sw.BurstRatio > 0 {
			burstCoeff := *sw.BurstRatio
			capacity := int64(math.Ceil(float64(sw.Threshold) * burstCoeff))
			if capacity < 1 {
				capacity = 1
			}
			_, _, _ = e.stateStore.RateLimitTake(ctx, limitKey+":"+window.String(), -tokens, sw.Threshold, capacity, window, time.Now())
		} else {
			_ = e.stateStore.RateLimitRefund(ctx, limitKey+":"+window.String(), tokens)
		}
	}
}

func (e *CostLimitExecutor) Refund(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	limitKey := GetLimitKey(gctx, lp)

	inputPrice := 3.0 // default yuan per million tokens
	outputPrice := 10.0
	if gctx.Policy != nil && gctx.Policy.Billing != nil {
		inputPrice = gctx.Policy.Billing.InputPrice
		outputPrice = gctx.Policy.Billing.OutputPrice
	}

	estimateInputTokens := EstimateInputTokens(gctx, lp)
	estimateOutputTokens := EstimateOutputTokens(ctx, e.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
	estimateCost := int64((float64(estimateInputTokens)*inputPrice +
		float64(estimateOutputTokens)*outputPrice) / 1000.0)

	for _, sw := range lp.SlidingWindows {
		window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
		if window <= 0 {
			window = time.Minute
		}
		if sw.BurstRatio != nil && *sw.BurstRatio > 0 {
			burstCoeff := *sw.BurstRatio
			capacity := int64(math.Ceil(float64(sw.Threshold) * burstCoeff))
			if capacity < 1 {
				capacity = 1
			}
			_, _, _ = e.stateStore.RateLimitTake(ctx, limitKey+":"+window.String(), -estimateCost, sw.Threshold, capacity, window, time.Now())
		} else {
			_ = e.stateStore.RateLimitRefund(ctx, limitKey+":"+window.String(), estimateCost)
		}
	}
	return nil
}
