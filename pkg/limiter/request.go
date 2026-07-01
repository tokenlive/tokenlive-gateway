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

// RequestLimitExecutor 针对请求数 (QPS/RPM) 的限流执行器
type RequestLimitExecutor struct {
	stateStore core.StateStore
}

// NewRequestLimitExecutor 创建 RequestLimitExecutor 实例
type TokenBucketLimiter interface {
	RateLimitTake(ctx context.Context, key string, tokens int64, limit int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error)
}

func NewRequestLimitExecutor(ss core.StateStore) *RequestLimitExecutor {
	return &RequestLimitExecutor{stateStore: ss}
}

func (e *RequestLimitExecutor) Execute(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	limitKey := getLimitKey(gctx, lp)
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
			allowed, remaining, err := e.stateStore.RateLimitTake(ctx, limitKey+":"+window.String(), 1, sw.Threshold, capacity, window, time.Now())
			if err != nil {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, 1)
				return err
			}
			if !allowed {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, 1)
				gctx.Logger(zap.L()).Warn("Rate limit enforced (request count exceeded via token bucket)",
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
				return &HTTPError{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"}
			}
		} else {
			current, err := e.stateStore.RateLimitIncr(ctx, limitKey+":"+window.String(), 1, window)
			if err != nil {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, 1)
				return err
			}
			if current > sw.Threshold {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i+1, 1)
				gctx.Logger(zap.L()).Warn("Rate limit enforced (request count exceeded)",
					zap.String("policy_name", lp.Name),
					zap.String("limit_type", lp.Type),
					zap.String("user_id", gctx.UserID),
					zap.String("tenant", gctx.Tenant),
					zap.String("model", gctx.Model),
					zap.String("window_size", window.String()),
					zap.Int64("threshold", sw.Threshold),
					zap.Int64("current", current),
					zap.Int64("request_tokens", 1),
				)
				return &HTTPError{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"}
			}
		}
	}
	return nil
}

func (e *RequestLimitExecutor) rollback(ctx context.Context, limitKey string, windows []*policy.SlidingWindow, count int, tokens int64) {
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

func (e *RequestLimitExecutor) Refund(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	limitKey := getLimitKey(gctx, lp)
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
			_, _, _ = e.stateStore.RateLimitTake(ctx, limitKey+":"+window.String(), -1, sw.Threshold, capacity, window, time.Now())
		} else {
			_ = e.stateStore.RateLimitRefund(ctx, limitKey+":"+window.String(), 1)
		}
	}
	return nil
}
