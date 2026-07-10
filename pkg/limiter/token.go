package limiter

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"github.com/pkoukk/tiktoken-go"
	"go.uber.org/zap"
)

// TokenLimitExecutor 针对 Token (TPM) 的限流执行器
type TokenLimitExecutor struct {
	stateStore core.StateStore
}

// NewTokenLimitExecutor 创建 TokenLimitExecutor 实例
func NewTokenLimitExecutor(ss core.StateStore) *TokenLimitExecutor {
	return &TokenLimitExecutor{stateStore: ss}
}

func (e *TokenLimitExecutor) Execute(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	limitKey := GetLimitKey(gctx, lp)
	estimate := EstimateInputTokens(gctx, lp) + EstimateOutputTokens(ctx, e.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
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
			allowed, remaining, err := e.stateStore.RateLimitTake(ctx, limitKey+":"+window.String(), estimate, sw.Threshold, capacity, window, time.Now())
			if err != nil {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, estimate)
				return err
			}
			if !allowed {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, estimate)
				gctx.Logger(zap.L()).Warn("Rate limit enforced (token usage exceeded via token bucket)",
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
					Message:      "rate limit exceeded",
					Threshold:    float64Ptr(float64(sw.Threshold)),
					CurrentValue: float64Ptr(float64(capacity - remaining)),
				}
			}
		} else {
			current, err := e.stateStore.RateLimitIncr(ctx, limitKey+":"+window.String(), estimate, window)
			if err != nil {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i, estimate)
				return err
			}

			if current > sw.Threshold {
				e.rollback(ctx, limitKey, lp.SlidingWindows, i+1, estimate)
				gctx.Logger(zap.L()).Warn("Rate limit enforced (token usage exceeded)",
					zap.String("policy_name", lp.Name),
					zap.String("limit_type", lp.Type),
					zap.String("user_id", gctx.UserID),
					zap.String("tenant", gctx.Tenant),
					zap.String("model", gctx.Model),
					zap.String("window_size", window.String()),
					zap.Int64("threshold", sw.Threshold),
					zap.Int64("current", current),
					zap.Int64("estimated_request_tokens", estimate),
				)
				return &HTTPError{
					Code:         http.StatusTooManyRequests,
					Message:      "rate limit exceeded",
					Threshold:    float64Ptr(float64(sw.Threshold)),
					CurrentValue: float64Ptr(float64(current)),
				}
			}
		}
	}
	return nil
}

func (e *TokenLimitExecutor) rollback(ctx context.Context, limitKey string, windows []*policy.SlidingWindow, count int, tokens int64) {
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

func (e *TokenLimitExecutor) Refund(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
	limitKey := GetLimitKey(gctx, lp)
	estimate := EstimateInputTokens(gctx, lp) + EstimateOutputTokens(ctx, e.stateStore, gctx.Tenant, gctx.UserID, gctx.Model)
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
			_, _, _ = e.stateStore.RateLimitTake(ctx, limitKey+":"+window.String(), -estimate, sw.Threshold, capacity, window, time.Now())
		} else {
			_ = e.stateStore.RateLimitRefund(ctx, limitKey+":"+window.String(), estimate)
		}
	}
	return nil
}

// EstimateInputTokens 粗略估算 input token 数（支持策略化估算，默认每 4 字节约 1 token）
func EstimateInputTokens(gctx *core.GatewayContext, lp *policy.LimitPolicy) int64 {
	if lp != nil && lp.Estimator != nil {
		switch lp.Estimator.Type {
		case "length_ratio":
			if lp.Estimator.Ratio > 0 {
				return int64(float64(len(gctx.RawBody)) * lp.Estimator.Ratio)
			}
		case "tiktoken":
			if len(gctx.RawBody) > 0 {
				var req chatRequest
				if json.Unmarshal(gctx.RawBody, &req) == nil && len(req.Messages) > 0 {
					return CountChatInputTokens(req.Messages, gctx.Model)
				}
			}
		}
	}
	return int64(len(gctx.RawBody)) / 4
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name"`
}

type chatRequest struct {
	Messages []chatMessage `json:"messages"`
}

// CountChatInputTokens 精准计算 chat completion input 的 token 数 (针对 tiktoken 库)
func CountChatInputTokens(messages []chatMessage, model string) int64 {
	tkm, err := tiktoken.EncodingForModel(model)
	if err != nil {
		// 未知模型退避使用 cl100k_base
		tkm, err = tiktoken.GetEncoding("cl100k_base")
		if err != nil {
			return 0
		}
	}

	var tokensPerMessage int
	var tokensPerName int
	if model == "gpt-3.5-turbo-0301" || model == "gpt-3.5-turbo" {
		tokensPerMessage = 4
		tokensPerName = -1
	} else if model == "gpt-4-0314" || model == "gpt-4" {
		tokensPerMessage = 3
		tokensPerName = 1
	} else {
		// 现代模型通用默认值
		tokensPerMessage = 3
		tokensPerName = 1
	}

	numTokens := 0
	for _, msg := range messages {
		numTokens += tokensPerMessage
		numTokens += len(tkm.Encode(msg.Content, nil, nil))
		numTokens += len(tkm.Encode(msg.Role, nil, nil))
		if msg.Name != "" {
			numTokens += len(tkm.Encode(msg.Name, nil, nil))
			numTokens += tokensPerName
		}
	}
	numTokens += 3 // assistant prefix 偏置
	return int64(numTokens)
}

// EstimateOutputTokens 获取该租户/模型在滑动窗口下的 Output EMA 估算值
func EstimateOutputTokens(ctx context.Context, ss core.StateStore, tenant, userID, model string) int64 {
	if ss == nil {
		return 200
	}

	// 1. 优先使用租户级特定模型的 EMA
	tenantKey := tenant
	if tenantKey == "" {
		tenantKey = userID
	}
	if tenantKey != "" {
		val, err := ss.GetEMA(ctx, "tenant:"+tenantKey+":"+model)
		if err == nil && val > 0 {
			return int64(val)
		}
	}

	// 2. 退避至模型全局级 EMA
	val, err := ss.GetEMA(ctx, "model:global:"+model)
	if err == nil && val > 0 {
		return int64(val)
	}

	// 3. 兜底默认值
	return 200
}
