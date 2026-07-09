package inbound

import (
	"net/http"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// RateLimitFilter 限流过滤器，基于动态 Policy 进行令牌桶限流
type RateLimitFilter struct {
	stateStore   core.StateStore
	eventHandler RateLimitEventHandler
}

// RateLimitEventHandler is called when a rate limit is triggered.
type RateLimitEventHandler func(tenant, model, policyID, policyName, limitType string)

// SetEventHandler 注入限流事件回调
func (f *RateLimitFilter) SetEventHandler(handler RateLimitEventHandler) {
	f.eventHandler = handler
}

// NewRateLimitFilter 创建 RateLimitFilter 并向工厂注册其执行器
func NewRateLimitFilter(ss core.StateStore) *RateLimitFilter {
	if ss != nil {
		core.DefaultLimitExecutorFactory.Register("request", limiter.NewRequestLimitExecutor(ss))
		core.DefaultLimitExecutorFactory.Register("token", limiter.NewTokenLimitExecutor(ss))
		core.DefaultLimitExecutorFactory.Register("cost", limiter.NewCostLimitExecutor(ss))
	}
	return &RateLimitFilter{stateStore: ss}
}

func (f *RateLimitFilter) Name() string { return "rate_limit" }
func (f *RateLimitFilter) Order() int   { return 20 }

func (f *RateLimitFilter) OnRequest(gctx *core.GatewayContext) error {
	p := gctx.Policy
	if p == nil || len(p.LimitPolicies) == 0 {
		return nil
	}

	for _, lp := range p.LimitPolicies {
		// 自治条件判断
		if !MatchLimitPolicyConditions(gctx, lp) {
			continue // 条件不匹配，跳过本条限流策略
		}
		// 估算并写入初始预估 InputTokens，防止流响应异常中断时统计归零退费
		if (lp.Type == "token" || lp.Type == "cost") && gctx.InputTokens == 0 {
			gctx.InputTokens = int(limiter.EstimateInputTokens(gctx, lp))
		}
		exec := core.DefaultLimitExecutorFactory.Get(lp.Type)
		if exec != nil {
			var err error
			if lp.MaxWaitMs > 0 {
				deadline := time.Now().Add(time.Duration(lp.MaxWaitMs) * time.Millisecond)
				for {
					err = exec.Execute(gctx.Ctx, gctx, lp)
					if err == nil {
						break
					}
					httpErr, ok := err.(*limiter.HTTPError)
					if !ok || httpErr.Code != http.StatusTooManyRequests {
						break
					}
					select {
					case <-gctx.Ctx.Done():
						err = gctx.Ctx.Err()
						break
					default:
					}
					if err != nil && err == gctx.Ctx.Err() {
						break
					}
					if time.Now().Add(20 * time.Millisecond).After(deadline) {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			} else {
				err = exec.Execute(gctx.Ctx, gctx, lp)
			}
			if err != nil {
				if httpErr, ok := err.(*limiter.HTTPError); ok && httpErr.Code == http.StatusTooManyRequests {
					if gctx.Tags == nil {
						gctx.Tags = make(map[string]string)
					}
					gctx.Tags["rate_limit_policy_name"] = lp.Name
					gctx.Tags["rate_limit_policy_id"] = lp.ID
				}
				// 限流触发时直接发事件，此时 policy 信息完整
				if f.eventHandler != nil {
					if httpErr, ok := err.(*limiter.HTTPError); ok && httpErr.Code == http.StatusTooManyRequests {
						f.eventHandler(gctx.Tenant, gctx.Model, lp.ID, lp.Name, lp.Type)
					}
				}
				return err
			}
		}
	}
	return nil
}

func MatchLimitPolicyConditions(gctx *core.GatewayContext, lp *policy.LimitPolicy) bool {
	for _, cond := range lp.Conditions {
		if cond.Type == "" {
			continue // 忽略空条件对象
		}
		m := matcher.DefaultTagMatcherFactory.Get(strings.ToLower(cond.Type))
		if m == nil || !m.Match(gctx.Ctx, cond, gctx) {
			return false // 任一有效条件不满足，即匹配失败
		}
	}
	return true
}
