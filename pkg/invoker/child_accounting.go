package invoker

import (
	"errors"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
)

// childAccounting 只复用已装配过滤器的预留与结算能力，不运行外层副作用。
type childAccounting struct {
	limits  *inbound.RateLimitFilter
	settler *outbound.TokenSettlementFilter
}

type childCallResult struct {
	admissionErr  error
	invokeErr     error
	settlementErr error
}

func (r childCallResult) err() error {
	if r.settlementErr != nil {
		return r.settlementErr
	}
	if r.admissionErr != nil {
		return r.admissionErr
	}
	return r.invokeErr
}

func (a childAccounting) invoke(child *core.GatewayContext, invoke core.Invoker) childCallResult {
	result := childCallResult{admissionErr: a.limits.OnRequest(child)}
	if result.admissionErr == nil {
		result.invokeErr = child.Ctx.Err()
		if result.invokeErr == nil {
			checked, err := capacityCheckedInvoker(invoke)
			result.invokeErr = err
			if err == nil {
				result.invokeErr = checked.Invoke(child)
			}
		}
	}
	child.Err = result.err()
	// SettleLimits 使用无取消上下文并以 reservation 的 Settled 标记幂等。
	result.settlementErr = a.settler.SettleLimits(child)
	return result
}

func (r *SmartRouter) childAccounting(pipe *core.Pipeline) (childAccounting, error) {
	a := childAccounting{limits: r.limits, settler: r.settler}
	if a.limits == nil {
		for _, filter := range pipe.InboundFilters {
			if limits, ok := filter.(*inbound.RateLimitFilter); ok {
				a.limits = limits
				break
			}
		}
	}
	if a.settler == nil {
		for _, filter := range pipe.OutboundFilters {
			if settler, ok := filter.(*outbound.TokenSettlementFilter); ok {
				a.settler = settler
				break
			}
		}
	}
	if a.limits == nil || a.settler == nil {
		return a, errors.New("smart child quota filters not configured")
	}
	return a, nil
}
