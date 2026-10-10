package invoker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"go.uber.org/zap"
)

// attemptObserver 集中许可与健康反馈，顺序调用和对冲保留各自的异常规则。
type attemptObserver struct {
	cbManager    *core.CircuitBreakerManager
	stateStore   core.StateStore
	logger       *zap.Logger
	enableActive bool
}

type observedAttempt struct {
	observer *attemptObserver
	gctx     *core.GatewayContext
	lease    *core.CircuitBreakerAttempt
	once     sync.Once
}

func (o *attemptObserver) begin(gctx *core.GatewayContext, ep *core.Endpoint) (*observedAttempt, error) {
	lease, err := o.cbManager.AcquireAttempt(gctx, ep, o.enableActive)
	if err != nil {
		return nil, err
	}
	return &observedAttempt{observer: o, gctx: gctx, lease: lease}, nil
}

func (a *observedAttempt) release() {
	a.once.Do(a.lease.Release)
}

func (a *observedAttempt) sequentialOutcome(err error) {
	a.once.Do(func() {
		defer a.lease.Release()
		if errors.Is(err, core.ErrClientDisconnected) || err != nil && isExplicitlyNonRetryable(err) {
			return
		}
		if err != nil {
			a.lease.RecordFailure(a.gctx, err)
			a.observer.recordFailurePenalty(a.gctx)
			return
		}
		if slowErr := sequentialSlowFailure(a.gctx); slowErr != nil {
			a.lease.RecordFailure(a.gctx, slowErr)
		} else {
			a.lease.RecordSuccess(a.gctx)
		}
		a.observer.recordLatency(a.gctx, true)
	})
}

func (a *observedAttempt) hedgedOutcome(err error) {
	a.once.Do(func() {
		defer a.lease.Release()
		if err != nil {
			// 包括 loser cancel/断连/不可重试，是否计数仍由 Manager.MatchError 决定。
			a.lease.RecordFailure(a.gctx, err)
			return
		}
		a.lease.RecordSuccess(a.gctx)
		a.observer.recordLatency(a.gctx, false)
	})
}

func sequentialSlowFailure(gctx *core.GatewayContext) error {
	if gctx.Policy == nil {
		return nil
	}
	for _, p := range gctx.Policy.CircuitBreakPolicies {
		if p == nil {
			continue
		}
		limit := time.Duration(p.SlowCallDurationThreshold) * time.Millisecond
		if p.SlowCallMetric == "TTFT" && gctx.TTFT > 0 {
			if gctx.TTFT > limit {
				return fmt.Errorf("slow call TTFT exceeded")
			}
		} else if p.SlowCallMetric == "RTT" || p.SlowCallMetric == "Duration" {
			if time.Since(gctx.UpstreamConnect) > limit {
				return fmt.Errorf("slow call RTT exceeded")
			}
		}
	}
	return nil
}

func (o *attemptObserver) recordLatency(gctx *core.GatewayContext, includeTTFT bool) {
	if gctx.SelectedEndpoint == nil {
		return
	}
	epID := gctx.SelectedEndpoint.ID
	o.stateStore.RecordLatency(gctx.Ctx, epID, time.Since(gctx.UpstreamConnect))
	if includeTTFT && gctx.TTFT > 0 {
		if err := o.stateStore.RecordTTFT(gctx.Ctx, epID, gctx.TTFT); err != nil {
			gctx.Logger(o.logger).Warn("record ttft failed", zap.String("endpoint", epID), zap.Error(err))
		}
	}
}

const (
	defaultFailurePenalty  = 3.0
	defaultFailureMax      = 30 * time.Second
	minFailurePenalty      = 1.0
	defaultLatencyWindowLL = 5 * time.Minute
)

// recordFailurePenalty 用历史均值乘系数记录合成延迟；对冲不走此路径。
func (o *attemptObserver) recordFailurePenalty(gctx *core.GatewayContext) {
	if gctx == nil || gctx.SelectedEndpoint == nil {
		return
	}
	params := lbParams(gctx)
	multiplier, maxPenalty := resolveFailurePenaltyConfig(params)
	if multiplier == 0 {
		return
	}
	if multiplier < minFailurePenalty {
		multiplier = minFailurePenalty
	}
	window, metric := resolveLatencyConfig(params)
	epID := gctx.SelectedEndpoint.ID
	histAvg := func() (time.Duration, error) {
		if metric == "ttft" {
			return o.stateStore.GetAvgTTFT(gctx.Ctx, epID, window)
		}
		return o.stateStore.GetAvgLatency(gctx.Ctx, epID, window)
	}
	avg, err := histAvg()
	if err != nil || avg <= 0 {
		o.writePenalty(gctx, metric, maxPenalty)
		return
	}
	penalty := time.Duration(float64(avg) * multiplier)
	if penalty > maxPenalty {
		penalty = maxPenalty
	}
	o.writePenalty(gctx, metric, penalty)
}

func (o *attemptObserver) writePenalty(gctx *core.GatewayContext, metric string, penalty time.Duration) {
	epID := gctx.SelectedEndpoint.ID
	if metric == "ttft" {
		if err := o.stateStore.RecordTTFT(gctx.Ctx, epID, penalty); err != nil {
			gctx.Logger(o.logger).Warn("record ttft penalty failed", zap.String("endpoint", epID), zap.Error(err))
		}
		return
	}
	if err := o.stateStore.RecordLatency(gctx.Ctx, epID, penalty); err != nil {
		gctx.Logger(o.logger).Warn("record latency penalty failed", zap.String("endpoint", epID), zap.Error(err))
	}
}

func lbParams(gctx *core.GatewayContext) map[string]interface{} {
	if gctx == nil || gctx.Policy == nil || gctx.Policy.LoadBalancePolicy == nil {
		return nil
	}
	return gctx.Policy.LoadBalancePolicy.Params
}

func resolveLatencyConfig(params map[string]interface{}) (window time.Duration, metric string) {
	window = defaultLatencyWindowLL
	metric = "total"
	if params == nil {
		return
	}
	if v, ok := params["latency_window"]; ok {
		switch x := v.(type) {
		case float64:
			if x > 0 {
				window = time.Duration(x) * time.Second
			}
		case int:
			if x > 0 {
				window = time.Duration(x) * time.Second
			}
		case string:
			if d, err := time.ParseDuration(x); err == nil && d > 0 {
				window = d
			}
		}
	}
	if v, ok := params["latency_metric"]; ok {
		if s, ok := v.(string); ok && (s == "ttft" || s == "total") {
			metric = s
		}
	}
	return
}

func resolveFailurePenaltyConfig(params map[string]interface{}) (multiplier float64, maxPenalty time.Duration) {
	multiplier = defaultFailurePenalty
	maxPenalty = defaultFailureMax
	if params == nil {
		return
	}
	if v, ok := params["latency_failure_penalty"]; ok {
		switch x := v.(type) {
		case float64:
			multiplier = x
		case int:
			multiplier = float64(x)
		}
	}
	if v, ok := params["latency_failure_max"]; ok {
		switch x := v.(type) {
		case float64:
			if x > 0 {
				maxPenalty = time.Duration(x) * time.Second
			}
		case int:
			if x > 0 {
				maxPenalty = time.Duration(x) * time.Second
			}
		case string:
			if d, err := time.ParseDuration(x); err == nil && d > 0 {
				maxPenalty = d
			}
		}
	}
	return
}
