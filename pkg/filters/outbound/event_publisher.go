package outbound

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// EventPublishFilter analyzes request outcomes and publishes policy events.
type EventPublishFilter struct {
	publisher events.Publisher
	logger    *zap.Logger
	discovery core.Discovery
}

// NewEventPublishFilter creates a new EventPublishFilter.
func NewEventPublishFilter(publisher events.Publisher, logger *zap.Logger) *EventPublishFilter {
	return &EventPublishFilter{
		publisher: publisher,
		logger:    logger,
	}
}

// SetDiscovery sets the Discovery service for endpoint lookup.
func (f *EventPublishFilter) SetDiscovery(discovery core.Discovery) {
	f.discovery = discovery
}

func (f *EventPublishFilter) Name() string                        { return "event_publisher" }
func (f *EventPublishFilter) Order() int                          { return 50 }
func (f *EventPublishFilter) Criticality() core.FilterCriticality { return core.BestEffort }
func (f *EventPublishFilter) InboundSafe()                        {}

func (f *EventPublishFilter) OnResponse(gctx *core.GatewayContext) error {
	if f.publisher == nil {
		return nil
	}

	evts := f.analyzeEvents(gctx)
	if len(evts) == 0 {
		return nil
	}

	// Publish asynchronously via publisher's buffered channel
	for _, evt := range evts {
		if err := f.publisher.Publish(gctx.Ctx, evt); err != nil {
			f.logger.Warn("event publish failed",
				zap.String("event_type", evt.EventType),
				zap.Error(err),
			)
		}
	}

	return nil
}

// analyzeEvents examines the GatewayContext and returns all applicable policy events.
// A single request may trigger multiple events (e.g. circuit_break + model_failover).
func (f *EventPublishFilter) analyzeEvents(gctx *core.GatewayContext) []*events.OpsEvent {
	if gctx.Err == nil && len(gctx.FallbackChain) <= 1 && !hasFailedAttempts(gctx.History) {
		return nil
	}

	traceID := ""
	if gctx.Request != nil {
		traceID = gctx.Request.Header.Get("X-Trace-ID")
	}
	if traceID == "" && gctx.ResponseWriter != nil {
		traceID = gctx.ResponseWriter.Header().Get("X-Trace-Id")
	}
	requestID := ""
	if gctx.Request != nil {
		requestID = gctx.Request.Header.Get("X-Request-ID")
	}
	if requestID == "" {
		requestID = traceID
	}

	base := events.OpsEvent{
		TenantCode: gctx.Tenant,
		ModelCode:  gctx.OriginalModel, // use original model, not fallback target
		RequestID:  requestID,
		TraceID:    traceID,
		Timestamp:  time.Now().Unix(),
	}
	if base.ModelCode == "" {
		base.ModelCode = gctx.Model
	}

	if gctx.SelectedEndpoint != nil {
		base.EndpointID = gctx.SelectedEndpoint.ID
		base.EndpointCode = gctx.SelectedEndpoint.Code
		base.ProviderName = gctx.SelectedEndpoint.Provider
		if gctx.SelectedEndpoint.Model != "" {
			base.ModelCode = gctx.SelectedEndpoint.Model
		}
	} else {
		// 针对限流拦截等未选路成功但模型正常配置的场景，补全供应商信息。
		// 但如果是因无可用端点（core.ErrNoAvailableEndpoint）引起的熔断或失败，不绑定任何具体的 EndpointCode/Provider，以防展示错乱。
		if gctx.Err == nil || !errors.Is(gctx.Err, core.ErrNoAvailableEndpoint) {
			if f.discovery != nil && gctx.Model != "" {
				if eps, err := f.discovery.List(gctx.Ctx, gctx.Model); err == nil && len(eps) > 0 {
					base.EndpointID = eps[0].ID
					base.EndpointCode = eps[0].Code
					base.ProviderName = eps[0].Provider
				}
			}
		}
	}

	var result []*events.OpsEvent

	// 1. Circuit breaker: detect from History (attempts blocked by CB) or final error
	if cbEvent := f.detectCircuitBreaker(gctx, base); cbEvent != nil {
		result = append(result, cbEvent)
	}

	// 2. Invocation failure for failed upstream attempts recorded in History.
	result = append(result, f.detectInvocationFailuresFromHistory(gctx, base)...)

	// 3. Model Failover / fallback
	if len(gctx.FallbackChain) > 1 {
		evt := base
		evt.EventType = events.EventTypeModelFailover
		evt.ModelCode = gctx.Model // fallback target model
		evt.Message = "fallback from " + gctx.FallbackChain[0] + " to " + gctx.FallbackChain[len(gctx.FallbackChain)-1]
		if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil {
			evt.PolicyID = gctx.Policy.InvocationPolicy.ID
			evt.PolicyName = gctx.Policy.InvocationPolicy.Name
		}
		result = append(result, &evt)
	}

	// 3.5. Endpoint-level failover (failover within the same model)
	if len(gctx.History) > 1 {
		lastAttempt := gctx.History[len(gctx.History)-1]
		if lastAttempt.Success {
			var steps []string
			hasFailure := false
			for i, rec := range gctx.History {
				stepNum := i + 1
				if rec.Success {
					steps = append(steps, fmt.Sprintf("[%d] %s (%s) succeeded", stepNum, rec.EndpointID, rec.Provider))
				} else {
					hasFailure = true
					errStr := rec.Error
					if errStr == "" {
						errStr = fmt.Sprintf("status %d", rec.StatusCode)
					}
					steps = append(steps, fmt.Sprintf("[%d] %s (%s) failed: %s", stepNum, rec.EndpointID, rec.Provider, errStr))
				}
			}
			if hasFailure {
				evt := base
				evt.EventType = events.EventTypeEndpointFailover
				evt.Message = "endpoint failover: " + strings.Join(steps, " -> ")
				if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil {
					evt.PolicyID = gctx.Policy.InvocationPolicy.ID
					evt.PolicyName = gctx.Policy.InvocationPolicy.Name
				}
				result = append(result, &evt)
			}
		}
	}

	// 4. Rate limit (HTTP 429)
	if gctx.Err != nil {
		var httpErr *limiter.HTTPError
		if errors.As(gctx.Err, &httpErr) && httpErr.Code == http.StatusTooManyRequests {
			evt := base
			evt.EventType = events.EventTypeRateLimit
			evt.Message = httpErr.Error()
			evt.Threshold = httpErr.Threshold
			evt.CurrentValue = httpErr.CurrentValue

			if gctx.Policy != nil && len(gctx.Policy.LimitPolicies) > 0 {
				// 获取可能匹配的限流策略以补充 policy 元数据
				var matchedLP *policy.LimitPolicy
				for _, lp := range gctx.Policy.LimitPolicies {
					if lp.Name == gctx.Tags["rate_limit_policy_name"] || lp.ID == gctx.Tags["rate_limit_policy_id"] {
						matchedLP = lp
						break
					}
				}
				if matchedLP == nil {
					matchedLP = gctx.Policy.LimitPolicies[0]
				}
				evt.PolicyID = matchedLP.ID
				evt.PolicyName = matchedLP.Name
				if evt.Threshold == nil && len(matchedLP.SlidingWindows) > 0 {
					tVal := float64(matchedLP.SlidingWindows[0].Threshold)
					evt.Threshold = &tVal
				}
			}
			return append(result, &evt) // rate limit is terminal, no other events
		}
	}

	// 5. Invocation failure (generic, only if no circuit breaker already detected and no policy event emitted by ClusterInvoker)
	// 只在真正调用了上游服务时才发出 invocation_fail 事件（AttemptCount > 0 或 SelectedEndpoint != nil）
	if gctx.Err != nil && len(result) == 0 && !gctx.PolicyEventEmitted {
		// 判断是否真正到达了 Invoker 阶段并尝试调用上游
		hasAttemptedInvocation := gctx.AttemptCount > 0 || gctx.SelectedEndpoint != nil
		if hasAttemptedInvocation {
			evt := base
			evt.EventType = events.EventTypeInvocationFail
			evt.Message = gctx.Err.Error()
			if gctx.OriginalModel != "" && evt.ModelCode != gctx.OriginalModel {
				evt.Message = fmt.Sprintf("%s (original request model: %s)", evt.Message, gctx.OriginalModel)
			}
			result = append(result, &evt)
		}
	}

	return result
}

func hasFailedAttempts(history []core.AttemptRecord) bool {
	for _, rec := range history {
		if !rec.Success {
			return true
		}
	}
	return false
}

func (f *EventPublishFilter) detectInvocationFailuresFromHistory(gctx *core.GatewayContext, base events.OpsEvent) []*events.OpsEvent {
	if gctx.Policy == nil {
		return nil
	}

	var result []*events.OpsEvent
	for _, rec := range gctx.History {
		if rec.Success {
			continue
		}
		errMsg := rec.Error

		if gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
			rp := gctx.Policy.InvocationPolicy.RetryPolicy
			if rp.Retry > 0 {
				if matched, reason := rp.MatchErrorWithReason(rec.StatusCode, rec.ContentType, errMsg, rec.Body); matched {
					evt := invocationFailEventFromAttempt(base, rec)
					evt.EventType = events.EventTypeRetryError
					evt.Message = fmt.Sprintf("retry policy matched: %s", reason)
					if errMsg != "" {
						evt.Message = fmt.Sprintf("%s (attempt error: %s)", evt.Message, errMsg)
					}
					evt.PolicyID = gctx.Policy.InvocationPolicy.ID
					evt.PolicyName = gctx.Policy.InvocationPolicy.Name
					result = append(result, &evt)
				}
			}
		}

		for _, cbPolicy := range gctx.Policy.CircuitBreakPolicies {
			if cbPolicy == nil {
				continue
			}
			if matched, reason := cbPolicy.MatchErrorWithReason(rec.StatusCode, rec.ContentType, errMsg, rec.Body); matched {
				evt := invocationFailEventFromAttempt(base, rec)
				evt.EventType = events.EventTypeCircuitBreakerError
				evt.Message = fmt.Sprintf("circuit breaker policy matched: %s", reason)
				if errMsg != "" {
					evt.Message = fmt.Sprintf("%s (attempt error: %s)", evt.Message, errMsg)
				}
				evt.PolicyID = cbPolicy.ID
				evt.PolicyName = cbPolicy.Name
				result = append(result, &evt)
			}
		}
	}

	return result
}

func invocationFailEventFromAttempt(base events.OpsEvent, rec core.AttemptRecord) events.OpsEvent {
	evt := base
	evt.EventType = events.EventTypeInvocationFail
	if rec.Model != "" {
		evt.ModelCode = rec.Model
	}
	evt.EndpointID = rec.EndpointID
	evt.EndpointCode = rec.EndpointCode
	evt.ProviderName = rec.Provider
	if !rec.Timestamp.IsZero() {
		evt.Timestamp = rec.Timestamp.Unix()
	}
	return evt
}

// detectCircuitBreaker checks if the request was blocked by the circuit breaker.
// Note: the Closed→Open transition event is published directly by CircuitBreakerManager.
// This detects requests blocked by an already-open breaker (no fallback or all fallbacks failed).
func (f *EventPublishFilter) detectCircuitBreaker(gctx *core.GatewayContext, base events.OpsEvent) *events.OpsEvent {
	if gctx.Err != nil && errors.Is(gctx.Err, core.ErrNoAvailableEndpoint) {
		evt := base
		evt.EventType = events.EventTypeCircuitBreak
		evt.Message = gctx.Err.Error()
		if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
			cb := gctx.Policy.CircuitBreakPolicies[0]
			evt.PolicyID = cb.ID
			evt.PolicyName = cb.Name
		}
		return &evt
	}
	return nil
}
