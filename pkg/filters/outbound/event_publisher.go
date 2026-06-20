package outbound

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"

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
// A single request may trigger multiple events (e.g. circuit_break + lb_switch).
func (f *EventPublishFilter) analyzeEvents(gctx *core.GatewayContext) []*events.OpsEvent {
	if gctx.Err == nil && len(gctx.FallbackChain) <= 1 {
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
		TenantCode:   gctx.Tenant,
		ModelCode:    gctx.OriginalModel, // use original model, not fallback target
		RequestID:    requestID,
		TraceID:      traceID,
		Timestamp:    time.Now().Unix(),
	}

	if gctx.SelectedEndpoint != nil {
		base.EndpointID = gctx.SelectedEndpoint.ID
		base.ProviderName = gctx.SelectedEndpoint.Provider
		if gctx.SelectedEndpoint.Model != "" {
			base.ModelCode = gctx.SelectedEndpoint.Model
		}
	} else {
		// 针对限流或熔断拦截等未进行物理选路的场景，尝试通过服务发现推断默认供应商，以补全事件中的供应商字段
		if f.discovery != nil && gctx.Model != "" {
			if eps, err := f.discovery.List(gctx.Ctx, gctx.Model); err == nil && len(eps) > 0 {
				base.EndpointID = eps[0].ID
				base.ProviderName = eps[0].Provider
			}
		}
	}

	var result []*events.OpsEvent

	// 1. Circuit breaker: detect from History (attempts blocked by CB) or final error
	if cbEvent := f.detectCircuitBreaker(gctx, base); cbEvent != nil {
		result = append(result, cbEvent)
	}

	// 2. LB switch / fallback
	if len(gctx.FallbackChain) > 1 {
		evt := base
		evt.EventType = events.EventTypeLBSwitch
		evt.ModelCode = gctx.Model // fallback target model
		evt.Message = "fallback from " + gctx.FallbackChain[0] + " to " + gctx.FallbackChain[len(gctx.FallbackChain)-1]
		if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil {
			evt.PolicyID = gctx.Policy.InvocationPolicy.ID
			evt.PolicyName = gctx.Policy.InvocationPolicy.Name
		}
		result = append(result, &evt)
	}

	// 3. Rate limit (HTTP 429)
	if gctx.Err != nil {
		var httpErr *limiter.HTTPError
		if errors.As(gctx.Err, &httpErr) && httpErr.Code == http.StatusTooManyRequests {
			evt := base
			evt.EventType = events.EventTypeRateLimit
			evt.Message = httpErr.Error()
			if gctx.Policy != nil && len(gctx.Policy.LimitPolicies) > 0 {
				lp := gctx.Policy.LimitPolicies[0]
				evt.PolicyID = lp.ID
				evt.PolicyName = lp.Name
				if len(lp.SlidingWindows) > 0 {
					tVal := float64(lp.SlidingWindows[0].Threshold)
					evt.Threshold = &tVal
				}
			}
			return append(result, &evt) // rate limit is terminal, no other events
		}
	}

	// 4. Invocation failure (generic, only if no circuit breaker already detected and no policy event emitted by ClusterInvoker)
	if gctx.Err != nil && len(result) == 0 && !gctx.PolicyEventEmitted {
		evt := base
		evt.EventType = events.EventTypeInvocationFail
		evt.Message = gctx.Err.Error()
		if gctx.OriginalModel != "" && evt.ModelCode != gctx.OriginalModel {
			evt.Message = fmt.Sprintf("%s (original request model: %s)", evt.Message, gctx.OriginalModel)
		}
		if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil {
			evt.PolicyID = gctx.Policy.InvocationPolicy.ID
			evt.PolicyName = gctx.Policy.InvocationPolicy.Name
		}
		result = append(result, &evt)
	}

	return result
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
