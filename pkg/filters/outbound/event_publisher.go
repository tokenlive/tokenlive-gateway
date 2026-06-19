package outbound

import (
	"context"
	"errors"
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
}

// NewEventPublishFilter creates a new EventPublishFilter.
func NewEventPublishFilter(publisher events.Publisher, logger *zap.Logger) *EventPublishFilter {
	return &EventPublishFilter{
		publisher: publisher,
		logger:    logger,
	}
}

func (f *EventPublishFilter) Name() string                        { return "event_publisher" }
func (f *EventPublishFilter) Order() int                          { return 50 }
func (f *EventPublishFilter) Criticality() core.FilterCriticality { return core.BestEffort }

func (f *EventPublishFilter) OnResponse(gctx *core.GatewayContext) error {
	if f.publisher == nil {
		return nil
	}

	evt := f.analyzeEvent(gctx)
	if evt == nil {
		return nil
	}

	// Publish asynchronously to avoid blocking the response path
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.publisher.Publish(bgCtx, evt); err != nil {
			f.logger.Warn("event publish failed",
				zap.String("event_type", evt.EventType),
				zap.Error(err),
			)
		}
	}()

	return nil
}

// analyzeEvent examines the GatewayContext and returns an Event if a policy was triggered.
func (f *EventPublishFilter) analyzeEvent(gctx *core.GatewayContext) *events.OpsEvent {
	if gctx.Err == nil && len(gctx.FallbackChain) <= 1 {
		return nil // No policy event occurred
	}

	base := events.OpsEvent{
		TenantCode:   gctx.Tenant,
		ModelCode:    gctx.Model,
		RequestID:    gctx.Request.Header.Get("X-Request-ID"),
		TraceID:      gctx.Request.Header.Get("X-Trace-ID"),
		Timestamp:    time.Now().Unix(),
	}

	if gctx.SelectedEndpoint != nil {
		base.EndpointID = gctx.SelectedEndpoint.ID
		base.ProviderName = gctx.SelectedEndpoint.Provider
	}

	// 1. LB switch / fallback (highest priority — check before error classification)
	if len(gctx.FallbackChain) > 1 {
		evt := base
		evt.EventType = events.EventTypeLBSwitch
		evt.Message = "fallback from " + gctx.FallbackChain[0] + " to " + gctx.FallbackChain[len(gctx.FallbackChain)-1]
		return &evt
	}

	if gctx.Err == nil {
		return nil
	}

	// 2. Rate limit (HTTP 429)
	var httpErr *limiter.HTTPError
	if errors.As(gctx.Err, &httpErr) && httpErr.Code == http.StatusTooManyRequests {
		evt := base
		evt.EventType = events.EventTypeRateLimit
		evt.Message = httpErr.Error()
		// Try to extract policy info from the matched limit policies
		if gctx.Policy != nil && len(gctx.Policy.LimitPolicies) > 0 {
			lp := gctx.Policy.LimitPolicies[0]
			evt.PolicyID = lp.ID
			evt.PolicyName = lp.Name
		}
		return &evt
	}

	// 3. Circuit breaker (no available endpoint)
	if errors.Is(gctx.Err, core.ErrNoAvailableEndpoint) {
		evt := base
		evt.EventType = events.EventTypeCircuitBreak
		evt.Message = gctx.Err.Error()
		return &evt
	}

	// 4. Invocation failure (generic)
	evt := base
	evt.EventType = events.EventTypeInvocationFail
	evt.Message = gctx.Err.Error()
	return &evt
}
