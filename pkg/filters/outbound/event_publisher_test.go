package outbound

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type mockPublisher struct {
	published []*events.OpsEvent
}

func (m *mockPublisher) Publish(ctx context.Context, event *events.OpsEvent) error {
	m.published = append(m.published, event)
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

type mockDiscovery struct {
	endpoints []*core.Endpoint
}

func (m *mockDiscovery) List(ctx context.Context, model string) ([]*core.Endpoint, error) {
	return m.endpoints, nil
}

func (m *mockDiscovery) Watch(ctx context.Context, model string) (<-chan []*core.Endpoint, error) {
	return nil, nil
}

func (m *mockDiscovery) Close() error {
	return nil
}

func TestEventPublishFilter_OnResponse(t *testing.T) {
	t.Run("No event when successful and no fallback", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:           context.Background(),
			Request:       httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:         "gpt-4",
			FallbackChain: []string{"gpt-4"},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 0 {
			t.Errorf("expected 0 published events, got %d", len(pub.published))
		}
	})

	t.Run("LB Switch event when fallback happens", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:           context.Background(),
			Request:       httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:         "gpt-4:free",
			FallbackChain: []string{"gpt-4", "gpt-4:free"},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(pub.published))
		}
		evt := pub.published[0]
		if evt.EventType != events.EventTypeLBSwitch {
			t.Errorf("expected event type %q, got %q", events.EventTypeLBSwitch, evt.EventType)
		}
		if evt.Message != "fallback from gpt-4 to gpt-4:free" {
			t.Errorf("unexpected message: %q", evt.Message)
		}
	})

	t.Run("Rate Limit event", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:   "gpt-4",
			Err:     &limiter.HTTPError{Code: http.StatusTooManyRequests, Message: "too many requests"},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(pub.published))
		}
		evt := pub.published[0]
		if evt.EventType != events.EventTypeRateLimit {
			t.Errorf("expected event type %q, got %q", events.EventTypeRateLimit, evt.EventType)
		}
	})

	t.Run("Circuit Break event", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:   "gpt-4",
			Err:     core.ErrNoAvailableEndpoint,
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(pub.published))
		}
		evt := pub.published[0]
		if evt.EventType != events.EventTypeCircuitBreak {
			t.Errorf("expected event type %q, got %q", events.EventTypeCircuitBreak, evt.EventType)
		}
	})

	t.Run("Generic Invocation Failure event", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:          context.Background(),
			Request:      httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:        "gpt-4",
			Err:          errors.New("some network error"),
			AttemptCount: 1, // 表示已经尝试过调用上游
			SelectedEndpoint: &core.Endpoint{
				ID:       "ep-test-1",
				Provider: "TestProvider",
				Model:    "gpt-4",
			},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(pub.published))
		}
		evt := pub.published[0]
		if evt.EventType != events.EventTypeInvocationFail {
			t.Errorf("expected event type %q, got %q", events.EventTypeInvocationFail, evt.EventType)
		}
	})

	t.Run("Generic Invocation Failure event does not claim unmatched retry policy", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:           context.Background(),
			Request:       httptest.NewRequest(http.MethodPost, "/v1/messages", nil),
			OriginalModel: "claude-opus-5.2",
			Model:         "glm-5.2",
			Err:           errors.New("empty upstream stream: no content or tool calls received"),
			AttemptCount:  1,
			SelectedEndpoint: &core.Endpoint{
				ID:       "ep-glm",
				Code:     "glm-primary",
				Provider: "Kimchi",
				Model:    "glm-5.2",
			},
			Policy: &policy.Policy{
				InvocationPolicy: &policy.InvocationPolicy{
					ID:   "retry-rich",
					Name: "rich retry policy",
					RetryPolicy: &policy.RetryPolicy{
						Retry:         3,
						ErrorCodes:    []string{"429", "500", "502", "503", "504"},
						ErrorMessages: []string{"rate limit", "timeout", "overloaded"},
					},
				},
			},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(pub.published))
		}
		evt := pub.published[0]
		if evt.EventType != events.EventTypeInvocationFail {
			t.Errorf("expected event type %q, got %q", events.EventTypeInvocationFail, evt.EventType)
		}
		if evt.PolicyID != "" || evt.PolicyName != "" {
			t.Fatalf("expected no policy attribution for unmatched retry policy, got id=%q name=%q", evt.PolicyID, evt.PolicyName)
		}
		if strings.Contains(evt.Message, "retry policy matched") {
			t.Fatalf("generic invocation failure should not claim retry match, got message %q", evt.Message)
		}
		if !strings.Contains(evt.Message, "original request model: claude-opus-5.2") {
			t.Fatalf("expected original model context in message, got %q", evt.Message)
		}
	})

	t.Run("No Invocation Failure event for Inbound validation error", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:   "unknown-model-xyz",
			Err:     errors.New("unknown model: unknown-model-xyz"),
			// 没有设置 AttemptCount 或 SelectedEndpoint，表示请求在 Inbound 阶段就失败了
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 0 {
			t.Fatalf("expected 0 published events for inbound error, got %d", len(pub.published))
		}
	})

	t.Run("Invocation Failure event for failed retry attempt from history", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:           context.Background(),
			Request:       httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			OriginalModel: "glm-5.2",
			Model:         "glm-5.2",
			Policy: &policy.Policy{
				InvocationPolicy: &policy.InvocationPolicy{
					ID:   "retry-402",
					Name: "retry 402",
					RetryPolicy: &policy.RetryPolicy{
						Retry:      1,
						ErrorCodes: []string{"402"},
					},
				},
			},
			AttemptCount: 2,
			SelectedEndpoint: &core.Endpoint{
				ID:       "healthy-endpoint",
				Code:     "glm-healthy",
				Provider: "glm-provider",
				Model:    "glm-5.2",
			},
			History: []core.AttemptRecord{
				{
					Model:        "glm-5.2",
					EndpointID:   "d90c177924mt69n7b0n0",
					EndpointCode: "glm-primary",
					Provider:     "glm-provider",
					StatusCode:   402,
					Error:        "upstream error: status 402, body: {\"error\": \"the provider for model glm-5.2-fp8 has exhausted its credits and cannot process requests\"}",
					Success:      false,
					Timestamp:    time.Now(),
				},
				{
					Model:      "glm-5.2",
					EndpointID: "healthy-endpoint",
					Provider:   "glm-provider",
					StatusCode: 200,
					Success:    true,
					Timestamp:  time.Now(),
				},
			},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 2 {
			t.Fatalf("expected 2 published events, got %d", len(pub.published))
		}
		var failEvt, lbEvt *events.OpsEvent
		for _, e := range pub.published {
			if e.EventType == events.EventTypeInvocationFail {
				failEvt = e
			} else if e.EventType == events.EventTypeLBSwitch {
				lbEvt = e
			}
		}
		if failEvt == nil {
			t.Fatalf("expected invocation_fail event to be published")
		}
		if lbEvt == nil {
			t.Fatalf("expected lb_switch event to be published")
		}
		if failEvt.EndpointID != "d90c177924mt69n7b0n0" {
			t.Errorf("expected EndpointID to be failed endpoint, got %q", failEvt.EndpointID)
		}
		if failEvt.EndpointCode != "glm-primary" {
			t.Errorf("expected EndpointCode to be failed endpoint code, got %q", failEvt.EndpointCode)
		}
		if failEvt.ModelCode != "glm-5.2" {
			t.Errorf("expected ModelCode to be %q, got %q", "glm-5.2", failEvt.ModelCode)
		}
		if failEvt.PolicyID != "retry-402" {
			t.Errorf("expected PolicyID to be %q, got %q", "retry-402", failEvt.PolicyID)
		}
	})

	t.Run("Invocation Failure event with fallback mismatch correction", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		gctx := &core.GatewayContext{
			Ctx:           context.Background(),
			Request:       httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			OriginalModel: "claude-opus-4.8",
			Model:         "mimo-v2.5-pro-anthropic",
			SelectedEndpoint: &core.Endpoint{
				ID:       "ep-xiaomi-1",
				Provider: "Xiaomi",
				Model:    "mimo-v2.5-pro-anthropic",
			},
			Err: errors.New("upstream error: status 401"),
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(pub.published))
		}
		evt := pub.published[0]
		if evt.EventType != events.EventTypeInvocationFail {
			t.Errorf("expected event type %q, got %q", events.EventTypeInvocationFail, evt.EventType)
		}
		if evt.ModelCode != "mimo-v2.5-pro-anthropic" {
			t.Errorf("expected ModelCode to be %q, got %q", "mimo-v2.5-pro-anthropic", evt.ModelCode)
		}
		if evt.ProviderName != "Xiaomi" {
			t.Errorf("expected ProviderName to be %q, got %q", "Xiaomi", evt.ProviderName)
		}
		expectedMsg := "upstream error: status 401 (original request model: claude-opus-4.8)"
		if evt.Message != expectedMsg {
			t.Errorf("expected Message to be %q, got %q", expectedMsg, evt.Message)
		}
	})

	t.Run("Rate Limit event with discovery provider completion", func(t *testing.T) {
		pub := &mockPublisher{}
		f := NewEventPublishFilter(pub, nil)
		disc := &mockDiscovery{
			endpoints: []*core.Endpoint{
				{
					ID:       "ep-muyuan-1",
					Provider: "Muyuan",
					Model:    "claude-opus-4.8",
				},
			},
		}
		f.SetDiscovery(disc)

		gctx := &core.GatewayContext{
			Ctx:     context.Background(),
			Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:   "claude-opus-4.8",
			Err:     &limiter.HTTPError{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Wait for async goroutine

		if len(pub.published) != 1 {
			t.Fatalf("expected 1 published event, got %d", len(pub.published))
		}
		evt := pub.published[0]
		if evt.EventType != events.EventTypeRateLimit {
			t.Errorf("expected event type %q, got %q", events.EventTypeRateLimit, evt.EventType)
		}
		if evt.ProviderName != "Muyuan" {
			t.Errorf("expected ProviderName to be %q, got %q", "Muyuan", evt.ProviderName)
		}
		if evt.EndpointID != "ep-muyuan-1" {
			t.Errorf("expected EndpointID to be %q, got %q", "ep-muyuan-1", evt.EndpointID)
		}
	})
}
