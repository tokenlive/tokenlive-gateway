package outbound

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/limiter"
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
			Ctx:     context.Background(),
			Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:   "gpt-4",
			Err:     errors.New("some network error"),
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
}
