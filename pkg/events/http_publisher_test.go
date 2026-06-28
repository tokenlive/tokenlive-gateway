package events

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPPublisher_Publish(t *testing.T) {
	var receivedEvent OpsEvent
	var receivedToken string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedToken = r.Header.Get("X-Sync-Token")
		if r.URL.Path == "/api/v1/gateway/events" && r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&receivedEvent)
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	pub := NewHTTPPublisher(ts.URL, "test-event-token")
	defer pub.Close()

	event := &OpsEvent{
		EventType:    EventTypeCircuitBreak,
		TenantCode:   "test-tenant",
		ModelCode:    "gpt-4",
		EndpointID:   "ep-123",
		EndpointCode: "ep-code",
		ProviderName: "openai",
		PolicyID:     "pol-123",
		PolicyName:   "test-policy",
		RequestID:    "req-123",
		TraceID:      "trace-123",
		Message:      "circuit breaker opened",
		Timestamp:    time.Now().Unix(),
	}

	err := pub.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if receivedToken != "test-event-token" {
		t.Errorf("expected token 'test-event-token', got %q", receivedToken)
	}
	if receivedEvent.EventType != EventTypeCircuitBreak || receivedEvent.TenantCode != "test-tenant" {
		t.Errorf("unexpected event payload: %+v", receivedEvent)
	}
}
