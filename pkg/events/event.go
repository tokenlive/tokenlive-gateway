package events

import "context"

// Event types
const (
	EventTypeCircuitBreak     = "circuit_break"
	EventTypeRateLimit        = "rate_limit"
	EventTypeInvocationFail   = "invocation_fail"
	EventTypeModelFailover    = "model_failover"
	EventTypeEndpointFailover = "endpoint_failover"
)

// Event represents a policy execution event to be published.
type OpsEvent struct {
	EventType    string  `json:"event_type"`
	TenantCode   string  `json:"tenant_code"`
	ModelCode    string  `json:"model_code"`
	EndpointID   string  `json:"endpoint_id"`
	EndpointCode string  `json:"endpoint_code"`
	ProviderName string  `json:"provider_name"`
	PolicyID     string  `json:"policy_id"`
	PolicyName   string  `json:"policy_name"`
	Threshold    *float64 `json:"threshold,omitempty"`
	CurrentValue *float64 `json:"current_value,omitempty"`
	RequestID    string  `json:"request_id"`
	TraceID      string  `json:"trace_id"`
	Message      string  `json:"message"`
	Timestamp    int64   `json:"ts"`
}

// Publisher abstracts the event transport.
type Publisher interface {
	// Publish sends an event to the configured transport.
	Publish(ctx context.Context, event *OpsEvent) error
	// Close releases transport resources.
	Close() error
}
