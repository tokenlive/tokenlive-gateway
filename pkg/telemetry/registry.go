package telemetry

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricsRegistry holds business metric definitions and views.
type MetricsRegistry struct {
	// Business metrics
	RequestDuration metric.Float64Histogram
	RequestTotal    metric.Int64Counter
	TokensTotal     metric.Int64Counter
	CostTotal       metric.Float64Counter
	RequestTTFT     metric.Float64Histogram

	// Circuit breaker metrics
	CircuitBreakerState metric.Int64Gauge
}

// LabelContract defines metric label dimensions.
type LabelContract struct {
	Model    string // gctx.Model
	Provider string // gctx.SelectedEndpoint.Provider (empty if none)
	Status   string // "success" | "error"
	Stream   string // "true" | "false"
	Tenant   string // Tenant tag ("others" if whitelist off)
	Type     string // Token type: "input" | "output" | "cached" | "cache_creation"
	Endpoint string // Winning endpoint ID; empty when none selected

	// Cached OTel attributes
	attrs []attribute.KeyValue
}

// ToAttributes returns cached OTel attributes.
func (lc *LabelContract) ToAttributes() []attribute.KeyValue {
	if lc.attrs != nil {
		return lc.attrs
	}

	// Base labels (all metrics)
	lc.attrs = []attribute.KeyValue{
		attribute.String("model", lc.Model),
		attribute.String("provider", lc.Provider),
		attribute.String("status", lc.Status),
		attribute.String("stream", lc.Stream),
		attribute.String("tenant", lc.Tenant),
	}

	// Token type label (token metrics only)
	if lc.Type != "" {
		lc.attrs = append(lc.attrs, attribute.String("type", lc.Type))
	}

	return lc.attrs
}

// ToAttributesWithoutType omits the type label (non-token metrics).
func (lc *LabelContract) ToAttributesWithoutType() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("model", lc.Model),
		attribute.String("provider", lc.Provider),
		attribute.String("status", lc.Status),
		attribute.String("stream", lc.Stream),
		attribute.String("tenant", lc.Tenant),
	}
}

// ToRequestTotalAttributes is the counter-only label set: shared non-token
// labels plus endpoint (empty string when no endpoint was selected).
func (lc *LabelContract) ToRequestTotalAttributes() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("model", lc.Model),
		attribute.String("provider", lc.Provider),
		attribute.String("status", lc.Status),
		attribute.String("stream", lc.Stream),
		attribute.String("tenant", lc.Tenant),
		attribute.String("endpoint", lc.Endpoint),
	}
}

// NewMetricsRegistry creates the registry and registers all metrics.
func NewMetricsRegistry(provider metric.MeterProvider) (*MetricsRegistry, error) {
	meter := provider.Meter("github.com/tokenlive/tokenlive-gateway")

	// 1. Request latency histogram
	requestDuration, err := meter.Float64Histogram(
		"gateway_request_duration_seconds",
		metric.WithDescription("Request duration in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_request_duration_seconds: %w", err)
	}

	// 2. Request counter
	requestTotal, err := meter.Int64Counter(
		"gateway_request_total",
		metric.WithDescription("Total requests"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_request_total: %w", err)
	}

	// 3. Token counter
	tokensTotal, err := meter.Int64Counter(
		"gateway_tokens_total",
		metric.WithDescription("Total tokens"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_tokens_total: %w", err)
	}

	// 4. Cost counter
	costTotal, err := meter.Float64Counter(
		"gateway_cost_total",
		metric.WithDescription("Total cost in USD"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_cost_total: %w", err)
	}

	// 5. TTFT histogram
	requestTTFT, err := meter.Float64Histogram(
		"gateway_ttft_seconds",
		metric.WithDescription("Time to first token in seconds for stream requests"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_ttft_seconds: %w", err)
	}

	// 6. Circuit breaker state gauge
	circuitBreakerState, err := meter.Int64Gauge(
		"gateway_circuit_breaker_state",
		metric.WithDescription("Circuit breaker state (0=Closed, 1=Open, 2=HalfOpen)"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_circuit_breaker_state: %w", err)
	}

	return &MetricsRegistry{
		RequestDuration:     requestDuration,
		RequestTotal:        requestTotal,
		TokensTotal:         tokensTotal,
		CostTotal:           costTotal,
		RequestTTFT:         requestTTFT,
		CircuitBreakerState: circuitBreakerState,
	}, nil
}
