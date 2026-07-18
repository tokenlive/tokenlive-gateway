package core

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// CircuitBreakerMetrics emits circuit breaker metrics.
type CircuitBreakerMetrics struct {
	state metric.Int64Gauge
}

// NewCircuitBreakerMetrics creates a circuit breaker metrics emitter.
func NewCircuitBreakerMetrics(state metric.Int64Gauge) *CircuitBreakerMetrics {
	return &CircuitBreakerMetrics{
		state: state,
	}
}

// RecordState records a circuit breaker state change.
func (m *CircuitBreakerMetrics) RecordState(key string, modelCode string, state CircuitState) {
	if m == nil || m.state == nil {
		return
	}

	level := "service"
	if isEndpointKey(key) {
		level = "instance"
	}

	m.state.Record(
		context.Background(),
		int64(state),
		metric.WithAttributes(
			attribute.String("entity_key", key),
			attribute.String("level", level),
			attribute.String("model", modelCode),
		),
	)
}

// isEndpointKey checks whether the key is instance-level (no colon separator).
func isEndpointKey(key string) bool {
	// endpoint IDs typically have no ":"; service keys are "provider:model"
	return !containsColon(key)
}

func containsColon(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return true
		}
	}
	return false
}
