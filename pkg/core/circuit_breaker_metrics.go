package core

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// CircuitBreakerMetrics 熔断器指标发射器
type CircuitBreakerMetrics struct {
	state metric.Int64Gauge
}

// NewCircuitBreakerMetrics 创建熔断器指标发射器
func NewCircuitBreakerMetrics(state metric.Int64Gauge) *CircuitBreakerMetrics {
	return &CircuitBreakerMetrics{
		state: state,
	}
}

// RecordState 记录熔断器状态变更
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

// isEndpointKey 判断是否为 instance 级别的熔断器 key
// 规则：包含 "endpoint-" 前缀或无 ":" 分隔符
func isEndpointKey(key string) bool {
	// endpoint ID 格式通常不包含 ":"，而 service key 格式为 "provider:model"
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
