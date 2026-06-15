package telemetry

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricsRegistry 指标注册中心，集中管理所有业务指标的定义、视图配置
type MetricsRegistry struct {
	// 业务指标
	RequestDuration metric.Float64Histogram
	RequestTotal    metric.Int64Counter
	TokensTotal     metric.Int64Counter
	CostTotal       metric.Float64Counter
	RequestTTFT     metric.Float64Histogram

	// 熔断器指标
	CircuitBreakerState metric.Int64Gauge
}

// LabelContract 指标标签契约，定义所有指标的标签维度
type LabelContract struct {
	Model    string // gctx.Model
	Provider string // gctx.SelectedEndpoint.Provider (空字符串表示未选中)
	Status   string // "success" | "error"
	Stream   string // "true" | "false"
	Tenant   string // 租户染色结果（"others" 表示未开启白名单）
	Type     string // Token 类型专用: "input" | "output" | "cached" | "cache_creation"

	// 内部缓存，避免重复转换为 OTel 属性
	attrs []attribute.KeyValue
}

// ToAttributes 转换为 OTel 属性切片，结果会被缓存
func (lc *LabelContract) ToAttributes() []attribute.KeyValue {
	if lc.attrs != nil {
		return lc.attrs
	}

	// 基础标签（所有指标通用）
	lc.attrs = []attribute.KeyValue{
		attribute.String("model", lc.Model),
		attribute.String("provider", lc.Provider),
		attribute.String("status", lc.Status),
		attribute.String("stream", lc.Stream),
		attribute.String("tenant", lc.Tenant),
	}

	// Token 类型标签（仅 Token 指标使用）
	if lc.Type != "" {
		lc.attrs = append(lc.attrs, attribute.String("type", lc.Type))
	}

	return lc.attrs
}

// ToAttributesWithoutType 返回不含 type 标签的属性切片（用于非 Token 指标）
func (lc *LabelContract) ToAttributesWithoutType() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("model", lc.Model),
		attribute.String("provider", lc.Provider),
		attribute.String("status", lc.Status),
		attribute.String("stream", lc.Stream),
		attribute.String("tenant", lc.Tenant),
	}
}

// NewMetricsRegistry 创建指标注册中心，自动配置视图和注册所有指标
func NewMetricsRegistry(provider metric.MeterProvider) (*MetricsRegistry, error) {
	meter := provider.Meter("github.com/tokenlive/tokenlive-gateway")

	// 1. 注册请求延迟直方图
	requestDuration, err := meter.Float64Histogram(
		"gateway_request_duration_seconds",
		metric.WithDescription("Request duration in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_request_duration_seconds: %w", err)
	}

	// 2. 注册请求计数器
	requestTotal, err := meter.Int64Counter(
		"gateway_request_total",
		metric.WithDescription("Total requests"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_request_total: %w", err)
	}

	// 3. 注册 Token 计数器
	tokensTotal, err := meter.Int64Counter(
		"gateway_tokens_total",
		metric.WithDescription("Total tokens"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_tokens_total: %w", err)
	}

	// 4. 注册费用计数器
	costTotal, err := meter.Float64Counter(
		"gateway_cost_total",
		metric.WithDescription("Total cost in USD"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_cost_total: %w", err)
	}

	// 5. 注册 TTFT 直方图
	requestTTFT, err := meter.Float64Histogram(
		"gateway_ttft_seconds",
		metric.WithDescription("Time to first token in seconds for stream requests"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_ttft_seconds: %w", err)
	}

	// 6. 注册熔断器状态 Gauge
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
