package telemetry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// InitOTelMetrics 初始化 OpenTelemetry Metrics SDK
func InitOTelMetrics(v *viper.Viper, logger *zap.Logger) (func(), error) {
	ctx := context.Background()
	var readers []metric.Reader

	// 注册全局 OTel 错误处理器，防止背景上报网络错误导致控制台刷屏
	var lastErrMu sync.Mutex
	lastErrMap := make(map[string]time.Time)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		if err == nil {
			return
		}
		errStr := err.Error()
		
		lastErrMu.Lock()
		lastTime, exists := lastErrMap[errStr]
		now := time.Now()
		if exists && now.Sub(lastTime) < 1*time.Minute {
			lastErrMu.Unlock()
			return
		}
		lastErrMap[errStr] = now
		lastErrMu.Unlock()

		logger.Warn("OpenTelemetry background error", zap.Error(err))
	}))

	// 1. Prometheus Pull Exporter
	// 只要启用了本地拉取，我们就注册 Prometheus Exporter
	// 它会自动将 OTel 指标暴露到 Prometheus 默认注册表中
	if v.GetBool("llm.enable_metrics") {
		promExporter, err := otelprom.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
		}
		readers = append(readers, promExporter)
		logger.Info("OpenTelemetry Prometheus Exporter initialized successfully")
	}

	// 2. OTLP/gRPC Push Exporter
	var otlpExporter *otlpmetricgrpc.Exporter
	var err error
	if v.GetBool("otel.metrics.enabled") {
		endpoint := v.GetString("otel.metrics.endpoint")
		if endpoint == "" {
			return nil, fmt.Errorf("otel.metrics.endpoint is required when otel.metrics.enabled is true")
		}

		opts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(endpoint),
		}
		if v.GetBool("otel.metrics.insecure") {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}

		otlpExporter, err = otlpmetricgrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
		}

		// 创建 PeriodicReader，15秒上报一次
		periodicReader := metric.NewPeriodicReader(
			otlpExporter,
			metric.WithInterval(15*time.Second),
		)
		readers = append(readers, periodicReader)
		logger.Info("OpenTelemetry OTLP/gRPC Exporter initialized successfully", zap.String("endpoint", endpoint))
	}

	if len(readers) == 0 {
		logger.Info("OpenTelemetry Metrics is disabled (no exporters configured)")
		return func() {}, nil
	}

	// 3. 配置 Histogram 边界视图 (Views)
	durationView := metric.NewView(
		metric.Instrument{Name: "gateway_request_duration_seconds"},
		metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}},
	)
	ttftView := metric.NewView(
		metric.Instrument{Name: "gateway_ttft_seconds"},
		metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0},
		}},
	)

	// 4. 初始化 MeterProvider
	var providerOpts []metric.Option
	for _, r := range readers {
		providerOpts = append(providerOpts, metric.WithReader(r))
	}
	providerOpts = append(providerOpts, metric.WithView(durationView, ttftView))

	mp := metric.NewMeterProvider(providerOpts...)
	otel.SetMeterProvider(mp)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mp.Shutdown(ctx); err != nil {
			logger.Error("failed to shutdown OpenTelemetry MeterProvider", zap.Error(err))
		} else {
			logger.Info("OpenTelemetry MeterProvider shutdown successfully")
		}
	}

	return cleanup, nil
}
