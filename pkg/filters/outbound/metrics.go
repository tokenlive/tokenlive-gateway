package outbound

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// MetricsFilter records request latency, count, token usage, cost, and TTFT via OpenTelemetry.
type MetricsFilter struct {
	registry  *telemetry.MetricsRegistry
	extractor MetricsExtractor
	logger    *zap.Logger
}

// NewMetricsFilter creates a MetricsFilter (explicit DI).
func NewMetricsFilter(registry *telemetry.MetricsRegistry, extractor MetricsExtractor, logger *zap.Logger) *MetricsFilter {
	return &MetricsFilter{
		registry:  registry,
		extractor: extractor,
		logger:    logger,
	}
}

func (f *MetricsFilter) Name() string                        { return "metrics" }
func (f *MetricsFilter) Order() int                          { return 30 }
func (f *MetricsFilter) Criticality() core.FilterCriticality { return core.BestEffort }
func (f *MetricsFilter) InboundSafe()                        {}

func (f *MetricsFilter) OnResponse(gctx *core.GatewayContext) error {
	// BestEffort: never block the response on errors
	defer func() {
		if r := recover(); r != nil {
			if f.logger != nil {
				f.logger.Warn("metrics filter panic recovered", zap.Any("panic", r))
			}
		}
	}()

	// defensive: graceful degradation if registry is nil
	if f.registry == nil {
		return nil
	}

	ctx := gctx.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// extract common labels (cached)
	labels := f.extractor.ExtractLabels(gctx)

	// 1. request latency histogram
	duration := time.Since(gctx.StartTime).Seconds()
	if err := f.recordMetric(func() error {
		f.registry.RequestDuration.Record(ctx, duration,
			metric.WithAttributes(labels.ToAttributesWithoutType()...))
		return nil
	}); err != nil {
		return nil
	}

		// 2. request counter (endpoint label is counter-only)
		if err := f.recordMetric(func() error {
			f.registry.RequestTotal.Add(ctx, 1,
				metric.WithAttributes(labels.ToRequestTotalAttributes()...))
			return nil
		}); err != nil {
		return nil
	}

	// 3. TTFT histogram (streaming only, when first byte received)
	if gctx.IsStream && gctx.TTFT > 0 {
		if err := f.recordMetric(func() error {
			f.registry.RequestTTFT.Record(ctx, gctx.TTFT.Seconds(),
				metric.WithAttributes(labels.ToAttributesWithoutType()...))
			return nil
		}); err != nil {
			return nil
		}
	}

	// 4. token counters (specialized labels)
	f.recordTokenMetrics(ctx, gctx)

	// 5. cost counter (only when Cost > 0)
	if gctx.Cost > 0 {
		if err := f.recordMetric(func() error {
			f.registry.CostTotal.Add(ctx, gctx.Cost,
				metric.WithAttributes(labels.ToAttributesWithoutType()...))
			return nil
		}); err != nil {
			return nil
		}
	}

	return nil
}

// recordTokenMetrics records token metrics (input/output/cached/cache_creation).
func (f *MetricsFilter) recordTokenMetrics(ctx context.Context, gctx *core.GatewayContext) {
	extractor, ok := f.extractor.(*DefaultMetricsExtractor)
	if !ok {
		return
	}

	// Input tokens
	if gctx.InputTokens > 0 {
		tokenLabels := extractor.ExtractTokenLabels(gctx, "input")
		_ = f.recordMetric(func() error {
			f.registry.TokensTotal.Add(ctx, int64(gctx.InputTokens),
				metric.WithAttributes(tokenLabels.ToAttributes()...))
			return nil
		})
	}

	// Output tokens
	if gctx.OutputTokens > 0 {
		tokenLabels := extractor.ExtractTokenLabels(gctx, "output")
		_ = f.recordMetric(func() error {
			f.registry.TokensTotal.Add(ctx, int64(gctx.OutputTokens),
				metric.WithAttributes(tokenLabels.ToAttributes()...))
			return nil
		})
	}

	// Cached tokens
	if gctx.CachedTokens > 0 {
		tokenLabels := extractor.ExtractTokenLabels(gctx, "cached")
		_ = f.recordMetric(func() error {
			f.registry.TokensTotal.Add(ctx, int64(gctx.CachedTokens),
				metric.WithAttributes(tokenLabels.ToAttributes()...))
			return nil
		})
	}

	// Cache creation tokens
	if gctx.CacheCreationTokens > 0 {
		tokenLabels := extractor.ExtractTokenLabels(gctx, "cache_creation")
		_ = f.recordMetric(func() error {
			f.registry.TokensTotal.Add(ctx, int64(gctx.CacheCreationTokens),
				metric.WithAttributes(tokenLabels.ToAttributes()...))
			return nil
		})
	}
}

// recordMetric wraps metric recording, swallowing all errors (BestEffort semantics).
func (f *MetricsFilter) recordMetric(fn func() error) error {
	if err := fn(); err != nil {
		if f.logger != nil {
			f.logger.Warn("failed to record metric", zap.Error(err))
		}
		return err
	}
	return nil
}
