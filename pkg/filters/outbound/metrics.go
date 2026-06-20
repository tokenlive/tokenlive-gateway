package outbound

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// MetricsFilter OpenTelemetry 指标过滤器，记录请求耗时、请求计数、token 用量、费用和首包延迟
type MetricsFilter struct {
	registry  *telemetry.MetricsRegistry
	extractor MetricsExtractor
	logger    *zap.Logger
}

// NewMetricsFilter 创建 MetricsFilter（显式依赖注入）
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
	// BestEffort 语义：即使出错也不能阻塞响应
	defer func() {
		if r := recover(); r != nil {
			if f.logger != nil {
				f.logger.Warn("metrics filter panic recovered", zap.Any("panic", r))
			}
		}
	}()

	// 防御性检查：如果 registry 未初始化，优雅降级
	if f.registry == nil {
		return nil
	}

	ctx := gctx.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	// 提取通用标签（复用缓存）
	labels := f.extractor.ExtractLabels(gctx)

	// 1. 请求延迟直方图
	duration := time.Since(gctx.StartTime).Seconds()
	if err := f.recordMetric(func() error {
		f.registry.RequestDuration.Record(ctx, duration,
			metric.WithAttributes(labels.ToAttributesWithoutType()...))
		return nil
	}); err != nil {
		return nil
	}

	// 2. 请求计数器
	if err := f.recordMetric(func() error {
		f.registry.RequestTotal.Add(ctx, 1,
			metric.WithAttributes(labels.ToAttributesWithoutType()...))
		return nil
	}); err != nil {
		return nil
	}

	// 3. TTFT 直方图（仅流式且有首包时）
	if gctx.IsStream && gctx.TTFT > 0 {
		if err := f.recordMetric(func() error {
			f.registry.RequestTTFT.Record(ctx, gctx.TTFT.Seconds(),
				metric.WithAttributes(labels.ToAttributesWithoutType()...))
			return nil
		}); err != nil {
			return nil
		}
	}

	// 4. Token 计数器（使用专用标签）
	f.recordTokenMetrics(ctx, gctx)

	// 5. 费用计数器（仅当 Cost > 0 时）
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

// recordTokenMetrics 记录 Token 指标（input/output/cached/cache_creation）
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

// recordMetric 包装指标记录逻辑，吞掉所有错误（BestEffort 语义）
func (f *MetricsFilter) recordMetric(fn func() error) error {
	if err := fn(); err != nil {
		if f.logger != nil {
			f.logger.Warn("failed to record metric", zap.Error(err))
		}
		return err
	}
	return nil
}
