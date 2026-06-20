package outbound

import (
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// redactKey 脱敏 API Key，只保留首尾各 4 个字符
func redactKey(key string) string {
	if len(key) <= 8 {
		return "***"
	}
	return key[:4] + "***" + key[len(key)-4:]
}

// AccessLogFilter 结构化访问日志过滤器
type AccessLogFilter struct {
	logger *zap.Logger
}

// NewAccessLogFilter 创建 AccessLogFilter
func NewAccessLogFilter(logger *zap.Logger) *AccessLogFilter {
	return &AccessLogFilter{logger: logger}
}

func (f *AccessLogFilter) Name() string                        { return "access_log" }
func (f *AccessLogFilter) Order() int                          { return 40 }
func (f *AccessLogFilter) Criticality() core.FilterCriticality { return core.BestEffort }
func (f *AccessLogFilter) InboundSafe()                        {}

func (f *AccessLogFilter) OnResponse(gctx *core.GatewayContext) error {
	provider := ""
	endpointID := ""
	if gctx.SelectedEndpoint != nil {
		provider = gctx.SelectedEndpoint.Provider
		endpointID = gctx.SelectedEndpoint.ID
	}
	fields := []zap.Field{
		zap.String("original_model", gctx.OriginalModel),
		zap.String("model", gctx.Model),
		zap.String("provider", provider),
		zap.String("endpoint", endpointID),
		zap.Bool("stream", gctx.IsStream),
		zap.Duration("latency", time.Since(gctx.StartTime)),
		zap.Duration("ttft", gctx.TTFT),
		zap.Int("input_tokens", gctx.InputTokens),
		zap.Int("output_tokens", gctx.OutputTokens),
		zap.Int("cached_tokens", gctx.CachedTokens),
		zap.Int("cache_creation_tokens", gctx.CacheCreationTokens),
		zap.Float64("cost", gctx.Cost),
		zap.Int("attempts", gctx.AttemptCount),
		zap.Strings("fallback_chain", gctx.FallbackChain),
		zap.String("api_key", redactKey(gctx.APIKey)),
		zap.String("user_id", gctx.UserID),
		zap.String("session_id", gctx.SessionID),
	}
	if gctx.Err != nil {
		fields = append(fields, zap.Error(gctx.Err))
		gctx.Logger(f.logger).Error("request completed with error", fields...)
	} else {
		gctx.Logger(f.logger).Info("request completed", fields...)
	}
	return nil
}
