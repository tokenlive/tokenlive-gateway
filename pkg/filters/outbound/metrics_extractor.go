package outbound

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"
)

// MetricsExtractor 从 GatewayContext 提取指标标签
type MetricsExtractor interface {
	ExtractLabels(gctx *core.GatewayContext) telemetry.LabelContract
}

// DefaultMetricsExtractor 默认标签提取器实现
type DefaultMetricsExtractor struct{}

// ExtractLabels 从 gctx 提取通用标签
func (e *DefaultMetricsExtractor) ExtractLabels(gctx *core.GatewayContext) telemetry.LabelContract {
	// 1. 状态标签
	status := "success"
	if gctx.Err != nil {
		status = "error"
	}

	// 2. 流式标签
	stream := "false"
	if gctx.IsStream {
		stream = "true"
	}

	// 3. Provider 标签
	provider := ""
	if gctx.SelectedEndpoint != nil {
		provider = gctx.SelectedEndpoint.Provider
	}

	// 4. 租户染色（符合 CONTEXT.md 的去中心化策略元数据染色）
	tenant := "others"
	if gctx.Policy != nil && gctx.Policy.EnableMetricsReporting {
		if gctx.Tenant != "" {
			tenant = gctx.Tenant
		}
	}

	return telemetry.LabelContract{
		Model:    gctx.Model,
		Provider: provider,
		Status:   status,
		Stream:   stream,
		Tenant:   tenant,
	}
}

// ExtractTokenLabels 提取 Token 指标专用标签（包含 type 维度）
func (e *DefaultMetricsExtractor) ExtractTokenLabels(gctx *core.GatewayContext, tokenType string) telemetry.LabelContract {
	labels := e.ExtractLabels(gctx)
	labels.Type = tokenType
	return labels
}
