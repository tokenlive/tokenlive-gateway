package outbound

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"
)

// MetricsExtractor extracts metric labels from GatewayContext.
type MetricsExtractor interface {
	ExtractLabels(gctx *core.GatewayContext) telemetry.LabelContract
}

// DefaultMetricsExtractor is the default label extractor.
type DefaultMetricsExtractor struct{}

// ExtractLabels extracts common labels from gctx.
func (e *DefaultMetricsExtractor) ExtractLabels(gctx *core.GatewayContext) telemetry.LabelContract {
	// 1. status label
	status := "success"
	if gctx.Err != nil {
		status = "error"
	}

	// 2. stream label
	stream := "false"
	if gctx.IsStream {
		stream = "true"
	}

	// 3. provider label
	provider := ""
	if gctx.SelectedEndpoint != nil {
		provider = gctx.SelectedEndpoint.Provider
	}

	// 4. tenant label (decentralized policy metadata tagging)
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
			Endpoint: winningEndpointID(gctx),
		}
}

// ExtractTokenLabels extracts token-specific labels (includes type dimension).
func (e *DefaultMetricsExtractor) ExtractTokenLabels(gctx *core.GatewayContext, tokenType string) telemetry.LabelContract {
	labels := e.ExtractLabels(gctx)
	labels.Type = tokenType
	return labels
}
