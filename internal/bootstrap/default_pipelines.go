package bootstrap

import "github.com/tokenlive/tokenlive-gateway/pkg/core"

// 只补齐缺失的管线名称，显式配置的鉴权、重试和负载均衡始终优先。
func addDefaultPipelines(pipelines map[string]*core.PipelineConfig, hasAuth bool) {
	defaults := []struct {
		name string
		kind core.RequestType
	}{
		{"chat_completion", core.RequestTypeChatCompletion},
		{"embedding", core.RequestTypeEmbedding},
		{"image_generation", core.RequestTypeImageGeneration},
		{"messages", core.RequestTypeMessages},
		{"responses", core.RequestTypeResponses},
	}
	for _, d := range defaults {
		if _, exists := pipelines[d.name]; !exists {
			pipelines[d.name] = newDefaultPipeline(d.name, d.kind, hasAuth)
		}
	}
}

func newDefaultPipeline(name string, kind core.RequestType, hasAuth bool) *core.PipelineConfig {
	inbound := []string{"session_reader", "tagging", "credits_check", "rate_limit", "validate"}
	if hasAuth {
		inbound = append([]string{"auth"}, inbound...)
	}
	p := &core.PipelineConfig{
		Name: name, RequestTypes: []core.RequestType{kind},
		Invoker: core.InvokerConfig{Type: "cluster"}, InboundFilters: inbound,
		OutboundFilters:         []string{"token_settlement", "sticky_session", "metrics", "status_collector", "access_log", "event_publisher"},
		CriticalOutboundFilters: []string{"token_settlement", "sticky_session"},
	}
	// 图像生成不报告 tokens，不执行 token 结算。
	if kind == core.RequestTypeImageGeneration {
		p.OutboundFilters = []string{"metrics", "status_collector", "access_log", "event_publisher"}
		p.CriticalOutboundFilters = nil
	}
	return p
}
