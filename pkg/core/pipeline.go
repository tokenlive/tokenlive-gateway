package core

import "github.com/tokenlive/tokenlive-gateway/pkg/policy"

// Pipeline 一组 Filter + Invoker 配置的有序组合
type Pipeline struct {
	Name                    string
	RequestTypes            []RequestType
	InboundFilters          []InboundFilter
	OutboundFilters         []OutboundFilter
	CriticalOutboundFilters map[string]bool
	Invoker                 Invoker            // 默认 Invoker
	Invokers                map[string]Invoker // 多态运行时 Invoker 注册表
}

// PipelineConfig Pipeline 配置
type PipelineConfig struct {
	Name                    string        `yaml:"name" json:"name" mapstructure:"name"`
	RequestTypes            []RequestType `yaml:"request_types" json:"request_types" mapstructure:"request_types"`
	InboundFilters          []string      `yaml:"inbound_filters" json:"inbound_filters" mapstructure:"inbound_filters"`
	OutboundFilters         []string      `yaml:"outbound_filters" json:"outbound_filters" mapstructure:"outbound_filters"`
	CriticalOutboundFilters []string      `yaml:"critical_outbound_filters" json:"critical_outbound_filters" mapstructure:"critical_outbound_filters"`
	Invoker                 InvokerConfig `yaml:"invoker" json:"invoker" mapstructure:"invoker"`
}

// InvokerConfig Invoker 配置
type InvokerConfig struct {
	Type         string       `yaml:"type" json:"type" mapstructure:"type"`                            // "cluster" or "fallback"
	Routers      []string     `yaml:"routers" json:"routers" mapstructure:"routers"`                   // Router 名称列表，如 ["capability", "tag", "circuit_breaker"]，默认 [capability, circuit_breaker]
	LoadBalancer string       `yaml:"load_balancer" json:"load_balancer" mapstructure:"load_balancer"` // 负载均衡器策略，如 "round_robin"
	Retry        *RetryConfig `yaml:"retry" json:"retry" mapstructure:"retry"`                         // 重试配置
}

// RetryConfig 重试策略配置（已与 policy.RetryPolicy 结构对齐）
type RetryConfig struct {
	Retry          int                       `yaml:"retry" json:"retry" mapstructure:"retry"`                               // 重试次数
	BackoffType    string                    `yaml:"backoff_type" json:"backoff_type" mapstructure:"backoff_type"`          // 退避类型 (e.g. "fixed", "exponential")
	BaseMs         int                       `yaml:"base_ms" json:"base_ms" mapstructure:"base_ms"`                         // 退避间隔 (毫秒)
	ErrorCodes     []string                  `yaml:"error_codes" json:"error_codes" mapstructure:"error_codes"`             // 需要重试的错误码/状态码列表
	ErrorMessages  []string                  `yaml:"error_messages" json:"error_messages" mapstructure:"error_messages"`    // 需要重试的错误消息列表
	CodePolicy     *policy.ErrorParserPolicy `yaml:"code_policy" json:"code_policy" mapstructure:"code_policy"`             // 错误码解析策略
	MessagePolicy  *policy.ErrorParserPolicy `yaml:"message_policy" json:"message_policy" mapstructure:"message_policy"`    // 错误消息解析策略
	ConnectTimeout int                       `yaml:"connect_timeout" json:"connect_timeout" mapstructure:"connect_timeout"` // 建立连接超时 (毫秒)
	TtftTimeout    int                       `yaml:"ttft_timeout" json:"ttft_timeout" mapstructure:"ttft_timeout"`          // 首字超时 (毫秒)
	TotalTimeout   int                       `yaml:"total_timeout" json:"total_timeout" mapstructure:"total_timeout"`       // 请求总超时 (毫秒)
}

// EngineConfig Engine 配置
type EngineConfig struct {
	Pipelines map[string]*PipelineConfig `yaml:"pipelines"`
	Providers map[string]*ProviderConfig `yaml:"providers"`
}
