package core

import "github.com/tokenlive/tokenlive-gateway/pkg/policy"

// Pipeline is an ordered set of filters plus invoker config.
type Pipeline struct {
	Name                    string
	RequestTypes            []RequestType
	InboundFilters          []InboundFilter
	OutboundFilters         []OutboundFilter
	CriticalOutboundFilters map[string]bool
	Invoker                 Invoker            // Default invoker
	Invokers                map[string]Invoker // Runtime invoker registry by key
}

// PipelineConfig configures a pipeline.
type PipelineConfig struct {
	Name                    string        `yaml:"name" json:"name" mapstructure:"name"`
	RequestTypes            []RequestType `yaml:"request_types" json:"request_types" mapstructure:"request_types"`
	InboundFilters          []string      `yaml:"inbound_filters" json:"inbound_filters" mapstructure:"inbound_filters"`
	OutboundFilters         []string      `yaml:"outbound_filters" json:"outbound_filters" mapstructure:"outbound_filters"`
	CriticalOutboundFilters []string      `yaml:"critical_outbound_filters" json:"critical_outbound_filters" mapstructure:"critical_outbound_filters"`
	Invoker                 InvokerConfig `yaml:"invoker" json:"invoker" mapstructure:"invoker"`
}

// InvokerConfig configures an invoker.
type InvokerConfig struct {
	Type         string       `yaml:"type" json:"type" mapstructure:"type"`                            // "cluster" or "fallback"
	Routers      []string     `yaml:"routers" json:"routers" mapstructure:"routers"`                   // Router names; default [capability, circuit_breaker]
	LoadBalancer string       `yaml:"load_balancer" json:"load_balancer" mapstructure:"load_balancer"` // Load balancer strategy, e.g. "round_robin"
	Retry        *RetryConfig `yaml:"retry" json:"retry" mapstructure:"retry"`                         // Retry config
}

// RetryConfig aligns with policy.RetryPolicy.
type RetryConfig struct {
	Retry          int                       `yaml:"retry" json:"retry" mapstructure:"retry"`                               // Retry count
	BackoffType    string                    `yaml:"backoff_type" json:"backoff_type" mapstructure:"backoff_type"`          // Backoff type (e.g. "fixed", "exponential")
	BaseMs         int                       `yaml:"base_ms" json:"base_ms" mapstructure:"base_ms"`                         // Backoff base (ms)
	ErrorCodes     []string                  `yaml:"error_codes" json:"error_codes" mapstructure:"error_codes"`             // Retryable error/status codes
	ErrorMessages  []string                  `yaml:"error_messages" json:"error_messages" mapstructure:"error_messages"`    // Retryable error message patterns
	CodePolicy     *policy.ErrorParserPolicy `yaml:"code_policy" json:"code_policy" mapstructure:"code_policy"`             // Error code parse policy
	MessagePolicy  *policy.ErrorParserPolicy `yaml:"message_policy" json:"message_policy" mapstructure:"message_policy"`    // Error message parse policy
	ConnectTimeout int                       `yaml:"connect_timeout" json:"connect_timeout" mapstructure:"connect_timeout"` // Connect timeout (ms)
	TtftTimeout    int                       `yaml:"ttft_timeout" json:"ttft_timeout" mapstructure:"ttft_timeout"`          // TTFT timeout (ms)
	TotalTimeout   int                       `yaml:"total_timeout" json:"total_timeout" mapstructure:"total_timeout"`       // Total request timeout (ms)
}

// EngineConfig configures the engine.
type EngineConfig struct {
	Pipelines map[string]*PipelineConfig `yaml:"pipelines"`
	Providers map[string]*ProviderConfig `yaml:"providers"`
}
