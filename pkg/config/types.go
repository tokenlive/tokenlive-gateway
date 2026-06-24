package config

import (
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// ModelConfig 模型定义（一等入口，model-centric）
type ModelConfig struct {
	RequestTypes []string         `mapstructure:"request_types" yaml:"request_types"` // 可选，API 列表
	Endpoints    []EndpointConfig `mapstructure:"endpoints" yaml:"endpoints"`         // 该 model 的所有可用 endpoint
}

// ProviderConfig Provider 基础设施定义（不含 endpoints）
type ProviderConfig struct {
	Protocol   string        `mapstructure:"protocol" yaml:"protocol"`       // openai / anthropic
	APIKey     string        `mapstructure:"api_key" yaml:"api_key"`         // 默认 API key
	Timeout    time.Duration `mapstructure:"timeout" yaml:"timeout"`         // 默认超时
	MaxRetries int           `mapstructure:"max_retries" yaml:"max_retries"` // 默认重试次数
}

// EndpointConfig endpoint 配置（挂在 model 下，引用 provider）
type EndpointConfig struct {
	Provider  string            `mapstructure:"provider" yaml:"provider"`                 // 引用 provider name（必填）
	URL       string            `mapstructure:"url" yaml:"url"`                           // 上游地址（必填）
	RealModel string            `mapstructure:"real_model" yaml:"real_model"`             // 可选，覆盖 model 的 real_model
	APIKey    string            `mapstructure:"api_key" yaml:"api_key"`                   // 可选，覆盖 provider 的 api_key
	Protocol  string            `mapstructure:"protocol" yaml:"protocol"`                 // 可选，覆盖 provider 的 protocol
	Timeout   time.Duration     `mapstructure:"timeout" yaml:"timeout"`                   // 可选，覆盖 provider 的 timeout
	Priority  int               `mapstructure:"priority" yaml:"priority"`                 // failover 优先级，值越小越优先
	Weight    int               `mapstructure:"weight" yaml:"weight"`                     // 同优先级内的负载均衡权重
	Headers   map[string]string `mapstructure:"headers" yaml:"headers"`                   // 自定义 Header
	Metadata  map[string]string `mapstructure:"metadata" yaml:"metadata" json:"metadata"` // 元数据
}

// GatewayConfig 网关配置（model-centric 两层结构）
type GatewayConfig struct {
	Models    map[string]ModelConfig          `mapstructure:"models" yaml:"models"`
	Providers map[string]ProviderConfig       `mapstructure:"providers" yaml:"providers"`
	Fallbacks map[string][]string             `mapstructure:"fallbacks" yaml:"fallbacks"`
	Pipelines map[string]*core.PipelineConfig `mapstructure:"pipelines" yaml:"pipelines"`
}

// ResolvedEndpoint 解析后的 endpoint（扁平化，每个 endpoint 自描述完整路由信息）
// timeout 字段为毫秒
type ResolvedEndpoint struct {
	ID                 string            `json:"id,omitempty"`
	Code               string            `json:"code,omitempty"`
	Description        string            `json:"description,omitempty"`
	RealModel          string            `json:"real_model"`
	ProviderName       string            `json:"provider_name"`
	ProviderProtocol   string            `json:"provider_protocol"`
	APIKey             string            `json:"api_key"`
	URL                string            `json:"url"`
	Timeout            int64             `json:"timeout"` // 毫秒
	MaxRetries         int               `json:"max_retries"`
	Priority           int               `json:"priority"`
	Weight             int               `json:"weight"`
	Headers            map[string]string `json:"headers,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	RequestTypes       []string          `mapstructure:"request_types" json:"request_types,omitempty"`
	InputPrice         *float64          `json:"input_price,omitempty"`
	OutputPrice        *float64          `json:"output_price,omitempty"`
	CachedPrice        *float64          `json:"cached_price,omitempty"`
	CacheCreationPrice *float64          `json:"cache_creation_price,omitempty"`
}
