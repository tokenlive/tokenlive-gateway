package config

import (
	"encoding/json"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// ModelConfig 模型定义（一等入口，model-centric）
type ModelConfig struct {
	RequestTypes []string         `mapstructure:"request_types" yaml:"request_types" json:"request_types"` // 可选，API 列表
	Endpoints    []EndpointConfig `mapstructure:"endpoints" yaml:"endpoints" json:"endpoints"`         // 该 model 的所有可用 endpoint
}

// ProviderConfig Provider 基础设施定义（不含 endpoints）
type ProviderConfig struct {
	Protocol   string        `mapstructure:"protocol" yaml:"protocol" json:"protocol"`       // openai / anthropic
	APIKey     string        `mapstructure:"api_key" yaml:"api_key" json:"api_key"`         // 默认 API key
	Timeout    time.Duration `mapstructure:"timeout" yaml:"timeout" json:"timeout"`         // 默认超时
	MaxRetries int           `mapstructure:"max_retries" yaml:"max_retries" json:"max_retries"` // 默认重试次数
}

// EndpointConfig endpoint 配置（挂在 model 下，引用 provider）
type EndpointConfig struct {
	ID        string            `mapstructure:"id" yaml:"id" json:"id,omitempty"`                             // 可选，端点唯一 ID
	Code      string            `mapstructure:"code" yaml:"code" json:"code,omitempty"`                         // 可选，端点业务编码
	Provider  string            `mapstructure:"provider" yaml:"provider" json:"provider"`                 // 引用 provider name（必填）
	URL       string            `mapstructure:"url" yaml:"url" json:"url"`                           // 上游地址（必填）
	RealModel string            `mapstructure:"real_model" yaml:"real_model" json:"real_model,omitempty"`             // 可选，覆盖 model 的 real_model
	APIKey    string            `mapstructure:"api_key" yaml:"api_key" json:"api_key,omitempty"`                   // 可选，覆盖 provider 的 api_key
	AuthType  string            `mapstructure:"auth_type" yaml:"auth_type" json:"auth_type,omitempty"`             // 认证类型
	Protocol  string            `mapstructure:"protocol" yaml:"protocol" json:"protocol,omitempty"`                 // 可选，覆盖 provider 的 protocol
	Timeout   time.Duration     `mapstructure:"timeout" yaml:"timeout" json:"timeout,omitempty"`                   // 可选，覆盖 provider 的 timeout
	Priority  int               `mapstructure:"priority" yaml:"priority" json:"priority"`                 // failover 优先级，值越小越优先
	Weight    int               `mapstructure:"weight" yaml:"weight" json:"weight"`                     // 同优先级内的负载均衡权重
	Headers   map[string]string `mapstructure:"headers" yaml:"headers" json:"headers,omitempty"`                   // 自定义 Header
	Metadata  map[string]string `mapstructure:"metadata" yaml:"metadata" json:"metadata,omitempty"` // 元数据
}

// GatewayConfig 网关配置（model-centric 两层结构）
type GatewayConfig struct {
	Models    map[string]ModelConfig          `mapstructure:"models" yaml:"models" json:"models"`
	Providers map[string]ProviderConfig       `mapstructure:"providers" yaml:"providers" json:"providers"`
	Fallbacks map[string][]string             `mapstructure:"fallbacks" yaml:"fallbacks" json:"fallbacks"`
	Pipelines map[string]*core.PipelineConfig `mapstructure:"pipelines" yaml:"pipelines" json:"pipelines,omitempty"`
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
	AuthType           string            `json:"auth_type,omitempty"`
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

// UnmarshalJSON 兼容 Admin/Redis 侧不同命名风格的端点 ID 与编码字段。
func (r *ResolvedEndpoint) UnmarshalJSON(data []byte) error {
	type Alias ResolvedEndpoint
	aux := &struct {
		EndpointIDSnake   string `json:"endpoint_id"`
		EndpointIDCamel   string `json:"endpointId"`
		EndpointCodeSnake string `json:"endpoint_code"`
		EndpointCodeCamel string `json:"endpointCode"`
		*Alias
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if r.ID == "" {
		if aux.EndpointIDSnake != "" {
			r.ID = aux.EndpointIDSnake
		} else if aux.EndpointIDCamel != "" {
			r.ID = aux.EndpointIDCamel
		}
	}
	if r.Code == "" {
		if aux.EndpointCodeSnake != "" {
			r.Code = aux.EndpointCodeSnake
		} else if aux.EndpointCodeCamel != "" {
			r.Code = aux.EndpointCodeCamel
		}
	}
	return nil
}

// UnmarshalJSON 允许从 JSON 中的时间字符串（如 "60s"）解析 Timeout 字段。
func (c *ProviderConfig) UnmarshalJSON(data []byte) error {
	type Alias ProviderConfig
	aux := &struct {
		Timeout interface{} `json:"timeout"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Timeout != nil {
		switch v := aux.Timeout.(type) {
		case string:
			dur, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Timeout = dur
		case float64:
			c.Timeout = time.Duration(v)
		}
	}
	return nil
}

// UnmarshalJSON 允许从 JSON 中的时间字符串（如 "60s"）解析 Timeout 字段。
func (c *EndpointConfig) UnmarshalJSON(data []byte) error {
	type Alias EndpointConfig
	aux := &struct {
		Timeout interface{} `json:"timeout"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Timeout != nil {
		switch v := aux.Timeout.(type) {
		case string:
			dur, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Timeout = dur
		case float64:
			c.Timeout = time.Duration(v)
		}
	}
	return nil
}
