package config

import (
	"encoding/json"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// ModelConfig is a model-centric model definition.
type ModelConfig struct {
	RequestTypes []string         `mapstructure:"request_types" yaml:"request_types" json:"request_types"` // Optional API list
	Endpoints    []EndpointConfig `mapstructure:"endpoints" yaml:"endpoints" json:"endpoints"`         // Endpoints for this model
}

// ProviderConfig is provider infrastructure (no endpoints).
type ProviderConfig struct {
	Protocol   string        `mapstructure:"protocol" yaml:"protocol" json:"protocol"`       // openai / anthropic
	APIKey     string        `mapstructure:"api_key" yaml:"api_key" json:"api_key"`         // Default API key
	Timeout    time.Duration `mapstructure:"timeout" yaml:"timeout" json:"timeout"`         // Default timeout
	MaxRetries int           `mapstructure:"max_retries" yaml:"max_retries" json:"max_retries"` // Default max retries
}

// EndpointConfig is under a model and references a provider.
type EndpointConfig struct {
	ID        string            `mapstructure:"id" yaml:"id" json:"id,omitempty"`                             // Optional unique endpoint ID
	Code      string            `mapstructure:"code" yaml:"code" json:"code,omitempty"`                         // Optional business code
	Provider  string            `mapstructure:"provider" yaml:"provider" json:"provider"`                 // Provider name (required)
	URL       string            `mapstructure:"url" yaml:"url" json:"url"`                           // Upstream URL (required)
	RealModel string            `mapstructure:"real_model" yaml:"real_model" json:"real_model,omitempty"`             // Optional override of model real_model
	APIKey    string            `mapstructure:"api_key" yaml:"api_key" json:"api_key,omitempty"`                   // Optional override of provider api_key
	AuthType  string            `mapstructure:"auth_type" yaml:"auth_type" json:"auth_type,omitempty"`             // Auth type
	Protocol  string            `mapstructure:"protocol" yaml:"protocol" json:"protocol,omitempty"`                 // Optional override of provider protocol
	Timeout   time.Duration     `mapstructure:"timeout" yaml:"timeout" json:"timeout,omitempty"`                   // Optional override of provider timeout
	Priority  int               `mapstructure:"priority" yaml:"priority" json:"priority"`                 // Failover priority (lower first)
	Weight    int               `mapstructure:"weight" yaml:"weight" json:"weight"`                     // LB weight within same priority
	Headers   map[string]string `mapstructure:"headers" yaml:"headers" json:"headers,omitempty"`                   // Custom headers
	Metadata  map[string]string `mapstructure:"metadata" yaml:"metadata" json:"metadata,omitempty"` // Metadata
}

// GatewayConfig is model-centric two-layer gateway config.
type GatewayConfig struct {
	Models    map[string]ModelConfig          `mapstructure:"models" yaml:"models" json:"models"`
	Providers map[string]ProviderConfig       `mapstructure:"providers" yaml:"providers" json:"providers"`
	Fallbacks map[string][]string             `mapstructure:"fallbacks" yaml:"fallbacks" json:"fallbacks"`
	Pipelines map[string]*core.PipelineConfig `mapstructure:"pipelines" yaml:"pipelines" json:"pipelines,omitempty"`
}

// ResolvedEndpoint is a flattened, self-describing endpoint.
// Timeout is in milliseconds.
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
	Timeout            int64             `json:"timeout"` // ms
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

// UnmarshalJSON accepts Admin/Redis endpoint ID/code field aliases.
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

// UnmarshalJSON parses Timeout from duration strings (e.g. "60s").
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

// UnmarshalJSON parses Timeout from duration strings (e.g. "60s").
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
