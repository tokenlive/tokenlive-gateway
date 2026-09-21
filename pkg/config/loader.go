package config

import (
	"fmt"
	"reflect"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

const (
	defaultTimeout    = 60 * time.Second
	defaultMaxRetries = 3
	defaultWeight     = 1
)

// Load unmarshals model-centric gateway config from Viper.
func Load(v *viper.Viper) (*GatewayConfig, error) {
	cfg := &GatewayConfig{}
	if err := v.UnmarshalKey("models", &cfg.Models, func(decoder *mapstructure.DecoderConfig) {
		decoder.DecodeHook = mapstructure.ComposeDecodeHookFunc(decodeSmartRoutingStrictly, decoder.DecodeHook)
	}); err != nil {
		return nil, fmt.Errorf("unmarshal models: %w", err)
	}
	if err := v.UnmarshalKey("providers", &cfg.Providers); err != nil {
		return nil, fmt.Errorf("unmarshal providers: %w", err)
	}
	if err := v.UnmarshalKey("fallbacks", &cfg.Fallbacks); err != nil {
		return nil, fmt.Errorf("unmarshal fallbacks: %w", err)
	}
	if err := v.UnmarshalKey("pipelines", &cfg.Pipelines); err != nil {
		return nil, fmt.Errorf("unmarshal pipelines: %w", err)
	}
	if err := v.UnmarshalKey("aliases", &cfg.Aliases); err != nil {
		return nil, fmt.Errorf("unmarshal aliases: %w", err)
	}
	for name, model := range cfg.Models {
		if model.ModelType == "" {
			model.ModelType = "normal"
		}
		if model.SmartRouting != nil {
			model.SmartRouting.ApplyDefaults()
		}
		cfg.Models[name] = model
	}
	return cfg, nil
}

// decodeSmartRoutingStrictly isolates the smart document from Viper's legacy
// weak conversions. Mapstructure otherwise truncates floats even when weak
// input is disabled; reject them before any integer destination is assigned.
func decodeSmartRoutingStrictly(_ reflect.Type, target reflect.Type, data any) (any, error) {
	if target != reflect.TypeOf(core.SmartRoutingConfig{}) {
		return data, nil
	}
	var smart core.SmartRoutingConfig
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result: &smart,
		DecodeHook: func(source reflect.Type, destination reflect.Type, value any) (any, error) {
			if source != nil && (destination.Kind() == reflect.Int || destination.Kind() == reflect.Int64) {
				switch source.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				default:
					return nil, fmt.Errorf("smart_routing numeric fields must be integers, got %s", source)
				}
			}
			return value, nil
		},
		TagName: "mapstructure",
	})
	if err != nil {
		return nil, err
	}
	if err := decoder.Decode(data); err != nil {
		return nil, err
	}
	return smart, nil
}

// Validate checks config reference integrity.
func Validate(cfg *GatewayConfig) error {
	if cfg == nil {
		return nil
	}
	for modelCode, m := range cfg.Models {
		if _, err := modelSmartRouting(cfg, modelCode, m); err != nil {
			return err
		}
		if len(m.RequestTypes) == 0 {
			return fmt.Errorf("model %s: requestTypes are required and cannot be empty", modelCode)
		}
		for i, ep := range m.Endpoints {
			if ep.Provider == "" {
				return fmt.Errorf("model %s endpoint[%d]: provider is required", modelCode, i)
			}
			if ep.URL == "" {
				return fmt.Errorf("model %s endpoint[%d]: url is required", modelCode, i)
			}
			if _, ok := cfg.Providers[ep.Provider]; !ok {
				return fmt.Errorf("model %s endpoint[%d]: references unknown provider: %s", modelCode, i, ep.Provider)
			}
		}
	}
	return nil
}

// Resolve flattens model-centric config into ResolvedEndpoint lists,
// merging model metadata with provider infrastructure fields.
func Resolve(cfg *GatewayConfig) map[string][]ResolvedEndpoint {
	resolved := make(map[string][]ResolvedEndpoint)
	if cfg == nil {
		return resolved
	}

	for modelCode, m := range cfg.Models {
		if _, err := modelSmartRouting(cfg, modelCode, m); err != nil {
			continue
		}
		var eps []ResolvedEndpoint
		for _, ep := range m.Endpoints {
			p := cfg.Providers[ep.Provider]

			// Protocol: endpoint > provider
			var protocol string
			if ep.Protocol != "" {
				protocol = ep.Protocol
			} else {
				protocol = p.Protocol
			}

			authType := ep.AuthType
			if authType == "" {
				authType = "api_key"
			}

			contextLength := ep.ContextLength
			if contextLength == 0 {
				contextLength = m.ContextLength
			}
			maxOutputTokens := ep.MaxOutputTokens
			if maxOutputTokens == 0 {
				maxOutputTokens = m.MaxOutputTokens
			}

			re := ResolvedEndpoint{
				ID:               ep.ID,
				Code:             ep.Code,
				ProviderName:     ep.Provider,
				ProviderCode:     ep.Provider,
				ProviderProtocol: protocol,
				URL:              ep.URL,
				Priority:         ep.Priority,
				Headers:          ep.Headers,
				RequestTypes:     m.RequestTypes,
				Metadata:         ep.Metadata,
				AuthType:         authType,
				ContextLength:    contextLength,
				MaxOutputTokens:  maxOutputTokens,
			}

			// RealModel is required at endpoint level (no provider fallback).
			re.RealModel = ep.RealModel

			// APIKey: endpoint > provider
			if ep.APIKey != "" {
				re.APIKey = ep.APIKey
			} else {
				re.APIKey = p.APIKey
			}

			// Timeout: endpoint > provider > default
			if ep.Timeout > 0 {
				re.Timeout = ep.Timeout.Milliseconds()
			} else if p.Timeout > 0 {
				re.Timeout = p.Timeout.Milliseconds()
			} else {
				re.Timeout = defaultTimeout.Milliseconds()
			}

			// MaxRetries: provider > default
			if p.MaxRetries > 0 {
				re.MaxRetries = p.MaxRetries
			} else {
				re.MaxRetries = defaultMaxRetries
			}

			// Weight: endpoint > default
			if ep.Weight > 0 {
				re.Weight = ep.Weight
			} else {
				re.Weight = defaultWeight
			}

			eps = append(eps, re)
		}
		resolved[modelCode] = eps
	}

	return resolved
}

// KnownModels returns the set of configured model names.
func KnownModels(cfg *GatewayConfig) map[string]bool {
	known := make(map[string]bool)
	if cfg != nil {
		for name, model := range cfg.Models {
			if _, err := modelSmartRouting(cfg, name, model); err == nil {
				known[name] = true
			}
		}
	}
	return known
}
