package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

const (
	defaultTimeout    = 60 * time.Second
	defaultMaxRetries = 3
	defaultWeight     = 1
)

// Load unmarshals model-centric gateway config from Viper.
func Load(v *viper.Viper) (*GatewayConfig, error) {
	cfg := &GatewayConfig{}
	if err := v.UnmarshalKey("models", &cfg.Models); err != nil {
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
	return cfg, nil
}

// Validate checks config reference integrity.
func Validate(cfg *GatewayConfig) error {
	for modelCode, m := range cfg.Models {
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

	for modelCode, m := range cfg.Models {
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
	known := make(map[string]bool, len(cfg.Models))
	for name := range cfg.Models {
		known[name] = true
	}
	return known
}
