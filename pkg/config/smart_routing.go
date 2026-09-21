package config

import (
	"fmt"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// RedisKeySmartRouting is the Admin/Gateway runtime smart-routing document key.
func RedisKeySmartRouting(model string) string {
	return "aigw:config:smart_routing:" + model
}

func modelSmartRouting(cfg *GatewayConfig, code string, model ModelConfig) (*core.SmartRoutingConfig, error) {
	switch model.ModelType {
	case "", "normal":
		if model.SmartRouting != nil {
			return nil, fmt.Errorf("model %s: normal model cannot contain smart_routing", code)
		}
		return nil, nil
	case "smart":
	default:
		return nil, fmt.Errorf("model %s: unknown model_type %q", code, model.ModelType)
	}
	if len(model.Endpoints) != 0 {
		return nil, fmt.Errorf("model %s: smart model cannot have direct endpoints", code)
	}
	if len(model.RequestTypes) != 1 || model.RequestTypes[0] != "chat_completion" {
		return nil, fmt.Errorf("model %s: smart model only supports chat_completion", code)
	}
	smart := model.SmartRouting.Clone()
	if err := smart.Validate(code); err != nil {
		return nil, fmt.Errorf("model %s: %w", code, err)
	}
	smart.ApplyDefaults()
	deps := []string{smart.JudgeModel}
	for _, r := range smart.Ranges {
		deps = append(deps, r.Model)
	}
	for _, dep := range deps {
		// A disabled/missing dependency is a runtime availability decision.
		for name, candidate := range cfg.Models {
			if strings.EqualFold(name, dep) && candidate.ModelType == "smart" {
				return nil, fmt.Errorf("model %s: dependency %s must be ordinary", code, dep)
			}
		}
	}
	return smart, nil
}

// cloneGatewayConfig takes ownership of mutable routing data at source boundaries.
func cloneGatewayConfig(cfg *GatewayConfig) *GatewayConfig {
	if cfg == nil {
		return nil
	}
	result := *cfg
	result.Models = make(map[string]ModelConfig, len(cfg.Models))
	for name, model := range cfg.Models {
		if model.ModelType == "" {
			model.ModelType = "normal"
		}
		model.SmartRouting = model.SmartRouting.Clone()
		if model.SmartRouting != nil {
			model.SmartRouting.ApplyDefaults()
		}
		model.RequestTypes = append([]string(nil), model.RequestTypes...)
		model.Endpoints = append([]EndpointConfig(nil), model.Endpoints...)
		for i := range model.Endpoints {
			model.Endpoints[i].Headers = cloneStringMap(model.Endpoints[i].Headers)
			model.Endpoints[i].Metadata = cloneStringMap(model.Endpoints[i].Metadata)
		}
		result.Models[name] = model
	}
	result.Providers = make(map[string]ProviderConfig, len(cfg.Providers))
	for name, provider := range cfg.Providers {
		result.Providers[name] = provider
	}
	result.Aliases = cloneStringMap(cfg.Aliases)
	result.Fallbacks = make(map[string][]string, len(cfg.Fallbacks))
	for name, targets := range cfg.Fallbacks {
		result.Fallbacks[name] = append([]string(nil), targets...)
	}
	return &result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
