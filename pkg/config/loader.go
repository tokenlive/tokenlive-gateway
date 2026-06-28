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

// Load 从 Viper 加载 model-centric 网关配置
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

// Validate 校验配置引用完整性
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

// Resolve 将 model-centric 配置展开为扁平的 ResolvedEndpoint 列表
// 每个 endpoint 合并了 model 元数据和 provider 基础设施字段
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
			}

			// RealModel: endpoint 级别必填（无 provider 级别回退）
			re.RealModel = ep.RealModel

			// APIKey: endpoint > provider
			if ep.APIKey != "" {
				re.APIKey = ep.APIKey
			} else {
				re.APIKey = p.APIKey
			}

			// Timeout: endpoint > provider > 默认值
			if ep.Timeout > 0 {
				re.Timeout = ep.Timeout.Milliseconds()
			} else if p.Timeout > 0 {
				re.Timeout = p.Timeout.Milliseconds()
			} else {
				re.Timeout = defaultTimeout.Milliseconds()
			}

			// MaxRetries: provider > 默认值
			if p.MaxRetries > 0 {
				re.MaxRetries = p.MaxRetries
			} else {
				re.MaxRetries = defaultMaxRetries
			}

			// Weight: endpoint > 默认值
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

// KnownModels 返回所有已配置的 model_name 集合
func KnownModels(cfg *GatewayConfig) map[string]bool {
	known := make(map[string]bool, len(cfg.Models))
	for name := range cfg.Models {
		known[name] = true
	}
	return known
}
