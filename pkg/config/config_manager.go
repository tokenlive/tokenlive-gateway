package config

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"go.uber.org/zap"
)

type ConfigManager struct {
	mu              sync.RWMutex
	yamlEndpoints   map[string][]ResolvedEndpoint
	fallbacks       map[string][]string
	aliases         map[string]string
	yamlSmart       map[string]*core.SmartRoutingConfig
	yamlSmartErrors map[string]error
	redisSrc        *RedisConfigSource
	logger          *zap.Logger
}

func NewConfigManager(yamlCfg *GatewayConfig, redisSrc *RedisConfigSource, logger *zap.Logger) *ConfigManager {
	m := &ConfigManager{
		redisSrc: redisSrc,
		logger:   logger,
	}
	m.UpdateYAMLConfig(yamlCfg)
	return m
}

// GetSmartRouting returns an owned, validated request snapshot; invalid smart
// documents return errors, never an ordinary-model fallback.
func (m *ConfigManager) GetSmartRouting(ctx context.Context, model string) (*core.SmartRoutingConfig, error) {
	if m.redisSrc != nil {
		if smart, configured, err := m.redisSmartRouting(ctx, model); configured || err != nil {
			if err != nil {
				return nil, err
			}
			return m.validateSmartDependencies(ctx, smart)
		}
	}
	m.mu.RLock()
	err := m.yamlSmartErrors[model]
	smart := m.yamlSmart[model].Clone()
	m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return m.validateSmartDependencies(ctx, smart)
}

// redisSmartRouting remembers Redis ownership when its ordinary definition
// overrides a static smart model. Normal-only models keep legacy YAML fallback.
func (m *ConfigManager) redisSmartRouting(ctx context.Context, model string) (*core.SmartRoutingConfig, bool, error) {
	smart, configured, err := m.redisSrc.GetSmartRouting(ctx, model)
	if configured {
		m.mu.RLock()
		staticSmart := m.yamlSmart[model] != nil
		m.mu.RUnlock()
		if staticSmart {
			m.redisSrc.mu.Lock()
			m.redisSrc.managedSmart[model] = true
			m.redisSrc.mu.Unlock()
		}
	}
	return smart, configured, err
}

func (m *ConfigManager) validateSmartDependencies(ctx context.Context, smart *core.SmartRoutingConfig) (*core.SmartRoutingConfig, error) {
	if smart == nil {
		return nil, nil
	}
	deps := []string{smart.JudgeModel}
	for _, target := range smart.Ranges {
		deps = append(deps, target.Model)
	}
	for _, dep := range deps {
		if m.redisSrc != nil {
			nested, configured, err := m.redisSmartRouting(ctx, dep)
			if configured {
				if nested != nil || err != nil {
					return nil, fmt.Errorf("smart routing dependency %s must be ordinary", dep)
				}
				continue
			}
		}
		m.mu.RLock()
		isNested := m.yamlSmart[dep] != nil
		m.mu.RUnlock()
		if isNested {
			return nil, fmt.Errorf("smart routing dependency %s must be ordinary", dep)
		}
	}
	return smart, nil
}

// HasModel checks only one model and does not enumerate all configured models.
func (m *ConfigManager) HasModel(ctx context.Context, model string) bool {
	smart, err := m.GetSmartRouting(ctx, model)
	if err != nil {
		return false
	}
	if smart != nil {
		return true
	}
	if m.redisSrc != nil {
		if exists, err := m.redisSrc.client.Exists(ctx, store.RedisKeyConfigEndpoints(model)).Result(); err == nil && exists > 0 {
			return true
		}
		m.redisSrc.mu.RLock()
		managed := m.redisSrc.managedSmart[model]
		m.redisSrc.mu.RUnlock()
		if managed {
			return false
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, known := m.yamlEndpoints[model]
	return known
}

// GetEndpoints returns resolved endpoints for a model.
// Prefers Redis; falls back to YAML on miss.
func (m *ConfigManager) GetEndpoints(ctx context.Context, modelCode string) []ResolvedEndpoint {
	if smart, err := m.GetSmartRouting(ctx, modelCode); smart != nil || err != nil {
		return nil
	}
	if m.redisSrc != nil {
		if endpoints, ok := m.redisSrc.GetEndpoints(ctx, modelCode); ok && len(endpoints) > 0 {
			return endpoints
		}
		m.redisSrc.mu.RLock()
		managed := m.redisSrc.managedSmart[modelCode]
		m.redisSrc.mu.RUnlock()
		if managed {
			return nil
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.yamlEndpoints[modelCode]
}

func (m *ConfigManager) AllKnownModels() map[string]bool {
	m.mu.RLock()
	known := make(map[string]bool, len(m.yamlEndpoints))
	for name := range m.yamlEndpoints {
		known[name] = true
	}
	m.mu.RUnlock()

	if m.redisSrc != nil {
		for name := range m.redisSrc.KnownModels() {
			known[name] = true
		}
		for name := range known {
			if !m.HasModel(context.Background(), name) {
				delete(known, name)
			}
		}
	}

	return known
}

// OwnerOf returns the owning provider name for a model, or "" if none.
// With multiple providers, returns the first (same order as GetEndpoints).
func (m *ConfigManager) OwnerOf(ctx context.Context, model string) string {
	eps := m.GetEndpoints(ctx, model)
	if len(eps) == 0 {
		return ""
	}
	return eps[0].ProviderName
}

// ModelCapacityOf returns context_length and max_output_tokens for a model, or (0, 0) if not configured.
func (m *ConfigManager) ModelCapacityOf(ctx context.Context, model string) (int64, int64) {
	eps := m.GetEndpoints(ctx, model)
	if len(eps) == 0 {
		return 0, 0
	}
	return eps[0].ContextLength, eps[0].MaxOutputTokens
}

func (m *ConfigManager) GetFallbacks() map[string][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fallbacks
}

func (m *ConfigManager) StartRedisPolling(ctx context.Context) {
	if m.redisSrc == nil {
		return
	}
	m.redisSrc.StartPolling(ctx)
}

// GetAlias resolves a model alias to target modelCode.
// Supports exact match followed by case-insensitive fallback.
func (m *ConfigManager) GetAlias(alias string) (string, bool) {
	if alias == "" {
		return "", false
	}
	m.mu.RLock()
	aliases := cloneStringMap(m.aliases)
	m.mu.RUnlock()
	if len(aliases) == 0 {
		return "", false
	}
	// 1. Exact match
	if target, ok := aliases[alias]; ok && target != "" && m.HasModel(context.Background(), target) {
		return target, true
	}

	// 2. Case-insensitive match
	for k, target := range aliases {
		if strings.EqualFold(k, alias) && target != "" && m.HasModel(context.Background(), target) {
			return target, true
		}
	}

	return "", false
}

// GetAliasesForModel returns all alias names configured for the specified modelCode.
func (m *ConfigManager) GetAliasesForModel(modelCode string) []string {
	if modelCode == "" {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []string
	for alias, target := range m.aliases {
		if target == modelCode || strings.EqualFold(target, modelCode) {
			result = append(result, alias)
		}
	}
	return result
}

// NormalizeModelCode checks if the given name matches a known real model name
// with case-insensitivity (e.g. "GLM-5.3" -> "glm-5.3").
func (m *ConfigManager) NormalizeModelCode(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	if m.HasModel(context.Background(), name) {
		return name, true
	}
	known := m.AllKnownModels()

	// 2. Case-insensitive match in YAML endpoints
	for knownModel := range known {
		if strings.EqualFold(knownModel, name) {
			return knownModel, true
		}
	}

	return "", false
}

// UpdateYAMLConfig hot-reloads in-memory static YAML config.
func (m *ConfigManager) UpdateYAMLConfig(yamlCfg *GatewayConfig) {
	yamlCfg = cloneGatewayConfig(yamlCfg)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.yamlEndpoints = Resolve(yamlCfg)
	m.yamlSmart = make(map[string]*core.SmartRoutingConfig)
	m.yamlSmartErrors = make(map[string]error)
	if yamlCfg != nil {
		for code, model := range yamlCfg.Models {
			smart, err := modelSmartRouting(yamlCfg, code, model)
			if err != nil {
				m.yamlSmartErrors[code] = err
			} else if smart != nil {
				m.yamlSmart[code] = smart
			}
		}
		m.fallbacks = yamlCfg.Fallbacks
		m.aliases = yamlCfg.Aliases
	} else {
		m.fallbacks = nil
		m.aliases = nil
	}
}
