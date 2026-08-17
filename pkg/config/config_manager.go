package config

import (
	"context"
	"strings"
	"sync"

	"go.uber.org/zap"
)

type ConfigManager struct {
	mu            sync.RWMutex
	yamlEndpoints map[string][]ResolvedEndpoint
	fallbacks     map[string][]string
	aliases       map[string]string
	redisSrc      *RedisConfigSource
	logger        *zap.Logger
}

func NewConfigManager(yamlCfg *GatewayConfig, redisSrc *RedisConfigSource, logger *zap.Logger) *ConfigManager {
	var aliases map[string]string
	var fallbacks map[string][]string
	if yamlCfg != nil {
		fallbacks = yamlCfg.Fallbacks
		aliases = yamlCfg.Aliases
	}
	return &ConfigManager{
		yamlEndpoints: Resolve(yamlCfg),
		fallbacks:     fallbacks,
		aliases:       aliases,
		redisSrc:      redisSrc,
		logger:        logger,
	}
}

// GetEndpoints returns resolved endpoints for a model.
// Prefers Redis; falls back to YAML on miss.
func (m *ConfigManager) GetEndpoints(ctx context.Context, modelCode string) []ResolvedEndpoint {
	if m.redisSrc != nil {
		if endpoints, ok := m.redisSrc.GetEndpoints(ctx, modelCode); ok && len(endpoints) > 0 {
			return endpoints
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
	defer m.mu.RUnlock()

	if len(m.aliases) == 0 {
		return "", false
	}

	// 1. Exact match
	if target, ok := m.aliases[alias]; ok && target != "" {
		return target, true
	}

	// 2. Case-insensitive match
	for k, target := range m.aliases {
		if strings.EqualFold(k, alias) && target != "" {
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
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 1. Exact match in YAML endpoints
	if _, ok := m.yamlEndpoints[name]; ok {
		return name, true
	}

	// 2. Case-insensitive match in YAML endpoints
	for knownModel := range m.yamlEndpoints {
		if strings.EqualFold(knownModel, name) {
			return knownModel, true
		}
	}

	// 3. Check Redis known models if available
	if m.redisSrc != nil {
		for knownModel := range m.redisSrc.KnownModels() {
			if strings.EqualFold(knownModel, name) {
				return knownModel, true
			}
		}
	}

	return "", false
}

// UpdateYAMLConfig hot-reloads in-memory static YAML config.
func (m *ConfigManager) UpdateYAMLConfig(yamlCfg *GatewayConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.yamlEndpoints = Resolve(yamlCfg)
	if yamlCfg != nil {
		m.fallbacks = yamlCfg.Fallbacks
		m.aliases = yamlCfg.Aliases
	} else {
		m.fallbacks = nil
		m.aliases = nil
	}
}

