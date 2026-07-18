package config

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

type ConfigManager struct {
	mu            sync.RWMutex
	yamlEndpoints map[string][]ResolvedEndpoint
	fallbacks     map[string][]string
	redisSrc      *RedisConfigSource
	logger        *zap.Logger
}

func NewConfigManager(yamlCfg *GatewayConfig, redisSrc *RedisConfigSource, logger *zap.Logger) *ConfigManager {
	return &ConfigManager{
		yamlEndpoints: Resolve(yamlCfg),
		fallbacks:     yamlCfg.Fallbacks,
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

// UpdateYAMLConfig hot-reloads in-memory static YAML config.
func (m *ConfigManager) UpdateYAMLConfig(yamlCfg *GatewayConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.yamlEndpoints = Resolve(yamlCfg)
	m.fallbacks = yamlCfg.Fallbacks
}

