package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	defaultPollInterval = 10 * time.Second
)

type RedisConfigSource struct {
	client       redis.Cmdable
	pollInterval time.Duration
	logger       *zap.Logger

	mu           sync.RWMutex
	cache        map[string][]ResolvedEndpoint
	lastVersions map[string]int64
	smartCache   map[string]*core.SmartRoutingConfig
	smartErrors  map[string]error
	managedSmart map[string]bool
}

func NewRedisConfigSource(client redis.Cmdable, pollInterval time.Duration, logger *zap.Logger) *RedisConfigSource {
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	return &RedisConfigSource{
		client:       client,
		pollInterval: pollInterval,
		logger:       logger,
		cache:        make(map[string][]ResolvedEndpoint),
		lastVersions: make(map[string]int64),
		smartCache:   make(map[string]*core.SmartRoutingConfig),
		smartErrors:  make(map[string]error),
		managedSmart: make(map[string]bool),
	}
}

// GetSmartRouting also distinguishes a Redis-owned normal/deleted model from
// a miss, preventing stale static smart definitions from surviving a type change.
// Each read checks Redis, so version changes and deletes take effect together;
// returned ranges never alias either the cache or another request.
func (r *RedisConfigSource) GetSmartRouting(ctx context.Context, modelCode string) (*core.SmartRoutingConfig, bool, error) {
	pipe := r.client.Pipeline()
	smartCmd := pipe.Get(ctx, RedisKeySmartRouting(modelCode))
	versionCmd := pipe.HGet(ctx, store.RedisKeyConfigModelVersions, modelCode)
	endpointCmd := pipe.Exists(ctx, store.RedisKeyConfigEndpoints(modelCode))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		r.mu.RLock()
		defer r.mu.RUnlock()
		if cached := r.smartCache[modelCode]; cached != nil {
			return cached.Clone(), true, nil
		}
		if invalid := r.smartErrors[modelCode]; invalid != nil {
			return nil, true, invalid
		}
		return nil, r.managedSmart[modelCode], nil
	}
	if smartCmd.Err() == redis.Nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.smartCache, modelCode)
		delete(r.smartErrors, modelCode)
		return nil, endpointCmd.Val() > 0 || r.managedSmart[modelCode], nil
	}
	if err := smartCmd.Err(); err != nil {
		return nil, true, fmt.Errorf("model %s: read smart_routing: %w", modelCode, err)
	}

	var smart core.SmartRoutingConfig
	data, _ := smartCmd.Bytes()
	err := json.Unmarshal(data, &smart)
	if err == nil {
		err = smart.Validate(modelCode)
	}
	if err == nil {
		smart.ApplyDefaults()
		err = r.validateSmartDependencies(ctx, &smart)
	}
	version, _ := strconv.ParseInt(versionCmd.Val(), 10, 64)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.managedSmart[modelCode] = true
	r.lastVersions[modelCode] = version
	delete(r.cache, modelCode)
	if err != nil {
		err = fmt.Errorf("model %s: invalid smart_routing: %w", modelCode, err)
		delete(r.smartCache, modelCode)
		r.smartErrors[modelCode] = err
		return nil, true, err
	}
	delete(r.smartErrors, modelCode)
	r.smartCache[modelCode] = smart.Clone()
	return smart.Clone(), true, nil
}

func (r *RedisConfigSource) validateSmartDependencies(ctx context.Context, smart *core.SmartRoutingConfig) error {
	deps := []string{smart.JudgeModel}
	for _, target := range smart.Ranges {
		deps = append(deps, target.Model)
	}
	pipe := r.client.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(deps))
	for _, dep := range deps {
		cmds[dep] = pipe.Exists(ctx, RedisKeySmartRouting(dep))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for dep, cmd := range cmds {
		if cmd.Val() > 0 {
			return fmt.Errorf("dependency %s must be ordinary", dep)
		}
	}
	return nil
}

// GetEndpoints returns resolved endpoints for a model (cache or Redis).
// modelCode should be a model_code.
func (r *RedisConfigSource) GetEndpoints(ctx context.Context, modelCode string) ([]ResolvedEndpoint, bool) {
	r.mu.RLock()
	if endpoints, ok := r.cache[modelCode]; ok {
		r.mu.RUnlock()
		return endpoints, true
	}
	r.mu.RUnlock()

	endpoints, version, err := r.fetchFromRedis(ctx, modelCode)
	if err != nil {
		r.logger.Warn("fetch endpoints from redis failed",
			zap.String("modelCode", modelCode),
			zap.Error(err),
		)
		return nil, false
	}

	r.mu.Lock()
	r.cache[modelCode] = endpoints
	r.lastVersions[modelCode] = version
	r.mu.Unlock()

	return endpoints, true
}

func (r *RedisConfigSource) fetchFromRedis(ctx context.Context, modelCode string) ([]ResolvedEndpoint, int64, error) {
	pipe := r.client.Pipeline()
	endpointsCmd := pipe.Get(ctx, store.RedisKeyConfigEndpoints(modelCode))
	versionCmd := pipe.HGet(ctx, store.RedisKeyConfigModelVersions, modelCode)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, 0, err
	}

	// Missing endpoints key → redis.Nil (cache miss).
	if err := endpointsCmd.Err(); err != nil {
		return nil, 0, err
	}

	var endpoints []ResolvedEndpoint
	data, _ := endpointsCmd.Bytes()
	if len(data) > 0 {
		if err := json.Unmarshal(data, &endpoints); err != nil {
			return nil, 0, err
		}
	}

	var version int64
	vStr, err := versionCmd.Result()
	if err == nil && vStr != "" {
		if parsed, err := strconv.ParseInt(vStr, 10, 64); err == nil {
			version = parsed
		}
	}

	return endpoints, version, nil
}

func (r *RedisConfigSource) StartPolling(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.checkVersion(ctx)
		}
	}
}

func (r *RedisConfigSource) checkVersion(ctx context.Context) {
	activeModels := r.KnownModelsList()
	if len(activeModels) == 0 {
		return
	}

	versions, err := r.client.HMGet(ctx, store.RedisKeyConfigModelVersions, activeModels...).Result()
	if err != nil {
		r.logger.Warn("check config versions failed", zap.Error(err))
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i, model := range activeModels {
		var remoteVer int64
		if i < len(versions) && versions[i] != nil {
			if vStr, ok := versions[i].(string); ok {
				remoteVer, _ = strconv.ParseInt(vStr, 10, 64)
			}
		}

		localVer := r.lastVersions[model]
		if remoteVer != localVer {
			r.logger.Info("model config version changed, evicting cache",
				zap.String("model", model),
				zap.Int64("old", localVer),
				zap.Int64("new", remoteVer),
			)
			delete(r.cache, model)
			delete(r.smartCache, model)
			delete(r.smartErrors, model)
			delete(r.lastVersions, model)
		}
	}
}

func (r *RedisConfigSource) KnownModels() map[string]bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// List models from aigw:config:model_versions (key: model_code, value: version).
	modelCodes, err := r.client.HKeys(ctx, store.RedisKeyConfigModelVersions).Result()
	if err != nil {
		r.logger.Warn("fetch model versions from redis failed, fallback to cache",
			zap.Error(err),
			zap.String("key", store.RedisKeyConfigModelVersions))
		// On failure, return models already in local cache.
		r.mu.RLock()
		defer r.mu.RUnlock()
		result := make(map[string]bool, len(r.cache))
		for name := range r.cache {
			result[name] = true
		}
		for name := range r.smartCache {
			result[name] = true
		}
		return result
	}

	// Keep only models that have an endpoints key (pipeline for speed).
	result := make(map[string]bool, len(modelCodes))
	if len(modelCodes) == 0 {
		return result
	}

	pipe := r.client.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(modelCodes))
	for _, code := range modelCodes {
		cmds[code] = pipe.Exists(ctx, store.RedisKeyConfigEndpoints(code), RedisKeySmartRouting(code))
	}
	_, _ = pipe.Exec(ctx)

	for code, cmd := range cmds {
		if cmd.Val() > 0 {
			if _, _, err := r.GetSmartRouting(ctx, code); err == nil {
				result[code] = true
			}
		}
	}

	return result
}

func (r *RedisConfigSource) KnownModelsList() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var list []string
	for name := range r.lastVersions {
		list = append(list, name)
	}
	return list
}

func (r *RedisConfigSource) ClearCache() {
	r.mu.Lock()
	r.cache = make(map[string][]ResolvedEndpoint)
	r.lastVersions = make(map[string]int64)
	r.smartCache = make(map[string]*core.SmartRoutingConfig)
	r.smartErrors = make(map[string]error)
	r.mu.Unlock()
}
