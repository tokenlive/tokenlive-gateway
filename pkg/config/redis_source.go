package config

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

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
	}
}

// GetEndpoints 返回指定 model 的 resolved endpoints（从 Redis 缓存或远程读取）
// model 参数应为 model_code
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

	// 如果 endpoints 配置不存在，返回 redis.Nil 错误，以模拟缓存未命中行为
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
			delete(r.lastVersions, model)
		}
	}
}

func (r *RedisConfigSource) KnownModels() map[string]bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 从 Redis 的 aigw:config:model_versions Hash 获取所有模型列表
	// Admin 在同步模型配置时会维护这个 Hash（key: model_code, value: version）
	modelCodes, err := r.client.HKeys(ctx, store.RedisKeyConfigModelVersions).Result()
	if err != nil {
		r.logger.Warn("fetch model versions from redis failed, fallback to cache",
			zap.Error(err),
			zap.String("key", store.RedisKeyConfigModelVersions))
		// 失败时回退到已缓存的模型
		r.mu.RLock()
		defer r.mu.RUnlock()
		result := make(map[string]bool, len(r.cache))
		for name := range r.cache {
			result[name] = true
		}
		return result
	}

	// 批量检查这些模型是否有实际的 endpoints 配置
	// 使用 pipeline 提高性能
	result := make(map[string]bool, len(modelCodes))
	if len(modelCodes) == 0 {
		return result
	}

	pipe := r.client.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(modelCodes))
	for _, code := range modelCodes {
		cmds[code] = pipe.Exists(ctx, store.RedisKeyConfigEndpoints(code))
	}
	_, _ = pipe.Exec(ctx)

	// 只返回有 endpoints 配置的模型
	for code, cmd := range cmds {
		if cmd.Val() > 0 {
			result[code] = true
		}
	}

	return result
}

func (r *RedisConfigSource) KnownModelsList() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var list []string
	for name := range r.cache {
		list = append(list, name)
	}
	return list
}

func (r *RedisConfigSource) ClearCache() {
	r.mu.Lock()
	r.cache = make(map[string][]ResolvedEndpoint)
	r.lastVersions = make(map[string]int64)
	r.mu.Unlock()
}
