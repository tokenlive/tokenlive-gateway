package service

import (
	"context"
	"errors"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// AliasService 负责将客户端请求中的 model alias 解析为真实的 model_code
type AliasService struct {
	rdb       *redis.Client
	configMgr *config.ConfigManager
	logger    *log.Logger
	cache     *store.ExpirableCache[string, string] // alias → model_code
}

// NewAliasService 创建 AliasService 实例
func NewAliasService(rdb *redis.Client, logger *log.Logger, configMgr *config.ConfigManager) *AliasService {
	cache := store.NewExpirableCache[string, string](
		5000, 30*time.Second, // valid cache: 5k 条, 30s TTL
		2000, 10*time.Second, // invalid cache: 2k 条, 10s TTL
	)
	return &AliasService{
		rdb:       rdb,
		configMgr: configMgr,
		logger:    logger,
		cache:     cache,
	}
}

// PurgeCache 清空所有本地别名缓存
func (s *AliasService) PurgeCache() {
	s.cache.Purge()
}

// Resolve 尝试将 model 解析为真实的 model_code。
// 优先查本地 LRU 缓存，次选 Redis，若 Redis 为空或未配置则从 ConfigManager 查询（支持大小写容错）。
// 如果 model 不是别名，返回原始 model（静默降级）。
func (s *AliasService) Resolve(ctx context.Context, model string) (string, error) {
	if model == "" {
		return model, nil
	}

	// 1. 查本地缓存（正向 + 负向）
	if modelCode, errMsg, ok := s.cache.Get(model); ok {
		if errMsg != "" {
			// 负向缓存命中：这个 model 不是已知别名，原样返回
			return model, nil
		}
		return modelCode, nil
	}

	// 2. 查 Redis
	if s.rdb != nil {
		key := store.RedisKeyAlias(model)
		modelCode, err := s.rdb.Get(ctx, key).Result()
		if err == nil && modelCode != "" {
			s.cache.AddValid(model, modelCode)
			return modelCode, nil
		}
		if err != nil && !errors.Is(err, redis.Nil) {
			s.logger.Logger.Error("failed to query alias from redis", zap.Error(err), zap.String("key", key))
			return "", err
		}
	}

	// 3. 查本地 ConfigManager（单机版/嵌入式模式，或 Redis 没查到的场景）
	if s.configMgr != nil {
		if target, ok := s.configMgr.GetAlias(model); ok {
			s.cache.AddValid(model, target)
			return target, nil
		}
		// 4. 检查是否为已知模型的 Case-Insensitive 归一化（如 GLM-5.3 -> glm-5.3）
		if normalized, ok := s.configMgr.NormalizeModelCode(model); ok && normalized != model {
			s.cache.AddValid(model, normalized)
			return normalized, nil
		}
	}

	// 5. 不是别名，写入负向缓存
	s.cache.AddInvalid(model, "not an alias")
	return model, nil
}

// GetAliases 返回指定 modelCode 的所有别名列表。
func (s *AliasService) GetAliases(ctx context.Context, modelCode string) ([]string, error) {
	if modelCode == "" {
		return nil, nil
	}

	if s.rdb != nil {
		key := store.RedisKeyModelAliases(modelCode)
		aliases, err := s.rdb.SMembers(ctx, key).Result()
		if err == nil {
			return aliases, nil
		}
		if !errors.Is(err, redis.Nil) {
			s.logger.Logger.Error("failed to query model aliases from redis", zap.Error(err), zap.String("key", key))
			return nil, err
		}
	}

	// Fallback to ConfigManager
	if s.configMgr != nil {
		return s.configMgr.GetAliasesForModel(modelCode), nil
	}

	return nil, nil
}
