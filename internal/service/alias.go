package service

import (
	"context"
	"errors"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// AliasService 负责将客户端请求中的 model alias 解析为真实的 model_code
type AliasService struct {
	rdb    *redis.Client
	logger *log.Logger
	cache  *store.ExpirableCache[string, string] // alias → model_code
}

// NewAliasService 创建 AliasService 实例
func NewAliasService(rdb *redis.Client, logger *log.Logger) *AliasService {
	cache := store.NewExpirableCache[string, string](
		5000, 30*time.Second, // valid cache: 5k 条, 30s TTL
		2000, 10*time.Second, // invalid cache: 2k 条, 10s TTL
	)
	return &AliasService{
		rdb:    rdb,
		logger: logger,
		cache:  cache,
	}
}

// Resolve 尝试将 model 解析为真实的 model_code。
// 如果 model 不是别名，返回原始 model（静默降级）。
// 如果 Redis 不可用，返回错误（fail-close）。
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
	if s.rdb == nil {
		return model, nil
	}

	key := store.RedisKeyAlias(model)
	modelCode, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// Redis 中不存在此别名，写入负向缓存
			s.cache.AddInvalid(model, "not an alias")
			return model, nil
		}
		// Redis 不可用，fail-close
		s.logger.Logger.Error("failed to query alias from redis", zap.Error(err), zap.String("key", key))
		return "", err
	}

	// 3. 命中，写入正向缓存
	s.cache.AddValid(model, modelCode)
	return modelCode, nil
}
