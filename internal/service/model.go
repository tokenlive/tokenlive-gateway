package service

import (
	"context"
	"errors"
	"sync"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type ModelService struct {
	mu             sync.RWMutex
	rdb            *redis.Client
	logger         *log.Logger
	fallbackModels map[string]bool
}

func NewModelService(rdb *redis.Client, logger *log.Logger, conf *viper.Viper) *ModelService {
	fallbackModels := make(map[string]bool)

	if conf.IsSet("models") {
		cfg, err := config.Load(conf)
		if err == nil {
			fallbackModels = config.KnownModels(cfg)
		} else {
			logger.Logger.Error("failed to load gateway config for model service", zap.Error(err))
		}
	}

	return &ModelService{
		rdb:            rdb,
		logger:         logger,
		fallbackModels: fallbackModels,
	}
}

func (s *ModelService) UpdateFallbackModels(models map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fallbackModels = models
}

// ValidateModel 校验指定用户的 model 是否存在且合法
// model 参数为客户端请求的模型标识（应为 model_code）
func (s *ModelService) ValidateModel(ctx context.Context, model string, tenant string, userID string) (bool, error) {
	// 1. ToB 租户模式校验
	if tenant != "" {
		s.mu.RLock()
		isFallback := s.fallbackModels[model]
		s.mu.RUnlock()

		if s.rdb == nil {
			s.logger.Logger.Debug("redis client is nil in ToB, fallback to local config validation", zap.String("model", model))
			return isFallback, nil
		}

		redisKey := "aigw:tenant:" + tenant + ":models"
		pipe := s.rdb.Pipeline()
		existsCmd := pipe.Exists(ctx, redisKey)
		isMemberCmd := pipe.SIsMember(ctx, redisKey, model)
		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			s.logger.Logger.Error("redis pipeline command error in ToB, fallback to local config validation",
				zap.Error(err),
				zap.String("key", redisKey),
				zap.String("model", model),
			)
			return isFallback, nil
		}

		exists := existsCmd.Val()
		if exists == 0 {
			// Key 不存在，说明 Redis 中未配置该租户模型授权集合。放通所有可用模型
			s.logger.Logger.Debug("tenant model key does not exist in redis, fallback to check all available models",
				zap.String("key", redisKey),
				zap.String("model", model),
			)
			if isFallback {
				return true, nil
			}
			configKey := store.RedisKeyConfigEndpoints(model)
			cfgExists, err := s.rdb.Exists(ctx, configKey).Result()
			if err == nil && cfgExists > 0 {
				return true, nil
			}
			return false, nil
		}

		isMember := isMemberCmd.Val()
		if !isMember {
			s.logger.Logger.Warn("model not allowed for tenant", zap.String("tenant", tenant), zap.String("model", model))
		}
		return isMember, nil
	}

	// 2. ToC 个人模式校验：放通系统中所有已配置/可用的模型
	s.mu.RLock()
	isFallback := s.fallbackModels[model]
	s.mu.RUnlock()

	if isFallback {
		return true, nil
	}
	if s.rdb != nil {
		configKey := store.RedisKeyConfigEndpoints(model)
		exists, err := s.rdb.Exists(ctx, configKey).Result()
		if err == nil && exists > 0 {
			return true, nil
		}
	}

	s.logger.Logger.Warn("unknown model in ToC mode", zap.String("userID", userID), zap.String("model", model))
	return false, nil
}

// ListUserModels 返回该用户在 Redis 中授权的模型 ID 列表。
// 严格语义：Key 不存在或 Redis 错误，统一视为 0 个授权模型，避免越权暴露。
func (s *ModelService) ListUserModels(ctx context.Context, userID string) ([]string, error) {
	if userID == "" {
		return nil, errors.New("userID is empty")
	}
	if s.rdb == nil {
		s.logger.Logger.Warn("redis client unavailable, return empty user models",
			zap.String("userID", userID))
		return []string{}, nil
	}
	redisKey := store.RedisKeyUserModels(userID)
	members, err := s.rdb.SMembers(ctx, redisKey).Result()
	if err != nil {
		s.logger.Logger.Error("redis SMEMBERS error, return empty list",
			zap.Error(err),
			zap.String("key", redisKey),
			zap.String("userID", userID))
		return []string{}, nil
	}
	return members, nil
}

// ListTenantModels 返回该租户在 Redis 中授权的模型 ID 列表。
// 如果 Key 不存在，说明该租户未受模型选择限制，此时返回包含通配符 "*" 的列表。
func (s *ModelService) ListTenantModels(ctx context.Context, tenant string) ([]string, error) {
	if tenant == "" {
		return nil, errors.New("tenant is empty")
	}
	if s.rdb == nil {
		s.logger.Logger.Warn("redis client unavailable, return empty tenant models",
			zap.String("tenant", tenant))
		return []string{}, nil
	}
	redisKey := "aigw:tenant:" + tenant + ":models"

	// 检查租户模型限制 Key 是否存在
	exists, err := s.rdb.Exists(ctx, redisKey).Result()
	if err != nil {
		s.logger.Logger.Error("redis EXISTS error",
			zap.Error(err),
			zap.String("key", redisKey),
			zap.String("tenant", tenant))
		return []string{}, nil
	}
	if exists == 0 {
		s.logger.Logger.Debug("tenant model key does not exist in redis, return wildcard",
			zap.String("key", redisKey),
			zap.String("tenant", tenant))
		return []string{"*"}, nil
	}

	members, err := s.rdb.SMembers(ctx, redisKey).Result()
	if err != nil {
		s.logger.Logger.Error("redis SMEMBERS error, return empty list",
			zap.Error(err),
			zap.String("key", redisKey),
			zap.String("tenant", tenant))
		return []string{}, nil
	}
	return members, nil
}
