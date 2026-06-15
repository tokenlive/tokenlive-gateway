package service

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ApiKeyInfo 表示 API Key 的元数据
type ApiKeyInfo struct {
	UserID     string `json:"user_id"`
	Tenant     string `json:"tenant"`      // 关联的租户唯一编码 (toB 场景)
	UserTenant string `json:"user_tenant"` // 用户所属租户 (toC 场景，用于模型过滤)
	Status     int    `json:"status"`      // 1-正常, 2-禁用
	Quota      int64  `json:"quota"`       // 剩余配额, -1表示无限制
	ExpiresAt  int64  `json:"expires_at"`  // 过期时间戳 (秒)，0表示永不过期
}

type ApiKeyService struct {
	rdb    *redis.Client
	logger *log.Logger
	cache  *store.ExpirableCache[string, *ApiKeyInfo]
}

func NewApiKeyService(rdb *redis.Client, logger *log.Logger) *ApiKeyService {
	// 按照 loadbalance.go 的规范，使用 hashicorp 的 expirable LRU 本地缓存
	cache := store.NewExpirableCache[string, *ApiKeyInfo](
		10000, 30*time.Second,
		5000, 10*time.Second,
	)

	return &ApiKeyService{
		rdb:    rdb,
		logger: logger,
		cache:  cache,
	}
}

// ValidateKey 校验 API Key
func (s *ApiKeyService) ValidateKey(ctx context.Context, apiKey string) (*ApiKeyInfo, error) {
	// 1. 优先查本地缓存（包含正向 and 负向缓存），防止非法 Key 穿透
	if info, errMsg, ok := s.cache.Get(apiKey); ok {
		if errMsg != "" {
			return nil, errors.New(errMsg)
		}
		return info, nil
	}

	// 3. 查 Redis 缓存
	redisKey := store.RedisKeyApiKey(apiKey)
	fields, err := s.rdb.HGetAll(ctx, redisKey).Result()
	if err != nil {
		s.logger.Logger.Error("failed to query api key from redis", zap.Error(err), zap.String("key", redisKey))
		return nil, err
	}

	// 如果 fields 为空（或者 user_id 与 tenant 均为空），说明 Key 不存在
	if len(fields) == 0 || (fields["user_id"] == "" && fields["tenant"] == "") {
		// 写入负向缓存，防止穿透
		errMsg := "invalid API key"
		s.cache.AddInvalid(apiKey, errMsg)
		return nil, errors.New(errMsg)
	}

	// 4. 解析字段
	userID := fields["user_id"]
	tenant := fields["tenant"]
	userTenant := fields["user_tenant"]
	status, _ := strconv.Atoi(fields["status"])
	quota, _ := strconv.ParseInt(fields["quota"], 10, 64)
	expiresAt, _ := strconv.ParseInt(fields["expires_at"], 10, 64)

	// 互斥身份安全校验
	if userID != "" && tenant != "" {
		errMsg := "misconfigured API key containing both user and tenant"
		s.cache.AddInvalid(apiKey, errMsg)
		return nil, errors.New(errMsg)
	}

	info := &ApiKeyInfo{
		UserID:     userID,
		Tenant:     tenant,
		UserTenant: userTenant,
		Status:     status,
		Quota:      quota,
		ExpiresAt:  expiresAt,
	}

	// 5. 校验状态、过期时间
	if info.Status != 1 {
		errMsg := "API key has been disabled"
		s.cache.AddInvalid(apiKey, errMsg)
		return nil, errors.New(errMsg)
	}

	if info.ExpiresAt > 0 && info.ExpiresAt < time.Now().Unix() {
		errMsg := "API key has expired"
		s.cache.AddInvalid(apiKey, errMsg)
		return nil, errors.New(errMsg)
	}

	// 6. 缓存结果到正向缓存中
	s.cache.AddValid(apiKey, info)

	return info, nil
}

// VerifyKey 专为鉴权中间件提供，校验成功返回 User ID、Tenant Code 和 User Tenant，失败返回 error
func (s *ApiKeyService) VerifyKey(ctx context.Context, apiKey string) (string, string, string, error) {
	info, err := s.ValidateKey(ctx, apiKey)
	if err != nil {
		return "", "", "", err
	}
	return info.UserID, info.Tenant, info.UserTenant, nil
}

// CheckQuota 检查 API Key 的配额是否充足（用于 InboundFilter 预检）
// 仅对个人用户（UserID != ""）进行配额检查，租户跳过
func (s *ApiKeyService) CheckQuota(ctx context.Context, apiKey string) error {
	info, err := s.ValidateKey(ctx, apiKey)
	if err != nil {
		return err
	}

	// 只对个人用户进行配额检查，租户跳过
	if info.UserID == "" {
		return nil
	}

	// -1 表示无限制配额
	if info.Quota == -1 {
		return nil
	}

	// 检查配额是否耗尽
	if info.Quota <= 0 {
		return errors.New("quota exceeded")
	}

	return nil
}

// DeductQuota 扣减 API Key 的配额（用于 OutboundFilter 在请求完成后调用）
// 仅对个人用户（UserID != ""）进行配额扣减，租户跳过
// tokens: 要扣减的 token 数量（InputTokens + OutputTokens）
// 返回扣减后的新配额值和错误
func (s *ApiKeyService) DeductQuota(ctx context.Context, apiKey string, tokens int64) (int64, error) {
	if tokens <= 0 {
		return 0, nil
	}

	// 先验证 API Key 并获取身份信息
	info, err := s.ValidateKey(ctx, apiKey)
	if err != nil {
		return 0, err
	}

	// 只对个人用户进行配额扣减，租户跳过
	if info.UserID == "" {
		return 0, nil
	}

	// -1 表示无限制配额，跳过扣减
	if info.Quota == -1 {
		return -1, nil
	}

	// 使用 Redis HINCRBY 原子性扣减配额
	redisKey := store.RedisKeyApiKey(apiKey)
	newQuota, err := s.rdb.HIncrBy(ctx, redisKey, "quota", -tokens).Result()
	if err != nil {
		s.logger.Logger.Error("failed to deduct quota",
			zap.Error(err),
			zap.String("api_key", apiKey[:8]+"..."),
			zap.Int64("tokens", tokens))
		return 0, err
	}

	// 扣减成功后，清除本地缓存，强制下次请求重新从 Redis 读取
	s.cache.Remove(apiKey)

	s.logger.Logger.Info("quota deducted",
		zap.String("user_id", info.UserID),
		zap.Int64("tokens", tokens),
		zap.Int64("new_quota", newQuota))

	return newQuota, nil
}
