package service

import (
	"context"
	"errors"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"

	"go.uber.org/zap"
)

// ApiKeyInfo 表示 API Key 的元数据
type ApiKeyInfo struct {
	KeyID       string `json:"key_id"`
	KeyHash     string `json:"key_hash"`
	UserID      string `json:"user_id"`
	Tenant      string `json:"tenant"`       // 关联的租户唯一编码 (toB 场景)
	WorkspaceID string `json:"workspace_id"` // 关联的工作空间 ID (toC 场景)
	UserTenant  string `json:"user_tenant"`  // 用户所属租户 (toC 场景，用于模型过滤)
	Status      int    `json:"status"`       // 1-正常, 2-禁用
	Credits     int64  `json:"credits"`      // 可用余额, -1表示无限制
	ExpiresAt   int64  `json:"expires_at"`   // 过期时间戳 (秒)，0表示永不过期
}

type ApiKeyService struct {
	provider config.GatewayProvider
	logger   *log.Logger
	cache    *store.ExpirableCache[string, *ApiKeyInfo]
}

func NewApiKeyService(provider config.GatewayProvider, logger *log.Logger) *ApiKeyService {
	cache := store.NewExpirableCache[string, *ApiKeyInfo](
		10000, 30*time.Second,
		5000, 10*time.Second,
	)

	return &ApiKeyService{
		provider: provider,
		logger:   logger,
		cache:    cache,
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

	// 2. 动态回源：通过统一的 provider 获取 API Key 详情
	item, err := s.provider.GetApiKey(ctx, apiKey)
	if err != nil {
		s.logger.Logger.Error("failed to query api key from provider", zap.Error(err), zap.String("key", apiKey[:8]+"..."))
		// 写入负向缓存，防止穿透
		errMsg := "invalid API key"
		s.cache.AddInvalid(apiKey, errMsg)
		return nil, errors.New(errMsg)
	}

	info := &ApiKeyInfo{
		KeyID:       item.KeyID,
		KeyHash:     item.KeyHash,
		UserID:      item.UserID,
		Tenant:      item.Tenant,
		WorkspaceID: item.WorkspaceID,
		UserTenant:  item.UserTenant,
		Status:      item.Status,
		Credits:     item.Credits,
		ExpiresAt:   item.ExpiresAt,
	}

	// 3. 校验状态、过期时间
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

	// 4. 缓存结果到正向缓存中
	s.cache.AddValid(apiKey, info)

	return info, nil
}

// VerifyKey 专为鉴权中间件提供，校验成功返回 User ID、Tenant Code、Workspace ID 和 User Tenant，失败返回 error
func (s *ApiKeyService) VerifyKey(ctx context.Context, apiKey string) (string, string, string, string, string, string, error) {
	info, err := s.ValidateKey(ctx, apiKey)
	if err != nil {
		return "", "", "", "", "", "", err
	}
	return info.UserID, info.Tenant, info.WorkspaceID, info.UserTenant, info.KeyID, info.KeyHash, nil
}

// CheckCredits 检查 API Key 的余额是否充足（用于 InboundFilter 预检）
// 仅对个人用户（UserID != ""）进行余额检查，租户跳过
func (s *ApiKeyService) CheckCredits(ctx context.Context, apiKey string) error {
	info, err := s.ValidateKey(ctx, apiKey)
	if err != nil {
		return err
	}

	if info.UserID == "" {
		return nil
	}

	if info.Credits == -1 {
		return nil
	}

	if info.Credits <= 0 {
		return errors.New("credits exceeded")
	}

	return nil
}

// DeductCredits 扣减 API Key 的余额（用于 OutboundFilter 在请求完成后调用）
// 仅对个人用户（UserID != ""）进行余额扣减，租户跳过
// credits: 要扣减的微元金额
// 返回扣减后的新值和错误
func (s *ApiKeyService) DeductCredits(ctx context.Context, apiKey string, credits int64) (int64, error) {
	if credits <= 0 {
		return 0, nil
	}

	info, err := s.ValidateKey(ctx, apiKey)
	if err != nil {
		return 0, err
	}

	if info.UserID == "" {
		return 0, nil
	}

	if info.Credits == -1 {
		return -1, nil
	}

	// 调用统一的 provider 扣减余额
	newCredits, err := s.provider.DeductCredits(ctx, apiKey, credits)
	if err != nil {
		s.logger.Logger.Error("failed to deduct credits via provider",
			zap.Error(err),
			zap.String("api_key", apiKey[:8]+"..."),
			zap.Int64("credits", credits))
		return 0, err
	}

	// 扣减成功后，清除本地缓存，强制下次请求重新读取最新数据
	s.cache.Remove(apiKey)

	s.logger.Logger.Info("credits deducted",
		zap.String("user_id", info.UserID),
		zap.Int64("credits", credits),
		zap.Int64("new_credits", newCredits))

	return newCredits, nil
}

// PurgeCache 清空本地 LRU 缓存以立使新配置生效
func (s *ApiKeyService) PurgeCache() {
	s.cache = store.NewExpirableCache[string, *ApiKeyInfo](
		10000, 30*time.Second,
		5000, 10*time.Second,
	)
}
