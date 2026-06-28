package config

import (
	"context"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// HTTPApiKeyItem 代表统一契约下的 API Key 数据项
type HTTPApiKeyItem struct {
	APIKey      string `json:"api_key"`
	UserID      string `json:"user_id"`
	Tenant      string `json:"tenant"`
	WorkspaceID string `json:"workspace_id"`
	UserTenant  string `json:"user_tenant"`
	Status      int    `json:"status"`
	Quota       int64  `json:"quota"`
	ExpiresAt   int64  `json:"expires_at"`
}

// HTTPPolicyItem 代表统一契约下的策略数据项
type HTTPPolicyItem struct {
	Scope string         `json:"scope"` // "user:userID", "tenant:tenantCode", "model:modelCode", "global"
	Model string         `json:"model"` // model_code 或 "*"
	Value *policy.Policy `json:"value"`
}

// GatewayProvider 定义了获取网关配置、策略和 API Key 数据的统一数据源接口。
// 屏蔽了底层数据获取源（Redis 或 HTTP）的差异。
type GatewayProvider interface {
	// GetConfig 获取特定模型或全量的路由配置。如果 modelCode 为空则获取全量。
	GetConfig(ctx context.Context, modelCode string) (*GatewayConfig, error)

	// GetPolicies 获取特定模型、用户、租户或全量的治理策略。
	GetPolicies(ctx context.Context, modelCode, userID, tenantCode string) ([]HTTPPolicyItem, error)

	// GetApiKey 获取特定 API Key 的详情。
	GetApiKey(ctx context.Context, apiKey string) (*HTTPApiKeyItem, error)

	// GetUserModels 获取用户被授权访问的模型列表
	GetUserModels(ctx context.Context, userID string) ([]string, error)

	// GetTenantModels 获取租户被授权访问的模型列表
	GetTenantModels(ctx context.Context, tenantCode string) ([]string, error)

	// DeductQuota 扣减指定 API Key 的配额，返回扣减后的新配额值。
	DeductQuota(ctx context.Context, apiKey string, tokens int64) (int64, error)
}
