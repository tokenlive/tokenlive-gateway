package config

import (
	"context"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// HTTPApiKeyItem is an API key record under the unified gateway contract.
type HTTPApiKeyItem struct {
	APIKey      string `json:"api_key"`
	KeyID       string `json:"key_id"`
	KeyHash     string `json:"key_hash"`
	UserID      string `json:"user_id"`
	Tenant      string `json:"tenant"`
	WorkspaceID string `json:"workspace_id"`
	UserTenant  string `json:"user_tenant"`
	Status      int    `json:"status"`
	Credits     int64  `json:"credits"`
	ExpiresAt   int64  `json:"expires_at"`
}

// HTTPPolicyItem is a policy record under the unified gateway contract.
type HTTPPolicyItem struct {
	Scope string         `json:"scope"` // "user:userID", "tenant:tenantCode", "model:modelCode", "global"
	Model string         `json:"model"` // model_code or "*"
	Value *policy.Policy `json:"value"`
}

// GatewayProvider is the unified data source for gateway config, policies, and API keys.
// Implementations may back onto Redis or HTTP.
type GatewayProvider interface {
	// GetConfig returns routing config for a model, or all models if modelCode is empty.
	GetConfig(ctx context.Context, modelCode string) (*GatewayConfig, error)

	// GetPolicies returns governance policies for model/user/tenant (or all).
	GetPolicies(ctx context.Context, modelCode, userID, tenantCode string) ([]HTTPPolicyItem, error)

	// GetApiKey returns details for an API key.
	GetApiKey(ctx context.Context, apiKey string) (*HTTPApiKeyItem, error)

	// GetUserModels returns models the user is allowed to access.
	GetUserModels(ctx context.Context, userID string) ([]string, error)

	// GetTenantModels returns models the tenant is allowed to access.
	GetTenantModels(ctx context.Context, tenantCode string) ([]string, error)

	// DeductCredits deducts credits for an API key and returns the new balance.
	DeductCredits(ctx context.Context, apiKey string, credits int64) (int64, error)
}
