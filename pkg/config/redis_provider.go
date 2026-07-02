package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

type RedisGatewayProvider struct {
	rdb          *redis.Client
	apiKeyPepper string
}

func NewRedisGatewayProvider(rdb *redis.Client) *RedisGatewayProvider {
	return NewRedisGatewayProviderWithAPIKeyPepper(rdb, "")
}

func NewRedisGatewayProviderWithAPIKeyPepper(rdb *redis.Client, apiKeyPepper string) *RedisGatewayProvider {
	return &RedisGatewayProvider{
		rdb:          rdb,
		apiKeyPepper: apiKeyPepper,
	}
}

func (p *RedisGatewayProvider) GetConfig(ctx context.Context, modelCode string) (*GatewayConfig, error) {
	return nil, fmt.Errorf("GetConfig not supported in Redis mode")
}

func (p *RedisGatewayProvider) GetPolicies(ctx context.Context, modelCode, userID, tenantCode string) ([]HTTPPolicyItem, error) {
	if p.rdb == nil {
		return nil, nil
	}
	var items []HTTPPolicyItem

	// 1. 查询用户策略 (Level 5 & Level 2)
	if userID != "" {
		userHashKey := "aigw:policies:user:" + userID
		fields, err := p.rdb.HGetAll(ctx, userHashKey).Result()
		if err == nil && len(fields) > 0 {
			for m, val := range fields {
				var temp policy.Policy
				if json.Unmarshal([]byte(val), &temp) == nil {
					items = append(items, HTTPPolicyItem{
						Scope: "user:" + userID,
						Model: m,
						Value: &temp,
					})
				}
			}
		}
	}

	// 2. 查询租户策略 (Level 4 & Level 1)
	if tenantCode != "" {
		tenantHashKey := "aigw:policies:tenant:" + tenantCode
		fields, err := p.rdb.HGetAll(ctx, tenantHashKey).Result()
		if err == nil && len(fields) > 0 {
			for m, val := range fields {
				var temp policy.Policy
				if json.Unmarshal([]byte(val), &temp) == nil {
					items = append(items, HTTPPolicyItem{
						Scope: "tenant:" + tenantCode,
						Model: m,
						Value: &temp,
					})
				}
			}
		}
	}

	// 3. 查询模型策略 (Level 3)
	if modelCode != "" {
		modelHashKey := "aigw:policies:model:" + modelCode
		val, err := p.rdb.HGet(ctx, modelHashKey, "*").Result()
		if err == nil && val != "" {
			var temp policy.Policy
			if json.Unmarshal([]byte(val), &temp) == nil {
				items = append(items, HTTPPolicyItem{
					Scope: "model:" + modelCode,
					Model: "*",
					Value: &temp,
				})
			}
		}
	}

	// 4. 查询全局策略 (Level 0)
	val, err := p.rdb.HGet(ctx, "aigw:policies:global", "*").Result()
	if err == nil && val != "" {
		var temp policy.Policy
		if json.Unmarshal([]byte(val), &temp) == nil {
			items = append(items, HTTPPolicyItem{
				Scope: "global",
				Model: "*",
				Value: &temp,
			})
		}
	}

	return items, nil
}

func (p *RedisGatewayProvider) GetApiKey(ctx context.Context, apiKey string) (*HTTPApiKeyItem, error) {
	item, _, err := p.getApiKeyWithRedisKey(ctx, apiKey)
	return item, err
}

func (p *RedisGatewayProvider) getApiKeyWithRedisKey(ctx context.Context, apiKey string) (*HTTPApiKeyItem, string, error) {
	if p.rdb == nil {
		return nil, "", fmt.Errorf("redis client is not initialized")
	}
	if p.apiKeyPepper == "" {
		return nil, "", fmt.Errorf("llm.api_key_pepper is required for redis api key lookup")
	}

	keyHash := HashAPIKey(apiKey, p.apiKeyPepper)
	redisKey := store.RedisKeyApiKeyHash(keyHash)
	fields, err := p.rdb.HGetAll(ctx, redisKey).Result()
	if err != nil {
		return nil, "", err
	}
	if item, ok := parseRedisApiKeyItem(apiKey, fields); ok {
		return item, redisKey, nil
	}

	return nil, "", fmt.Errorf("api key not found in redis")
}

func parseRedisApiKeyItem(apiKey string, fields map[string]string) (*HTTPApiKeyItem, bool) {
	if len(fields) == 0 || (fields["user_id"] == "" && fields["tenant"] == "" && fields["workspace_id"] == "") {
		return nil, false
	}

	userID := fields["user_id"]
	tenant := fields["tenant"]
	workspaceID := fields["workspace_id"]
	userTenant := fields["user_tenant"]
	status, _ := strconv.Atoi(fields["status"])
	quota, _ := strconv.ParseInt(fields["quota"], 10, 64)
	expiresAt, _ := strconv.ParseInt(fields["expires_at"], 10, 64)

	return &HTTPApiKeyItem{
		APIKey:      apiKey,
		UserID:      userID,
		Tenant:      tenant,
		WorkspaceID: workspaceID,
		UserTenant:  userTenant,
		Status:      status,
		Quota:       quota,
		ExpiresAt:   expiresAt,
	}, true
}

func (p *RedisGatewayProvider) GetUserModels(ctx context.Context, userID string) ([]string, error) {
	if p.rdb == nil {
		return nil, nil
	}
	userKey := "aigw:user:" + userID + ":models"
	return p.rdb.SMembers(ctx, userKey).Result()
}

func (p *RedisGatewayProvider) GetTenantModels(ctx context.Context, tenantCode string) ([]string, error) {
	if p.rdb == nil {
		return nil, nil
	}
	tenantKey := "aigw:tenant:" + tenantCode + ":models"
	return p.rdb.SMembers(ctx, tenantKey).Result()
}

func (p *RedisGatewayProvider) DeductQuota(ctx context.Context, apiKey string, tokens int64) (int64, error) {
	if p.rdb == nil {
		return 0, fmt.Errorf("redis client is not initialized")
	}
	_, redisKey, err := p.getApiKeyWithRedisKey(ctx, apiKey)
	if err != nil {
		return 0, err
	}
	return p.rdb.HIncrBy(ctx, redisKey, "quota", -tokens).Result()
}
