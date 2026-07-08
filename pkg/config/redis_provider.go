package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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

	pipe := p.rdb.Pipeline()

	var (
		userHMGetCmd     *redis.SliceCmd
		userHGetAllCmd   *redis.MapStringStringCmd
		tenantHMGetCmd   *redis.SliceCmd
		tenantHGetAllCmd *redis.MapStringStringCmd
		modelHMGetCmd    *redis.SliceCmd
	)

	// 1. 查询用户策略 (Level 5 & Level 2)
	if userID != "" {
		userHashKey := "aigw:policies:user:" + userID
		if modelCode != "" && modelCode != "*" {
			// 一次性拉取用户针对当前模型的策略、计费配置，以及默认策略、默认计费配置
			userHMGetCmd = pipe.HMGet(ctx, userHashKey, modelCode, modelCode+":billing", "*", "*:billing")
		} else {
			userHGetAllCmd = pipe.HGetAll(ctx, userHashKey)
		}
	}

	// 2. 查询租户策略 (Level 4 & Level 1)
	if tenantCode != "" {
		tenantHashKey := "aigw:policies:tenant:" + tenantCode
		if modelCode != "" && modelCode != "*" {
			tenantHMGetCmd = pipe.HMGet(ctx, tenantHashKey, modelCode, modelCode+":billing", "*", "*:billing")
		} else {
			tenantHGetAllCmd = pipe.HGetAll(ctx, tenantHashKey)
		}
	}

	// 3. 查询模型策略 (Level 3)
	if modelCode != "" {
		modelHashKey := "aigw:policies:model:" + modelCode
		modelHMGetCmd = pipe.HMGet(ctx, modelHashKey, "*", "*:billing")
	}

	// 执行 Pipeline，忽略 Exec 返回的单个 error，我们会检查具体 command 的 error
	_, _ = pipe.Exec(ctx)

	// 5. 解析并反序列化用户策略
	if userHMGetCmd != nil {
		if vals, err := userHMGetCmd.Result(); err == nil && len(vals) == 4 {
			// vals: [modelCode, modelCode+":billing", "*", "*:billing"]
			var userPolicy *policy.Policy
			var userDefaultPolicy *policy.Policy

			// 5.1. 处理特定模型策略
			if vals[0] != nil {
				if valStr, ok := vals[0].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &userPolicy)
				}
			}
			var modelBilling *policy.BillingPolicy
			if vals[1] != nil {
				if valStr, ok := vals[1].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &modelBilling)
				}
			}
			if modelBilling != nil {
				if userPolicy == nil {
					userPolicy = &policy.Policy{}
				}
				userPolicy.Billing = modelBilling
			}
			if userPolicy != nil {
				items = append(items, HTTPPolicyItem{
					Scope: "user:" + userID,
					Model: modelCode,
					Value: userPolicy,
				})
			}

			// 5.2. 处理通配默认策略
			if vals[2] != nil {
				if valStr, ok := vals[2].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &userDefaultPolicy)
				}
			}
			var defaultBilling *policy.BillingPolicy
			if vals[3] != nil {
				if valStr, ok := vals[3].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &defaultBilling)
				}
			}
			if defaultBilling != nil {
				if userDefaultPolicy == nil {
					userDefaultPolicy = &policy.Policy{}
				}
				userDefaultPolicy.Billing = defaultBilling
			}
			if userDefaultPolicy != nil {
				items = append(items, HTTPPolicyItem{
					Scope: "user:" + userID,
					Model: "*",
					Value: userDefaultPolicy,
				})
			}
		}
	} else if userHGetAllCmd != nil {
		if fields, err := userHGetAllCmd.Result(); err == nil && len(fields) > 0 {
			policies := make(map[string]*policy.Policy)
			billings := make(map[string]*policy.BillingPolicy)

			for m, val := range fields {
				if val == "" {
					continue
				}
				if strings.HasSuffix(m, ":billing") {
					mName := strings.TrimSuffix(m, ":billing")
					var temp policy.BillingPolicy
					if json.Unmarshal([]byte(val), &temp) == nil {
						billings[mName] = &temp
					}
				} else {
					var temp policy.Policy
					if json.Unmarshal([]byte(val), &temp) == nil {
						policies[m] = &temp
					}
				}
			}

			// 合并计费到对应策略中
			for m, b := range billings {
				if p, ok := policies[m]; ok {
					p.Billing = b
				} else {
					policies[m] = &policy.Policy{Billing: b}
				}
			}

			for m, p := range policies {
				items = append(items, HTTPPolicyItem{
					Scope: "user:" + userID,
					Model: m,
					Value: p,
				})
			}
		}
	}

	// 6. 解析并反序列化租户策略
	if tenantHMGetCmd != nil {
		if vals, err := tenantHMGetCmd.Result(); err == nil && len(vals) == 4 {
			var tenantPolicy *policy.Policy
			var tenantDefaultPolicy *policy.Policy

			// 6.1. 处理特定模型策略
			if vals[0] != nil {
				if valStr, ok := vals[0].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &tenantPolicy)
				}
			}
			var modelBilling *policy.BillingPolicy
			if vals[1] != nil {
				if valStr, ok := vals[1].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &modelBilling)
				}
			}
			if modelBilling != nil {
				if tenantPolicy == nil {
					tenantPolicy = &policy.Policy{}
				}
				tenantPolicy.Billing = modelBilling
			}
			if tenantPolicy != nil {
				items = append(items, HTTPPolicyItem{
					Scope: "tenant:" + tenantCode,
					Model: modelCode,
					Value: tenantPolicy,
				})
			}

			// 6.2. 处理通配默认策略
			if vals[2] != nil {
				if valStr, ok := vals[2].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &tenantDefaultPolicy)
				}
			}
			var defaultBilling *policy.BillingPolicy
			if vals[3] != nil {
				if valStr, ok := vals[3].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &defaultBilling)
				}
			}
			if defaultBilling != nil {
				if tenantDefaultPolicy == nil {
					tenantDefaultPolicy = &policy.Policy{}
				}
				tenantDefaultPolicy.Billing = defaultBilling
			}
			if tenantDefaultPolicy != nil {
				items = append(items, HTTPPolicyItem{
					Scope: "tenant:" + tenantCode,
					Model: "*",
					Value: tenantDefaultPolicy,
				})
			}
		}
	} else if tenantHGetAllCmd != nil {
		if fields, err := tenantHGetAllCmd.Result(); err == nil && len(fields) > 0 {
			policies := make(map[string]*policy.Policy)
			billings := make(map[string]*policy.BillingPolicy)

			for m, val := range fields {
				if val == "" {
					continue
				}
				if strings.HasSuffix(m, ":billing") {
					mName := strings.TrimSuffix(m, ":billing")
					var temp policy.BillingPolicy
					if json.Unmarshal([]byte(val), &temp) == nil {
						billings[mName] = &temp
					}
				} else {
					var temp policy.Policy
					if json.Unmarshal([]byte(val), &temp) == nil {
						policies[m] = &temp
					}
				}
			}

			for m, b := range billings {
				if p, ok := policies[m]; ok {
					p.Billing = b
				} else {
					policies[m] = &policy.Policy{Billing: b}
				}
			}

			for m, p := range policies {
				items = append(items, HTTPPolicyItem{
					Scope: "tenant:" + tenantCode,
					Model: m,
					Value: p,
				})
			}
		}
	}

	// 7. 解析模型策略
	if modelHMGetCmd != nil {
		if vals, err := modelHMGetCmd.Result(); err == nil && len(vals) == 2 {
			var modelPolicy *policy.Policy
			if vals[0] != nil {
				if valStr, ok := vals[0].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &modelPolicy)
				}
			}
			var modelBilling *policy.BillingPolicy
			if vals[1] != nil {
				if valStr, ok := vals[1].(string); ok && valStr != "" {
					_ = json.Unmarshal([]byte(valStr), &modelBilling)
				}
			}
			if modelBilling != nil {
				if modelPolicy == nil {
					modelPolicy = &policy.Policy{}
				}
				modelPolicy.Billing = modelBilling
			}
			if modelPolicy != nil {
				items = append(items, HTTPPolicyItem{
					Scope: "model:" + modelCode,
					Model: "*",
					Value: modelPolicy,
				})
			}
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
	if item, ok := parseRedisApiKeyItem(apiKey, keyHash, fields); ok {
		return item, redisKey, nil
	}

	return nil, "", fmt.Errorf("api key not found in redis")
}

func parseRedisApiKeyItem(apiKey, keyHash string, fields map[string]string) (*HTTPApiKeyItem, bool) {
	if len(fields) == 0 || (fields["user_id"] == "" && fields["tenant"] == "" && fields["workspace_id"] == "") {
		return nil, false
	}

	userID := fields["user_id"]
	tenant := fields["tenant"]
	workspaceID := fields["workspace_id"]
	userTenant := fields["user_tenant"]
	status, _ := strconv.Atoi(fields["status"])
	credits, _ := strconv.ParseInt(fields["credits"], 10, 64)
	expiresAt, _ := strconv.ParseInt(fields["expires_at"], 10, 64)

	return &HTTPApiKeyItem{
		APIKey:      apiKey,
		KeyID:       fields["key_id"],
		KeyHash:     keyHash,
		UserID:      userID,
		Tenant:      tenant,
		WorkspaceID: workspaceID,
		UserTenant:  userTenant,
		Status:      status,
		Credits:     credits,
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

func (p *RedisGatewayProvider) DeductCredits(ctx context.Context, apiKey string, credits int64) (int64, error) {
	if p.rdb == nil {
		return 0, fmt.Errorf("redis client is not initialized")
	}
	_, redisKey, err := p.getApiKeyWithRedisKey(ctx, apiKey)
	if err != nil {
		return 0, err
	}
	return p.rdb.HIncrBy(ctx, redisKey, "credits", -credits).Result()
}
