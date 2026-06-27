package store

import "fmt"

const (
	RedisKeyConfigModelVersions = "aigw:config:model_versions"
)

// RedisKeyConfigEndpoints 返回 endpoints 配置的 key，使用 model_code
func RedisKeyConfigEndpoints(modelCode string) string {
	return "aigw:config:endpoints:" + modelCode
}

func RedisKeyApiKey(apiKey string) string {
	return "aigw:apikey:" + apiKey
}

func RedisKeyUserModels(userID string) string {
	return fmt.Sprintf("aigw:user:%s:models", userID)
}

func RedisKeyTenantEndpoints(tenantCode, modelCode string) string {
	return fmt.Sprintf("aigw:tenant:%s:model:%s:endpoints", tenantCode, modelCode)
}

// RedisKeyAlias 返回别名映射的 Redis key
func RedisKeyAlias(alias string) string {
	return "aigw:config:alias:" + alias
}

// RedisKeyModelAliases 返回模型别名集合的 Redis key（反向索引）
func RedisKeyModelAliases(modelCode string) string {
	return "aigw:config:model_aliases:" + modelCode
}
