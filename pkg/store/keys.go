package store

import "fmt"

const (
	RedisKeyConfigModelVersions = "aigw:config:model_versions"
)

// RedisKeyConfigEndpoints returns the endpoints config key for a model_code.
func RedisKeyConfigEndpoints(modelCode string) string {
	return "aigw:config:endpoints:" + modelCode
}

func RedisKeyApiKeyHash(keyHash string) string {
	return "aigw:apikey_hash:" + keyHash
}

func RedisKeyUserModels(userID string) string {
	return fmt.Sprintf("aigw:user:%s:models", userID)
}

func RedisKeyTenantEndpoints(tenantCode, modelCode string) string {
	return fmt.Sprintf("aigw:tenant:%s:model:%s:endpoints", tenantCode, modelCode)
}

// RedisKeyAlias returns the Redis key for an alias mapping.
func RedisKeyAlias(alias string) string {
	return "aigw:config:alias:" + alias
}

// RedisKeyModelAliases returns the Redis key for a model's alias set (reverse index).
func RedisKeyModelAliases(modelCode string) string {
	return "aigw:config:model_aliases:" + modelCode
}
