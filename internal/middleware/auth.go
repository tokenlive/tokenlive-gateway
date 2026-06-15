package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ApiKeyValidator API Key 验证器接口（解耦）
type ApiKeyValidator interface {
	VerifyKey(ctx context.Context, apiKey string) (string, string, string, error)
}

// AuthConfig 认证配置
type AuthConfig struct {
	Validator ApiKeyValidator
	Logger    *zap.Logger
}

// NewAuthMiddleware 创建认证中间件
func NewAuthMiddleware(config *AuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 从请求头获取 API Key
		apiKey := extractAPIKey(c)

		if apiKey == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "Missing API key",
					"type":    "authentication_error",
				},
			})
			c.Abort()
			return
		}

		// 验证 API Key 并获取 User ID、Tenant Code 和 User Tenant
		userID, tenant, userTenant, err := config.Validator.VerifyKey(c.Request.Context(), apiKey)
		if err != nil {
			config.Logger.Warn("invalid API key", zap.String("key", maskAPIKey(apiKey)), zap.Error(err))
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": err.Error(),
					"type":    "authentication_error",
				},
			})
			c.Abort()
			return
		}

		// 区分 toB 和 toC，分别设置 Header
		if tenant != "" {
			c.Request.Header.Set("X-Tenant-ID", tenant)
			c.Set("tenant", tenant)
		} else {
			c.Request.Header.Set("X-User-ID", userID)
			c.Set("user_id", userID)
			// toC 场景：设置用户所属租户，供 ListModels/ValidateModel 过滤使用
			c.Request.Header.Set("X-User-Tenant", userTenant)
			c.Set("user_tenant", userTenant)
		}
		c.Request.Header.Set("X-API-Key", apiKey)
		c.Set("api_key", apiKey)
		c.Next()
	}
}

// extractAPIKey 从请求中提取 API Key
func extractAPIKey(c *gin.Context) string {
	// 1. 从 Authorization Header 获取 (Bearer token)
	auth := c.GetHeader("Authorization")
	if auth != "" {
		parts := strings.SplitN(auth, " ", 2)
		if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
			return parts[1]
		}
	}

	// 2. 从 api-key Header 获取
	if apiKey := c.GetHeader("api-key"); apiKey != "" {
		return apiKey
	}

	// 3. 从 x-api-key Header 获取
	if apiKey := c.GetHeader("x-api-key"); apiKey != "" {
		return apiKey
	}

	// 4. 从查询参数获取
	if apiKey := c.Query("api_key"); apiKey != "" {
		return apiKey
	}

	return ""
}

// maskAPIKey 掩码 API Key
func maskAPIKey(apiKey string) string {
	if len(apiKey) <= 8 {
		return "****"
	}
	return apiKey[:4] + "****" + apiKey[len(apiKey)-4:]
}
