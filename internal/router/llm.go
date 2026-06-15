package router

import (
	"github.com/tokenlive/tokenlive-gateway/internal/middleware"

	"github.com/gin-gonic/gin"
)

// InitLLMRouter 初始化 LLM 路由
func InitLLMRouter(deps RouterDeps, r *gin.RouterGroup) {
	cfg := deps.Config
	logger := deps.Logger.Logger

	// 1. 读取 LLM 特有配置
	enableAuth := true
	if cfg.IsSet("llm.enable_auth") {
		enableAuth = cfg.GetBool("llm.enable_auth")
	}

	enableLogging := true
	if cfg.IsSet("llm.enable_logging") {
		enableLogging = cfg.GetBool("llm.enable_logging")
	}

	// 2. 创建 LLM 的路由组
	llmGroup := r.Group("/")

	// 3. 应用中间件
	if enableLogging {
		llmGroup.Use(middleware.NewLoggingMiddleware(&middleware.LoggingConfig{
			Logger:      logger,
			EnableBody:  true,
			MaxBodySize: 4096,
			SkipPaths:   []string{"/health", "/metrics"},
		}))
	}

	// 注意：指标上报已由 Engine 的 OutboundFilter（MetricsFilter）统一处理
	// 不再需要 Gin 级别的 metrics 中间件

	if enableAuth {
		llmGroup.Use(middleware.NewAuthMiddleware(&middleware.AuthConfig{
			Validator: deps.ApiKeyService,
			Logger:    logger,
		}))
	}

	// 4. 注册 LLM API 路由
	{
		// 聊天补全
		llmGroup.POST("/chat/completions", deps.LLMHandler.ChatCompletion)

		// Anthropic Messages 协议
		llmGroup.POST("/messages", deps.LLMHandler.Messages)

		// Responses 协议
		llmGroup.POST("/responses", deps.LLMHandler.Responses)

		// 嵌入向量
		llmGroup.POST("/embeddings", deps.LLMHandler.CreateEmbedding)

		// 模型列表
		llmGroup.GET("/models", deps.LLMHandler.ListModels)
	}
}
