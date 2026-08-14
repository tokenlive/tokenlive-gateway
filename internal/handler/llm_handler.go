package handler

import (
	"context"
	"net/http"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/gin-gonic/gin"
)

// modelLister 抽象租户授权模型读取，用于测试注入。
type modelLister interface {
	ListTenantModels(ctx context.Context, tenant string) ([]string, error)
}

// modelOwner 抽象 model→provider 归属解析与所有已知模型查询，用于测试注入。
type modelOwner interface {
	OwnerOf(ctx context.Context, model string) string
	ModelCapacityOf(ctx context.Context, model string) (int64, int64)
	AllKnownModels() map[string]bool
}

// aliasQuerier 抽象别名查询，用于测试注入。
type aliasQuerier interface {
	GetAliases(ctx context.Context, modelCode string) ([]string, error)
}

// LLMHandler LLM 请求处理器（薄 Gin 适配器）
type LLMHandler struct {
	engine *core.Engine
	// modelService 与 configManager 服务于 ListModels 的权限化输出；
	// ChatCompletion / CreateEmbedding 不依赖它们。
	modelService  modelLister
	configManager modelOwner
	aliasService  aliasQuerier
}

// NewLLMHandler 创建 LLM Handler（生产用，注入具体类型）。
func NewLLMHandler(
	engine *core.Engine,
	modelService *service.ModelService,
	configManager *config.ConfigManager,
	aliasService *service.AliasService,
) *LLMHandler {
	return &LLMHandler{
		engine:        engine,
		modelService:  modelService,
		configManager: configManager,
		aliasService:  aliasService,
	}
}

// NewLLMHandlerWithDeps 用接口形态注入依赖，engine 显式置 nil。
// 仅用于本包测试或不需要 LLM 流量委托的场景。
// WARNING: 用此构造函数生成的 handler 调用 ChatCompletion 或 CreateEmbedding 会 nil panic，
// 因为 engine == nil。生产 HTTP 路由请使用 NewLLMHandler。
func NewLLMHandlerWithDeps(modelService modelLister, configManager modelOwner, aliasService aliasQuerier) *LLMHandler {
	return &LLMHandler{
		modelService:  modelService,
		configManager: configManager,
		aliasService:  aliasService,
	}
}

// ChatCompletion 处理聊天补全请求
func (h *LLMHandler) ChatCompletion(c *gin.Context) {
	h.engine.HandleRequest(c.Writer, c.Request)
}

// CreateEmbedding 处理向量生成请求
func (h *LLMHandler) CreateEmbedding(c *gin.Context) {
	h.engine.HandleRequest(c.Writer, c.Request)
}

// CreateImage 处理图像生成请求
func (h *LLMHandler) CreateImage(c *gin.Context) {
	h.engine.HandleRequest(c.Writer, c.Request)
}

// Messages 处理 Anthropic Messages 协议请求
func (h *LLMHandler) Messages(c *gin.Context) {
	h.engine.HandleRequest(c.Writer, c.Request)
}

// Responses 处理 OpenAI Responses 协议请求
func (h *LLMHandler) Responses(c *gin.Context) {
	if strings.ToLower(c.GetHeader("Upgrade")) == "websocket" {
		h.engine.HandleWebSocketRequest(c.Writer, c.Request)
		return
	}
	h.engine.HandleRequest(c.Writer, c.Request)
}

// GeminiGenerateContent 处理 Gemini 原生 generateContent / streamGenerateContent 协议请求。
func (h *LLMHandler) GeminiGenerateContent(c *gin.Context) {
	h.engine.HandleRequest(c.Writer, c.Request)
}

// ListModels 返回当前 API Key 授权的模型列表（区分 ToB 与 ToC 逻辑，不再调用 Engine）。
func (h *LLMHandler) ListModels(c *gin.Context) {
	tenant := c.GetString("tenant")
	userID := c.GetString("user_id")

	if tenant == "" && userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": gin.H{
				"message": "Missing or invalid API key",
				"type":    "authentication_error",
			},
		})
		return
	}

	var ids []string
	if tenant != "" {
		// ToB 租户场景：返回租户白名单授权模型
		ids, _ = h.modelService.ListTenantModels(c.Request.Context(), tenant)
	} else {
		// ToC 个人场景：返回网关中所有已开通的可用模型
		knownMap := h.configManager.AllKnownModels()
		ids = make([]string, 0, len(knownMap))
		for mName := range knownMap {
			ids = append(ids, mName)
		}
	}

	// 如果返回了通配符 "*"，说明不限模型，提取系统所有已知模型
	if len(ids) == 1 && ids[0] == "*" {
		known := h.configManager.AllKnownModels()
		ids = make([]string, 0, len(known))
		for k := range known {
			ids = append(ids, k)
		}
	}

	ctx := c.Request.Context()
	data := make([]gin.H, 0, len(ids))

	for _, id := range ids {
		owner := h.configManager.OwnerOf(ctx, id)
		if owner == "" {
			owner = "github.com/tokenlive/tokenlive-gateway"
		}
		contextLength, maxOutputTokens := h.configManager.ModelCapacityOf(ctx, id)

		// 添加主模型
		data = append(data, gin.H{
			"id":                id,
			"object":            "model",
			"created":           0,
			"owned_by":          owner,
			"context_length":    contextLength,
			"max_output_tokens": maxOutputTokens,
		})

		// 查询并添加别名作为独立模型（继承主模型容量）
		if h.aliasService != nil {
			if aliases, err := h.aliasService.GetAliases(ctx, id); err == nil {
				for _, alias := range aliases {
					data = append(data, gin.H{
						"id":                alias,
						"object":            "model",
						"created":           0,
						"owned_by":          owner,
						"context_length":    contextLength,
						"max_output_tokens": maxOutputTokens,
					})
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"object": "list",
		"data":   data,
	})
}
