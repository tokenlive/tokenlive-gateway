package handler

import (
	"encoding/base64"
	"net/http"
	"slices"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func isGeminiModelList(c *gin.Context) bool {
	query := c.Request.URL.Query()
	return c.FullPath() == "/v1beta/models" || c.GetHeader("x-goog-api-key") != "" ||
		query.Has("key") || query.Has("pageSize") || query.Has("pageToken")
}

type geminiModel struct {
	Name                       string   `json:"name"`
	BaseModelID                string   `json:"baseModelId"`
	DisplayName                string   `json:"displayName"`
	InputTokenLimit            int64    `json:"inputTokenLimit,omitempty"`
	OutputTokenLimit           int64    `json:"outputTokenLimit,omitempty"`
	SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
}

func (h *LLMHandler) listGeminiModels(c *gin.Context, ids []string) {
	pageSize, err := strconv.Atoi(c.DefaultQuery("pageSize", "50"))
	if err != nil || pageSize < 0 {
		geminiModelListError(c, "pageSize must be a non-negative integer")
		return
	}
	if pageSize == 0 {
		pageSize = 50
	}
	pageSize = min(pageSize, 1000)

	ctx := c.Request.Context()
	models := make(map[string]geminiModel, len(ids))
	for _, id := range ids {
		for _, ep := range h.configManager.GetEndpoints(ctx, id) {
			if ep.ProviderProtocol != string(core.ProtocolGemini) ||
				!slices.Contains(ep.RequestTypes, string(core.RequestTypeGeminiGenerateContent)) {
				continue
			}
			model := geminiModel{
				Name:                       "models/" + id,
				BaseModelID:                id,
				DisplayName:                id,
				InputTokenLimit:            ep.ContextLength,
				OutputTokenLimit:           ep.MaxOutputTokens,
				SupportedGenerationMethods: []string{"generateContent"},
			}
			models[id] = model
			for _, alias := range h.modelAliases(ctx, id) {
				model.Name = "models/" + alias
				model.BaseModelID = alias
				model.DisplayName = alias
				models[alias] = model
			}
			break
		}
	}

	names := make([]string, 0, len(models))
	for name := range models {
		names = append(names, name)
	}
	slices.Sort(names)

	start := 0
	if token := c.Query("pageToken"); token != "" {
		cursor, err := base64.RawURLEncoding.DecodeString(token)
		i, ok := slices.BinarySearch(names, string(cursor))
		if err != nil || !ok {
			geminiModelListError(c, "invalid pageToken")
			return
		}
		start = i + 1
	}
	end := min(start+pageSize, len(names))
	page := make([]geminiModel, 0, end-start)
	for _, name := range names[start:end] {
		page = append(page, models[name])
	}
	resp := gin.H{"models": page}
	if end < len(names) {
		resp["nextPageToken"] = base64.RawURLEncoding.EncodeToString([]byte(names[end-1]))
	}
	c.JSON(http.StatusOK, resp)
}

func geminiModelListError(c *gin.Context, message string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{
		"code":    http.StatusBadRequest,
		"message": message,
		"status":  "INVALID_ARGUMENT",
	}})
}
