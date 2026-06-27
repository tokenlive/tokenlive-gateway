package core

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// parseRequest 解析 HTTP 请求
func (e *Engine) parseRequest(gctx *GatewayContext) error {
	// 解析 RequestType
	gctx.RequestType = resolveRequestType(gctx.Request.URL.Path)

	// 1. 提取 API Key 和 User ID
	apiKey := gctx.Request.Header.Get("X-API-Key")
	if apiKey == "" {
		// 从 Authorization / api-key / x-api-key 提取，保证旧测试或直连场景兼容
		auth := gctx.Request.Header.Get("Authorization")
		if auth != "" {
			parts := strings.SplitN(auth, " ", 2)
			if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
				apiKey = parts[1]
			}
		}
		if apiKey == "" {
			apiKey = gctx.Request.Header.Get("api-key")
		}
		if apiKey == "" {
			apiKey = gctx.Request.Header.Get("x-api-key")
		}
		if apiKey == "" {
			apiKey = gctx.Request.URL.Query().Get("api_key")
		}
	}
	gctx.APIKey = apiKey
	gctx.Tenant = gctx.Request.Header.Get("X-Tenant-ID")
	gctx.UserID = gctx.Request.Header.Get("X-User-ID")
	gctx.WorkspaceID = gctx.Request.Header.Get("X-Workspace-ID")
	gctx.UserTenant = gctx.Request.Header.Get("X-User-Tenant")

	// 读取 body
	if err := e.readBody(gctx); err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	// 提取 model 和 stream
	if len(gctx.RawBody) > 0 {
		gctx.Model = e.extractModel(gctx.RawBody)
		gctx.OriginalModel = gctx.Model
		gctx.IsStream = e.extractStream(gctx.RawBody)
	}

	return nil
}

// matchPipeline 按标准的 RequestType 一对一精准查表匹配，有且仅匹配一份。
func (e *Engine) matchPipeline(rt RequestType) *Pipeline {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. O(1) 通过标准的 RequestType 直接查表匹配
	if p, ok := e.pipelines[string(rt)]; ok {
		return p
	}

	// 2. 图像生成默认共用聊天生成的 Pipeline（若未单独为 image_generation 声明专属 Pipeline）
	if rt == RequestTypeImageGeneration {
		if p, ok := e.pipelines[string(RequestTypeChatCompletion)]; ok {
			return p
		}
	}

	// 3. 通用 fallback
	if p, ok := e.pipelines["default"]; ok {
		return p
	}

	return nil
}

// readBody 读取请求 body
func (e *Engine) readBody(gctx *GatewayContext) error {
	if gctx.Request.Body == nil {
		return nil
	}
	defer gctx.Request.Body.Close()

	body, err := io.ReadAll(gctx.Request.Body)
	if err != nil {
		return err
	}
	gctx.RawBody = body
	return nil
}

// extractModel 从 JSON body 提取 model 字段
func (e *Engine) extractModel(body []byte) string {
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.Model
}

// extractStream 从 JSON body 提取 stream 字段
func (e *Engine) extractStream(body []byte) bool {
	var req struct {
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return false
	}
	return req.Stream
}

// resolveRequestType URL path 映射到 RequestType
func resolveRequestType(path string) RequestType {
	switch {
	case strings.HasSuffix(path, "/chat/completions"):
		return RequestTypeChatCompletion
	case strings.HasSuffix(path, "/messages"):
		return RequestTypeMessages
	case strings.HasSuffix(path, "/embeddings"):
		return RequestTypeEmbedding
	case strings.HasSuffix(path, "/images/generations"):
		return RequestTypeImageGeneration
	case strings.HasSuffix(path, "/responses"):
		return RequestTypeResponses
	default:
		return RequestTypeChatCompletion
	}
}
