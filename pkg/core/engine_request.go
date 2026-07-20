package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// parseRequest parses the HTTP request.
func (e *Engine) parseRequest(gctx *GatewayContext) error {
	// Resolve RequestType
	gctx.RequestType = resolveRequestType(gctx.Request.URL.Path)

	// 1. Extract API Key and User ID
	apiKey := gctx.Request.Header.Get("X-API-Key")
	if apiKey == "" {
		// Extract from Authorization / api-key / x-api-key for legacy test and direct-connect compatibility
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
	gctx.APIKeyID = gctx.Request.Header.Get("X-API-Key-ID")
	gctx.APIKeyHash = gctx.Request.Header.Get("X-API-Key-Hash")
	gctx.Tenant = gctx.Request.Header.Get("X-Tenant-ID")
	gctx.UserID = gctx.Request.Header.Get("X-User-ID")
	gctx.WorkspaceID = gctx.Request.Header.Get("X-Workspace-ID")
	gctx.UserTenant = gctx.Request.Header.Get("X-User-Tenant")

	// Read body
	if err := e.readBody(gctx); err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	// Extract model and stream
	if gctx.RequestType == RequestTypeGeminiGenerateContent {
		gctx.Model = extractGeminiModelFromPath(gctx.Request.URL.Path)
		gctx.OriginalModel = gctx.Model
		gctx.IsStream = isGeminiStreamPath(gctx.Request.URL.Path)
	} else if len(gctx.RawBody) > 0 {
		gctx.Model = e.extractModel(gctx.RawBody)
		gctx.OriginalModel = gctx.Model
		gctx.IsStream = e.extractStream(gctx.RawBody)
	}

	return nil
}

// matchPipeline matches a Pipeline by standard RequestType with exact 1:1 lookup.
func (e *Engine) matchPipeline(rt RequestType) *Pipeline {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. O(1) direct lookup by standard RequestType
	if p, ok := e.pipelines[string(rt)]; ok {
		return p
	}

	// 2. Image generation shares the chat completion pipeline by default (if no dedicated pipeline declared)
	if rt == RequestTypeImageGeneration {
		if p, ok := e.pipelines[string(RequestTypeChatCompletion)]; ok {
			return p
		}
	}

	// 3. Generic fallback
	if p, ok := e.pipelines["default"]; ok {
		return p
	}

	return nil
}

// readBody reads the request body.
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

// extractModel extracts the model field from JSON body.
func (e *Engine) extractModel(body []byte) string {
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return req.Model
}

// extractStream extracts the stream field from JSON body.
func (e *Engine) extractStream(body []byte) bool {
	var req struct {
		Stream bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return false
	}
	return req.Stream
}

// resolveRequestType maps URL path to RequestType.
func resolveRequestType(path string) RequestType {
	switch {
	case isGeminiGenerateContentPath(path):
		return RequestTypeGeminiGenerateContent
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

func isGeminiGenerateContentPath(path string) bool {
	return strings.Contains(path, "/models/") &&
		(strings.HasSuffix(path, ":generateContent") || strings.HasSuffix(path, ":streamGenerateContent"))
}

func isGeminiStreamPath(path string) bool {
	return strings.Contains(path, "/models/") && strings.HasSuffix(path, ":streamGenerateContent")
}

func extractGeminiModelFromPath(path string) string {
	const marker = "/models/"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	modelWithMethod := path[idx+len(marker):]
	switch {
	case strings.HasSuffix(modelWithMethod, ":generateContent"):
		modelWithMethod = strings.TrimSuffix(modelWithMethod, ":generateContent")
	case strings.HasSuffix(modelWithMethod, ":streamGenerateContent"):
		modelWithMethod = strings.TrimSuffix(modelWithMethod, ":streamGenerateContent")
	default:
		return ""
	}
	model, err := url.PathUnescape(modelWithMethod)
	if err != nil {
		return modelWithMethod
	}
	return model
}
