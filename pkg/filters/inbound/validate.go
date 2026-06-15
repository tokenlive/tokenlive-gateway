package inbound

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// ModelValidator 校验 model 是否存在且合法（区分租户 ToB 与个人 ToC）
type ModelValidator interface {
	ValidateModel(ctx context.Context, model string, tenant string, userID string) (bool, error)
}

// ValidateFilter 请求校验过滤器，校验 model 是否存在且合法
type ValidateFilter struct {
	validator ModelValidator
}

// NewValidateFilter 创建 ValidateFilter
func NewValidateFilter(validator ModelValidator) *ValidateFilter {
	return &ValidateFilter{validator: validator}
}

func (f *ValidateFilter) Name() string { return "validate" }
func (f *ValidateFilter) Order() int   { return 30 }

func (f *ValidateFilter) OnRequest(gctx *core.GatewayContext) error {
	if gctx.Model == "" {
		return &HTTPError{Code: http.StatusBadRequest, Message: "model is required"}
	}

	// toC 场景：tenant 为空时回退使用 userTenant（用户所属租户）
	tenant := gctx.Tenant
	if tenant == "" {
		tenant = gctx.UserTenant
	}

	valid, err := f.validator.ValidateModel(gctx.Ctx, gctx.Model, tenant, gctx.UserID)
	if err != nil {
		return &HTTPError{Code: http.StatusBadRequest, Message: "failed to validate model: " + err.Error()}
	}
	if !valid {
		return &HTTPError{Code: http.StatusBadRequest, Message: "unknown model: " + gctx.Model}
	}

	if len(gctx.RawBody) > 0 {
		switch gctx.RequestType {
		case core.RequestTypeChatCompletion:
			var body struct {
				Messages []json.RawMessage `json:"messages"`
			}
			if err := json.Unmarshal(gctx.RawBody, &body); err != nil {
				return &HTTPError{Code: http.StatusBadRequest, Message: "invalid JSON body"}
			}
			if len(body.Messages) == 0 {
				return &HTTPError{Code: http.StatusBadRequest, Message: "messages is required and must not be empty"}
			}
		case core.RequestTypeMessages:
			return f.validateAnthropicMessages(gctx.RawBody)
		case core.RequestTypeResponses:
			var body interface{}
			if err := json.Unmarshal(gctx.RawBody, &body); err != nil {
				return &HTTPError{Code: http.StatusBadRequest, Message: "invalid JSON body"}
			}
		}
	}
	return nil
}

func (f *ValidateFilter) validateAnthropicMessages(body []byte) error {
	var req struct {
		Messages  []json.RawMessage `json:"messages"`
		MaxTokens *int              `json:"max_tokens"` // 指针区分缺省 vs 显式 0
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return &HTTPError{Code: http.StatusBadRequest, Message: "invalid JSON body"}
	}
	if len(req.Messages) == 0 {
		return &HTTPError{Code: http.StatusBadRequest, Message: "messages is required and must not be empty"}
	}
	if req.MaxTokens == nil {
		return &HTTPError{Code: http.StatusBadRequest, Message: "max_tokens is required"}
	}
	if *req.MaxTokens <= 0 {
		return &HTTPError{Code: http.StatusBadRequest, Message: "max_tokens must be positive"}
	}
	return nil
}
