package inbound

import (
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
)

// AuthFilter 授权过滤器，校验 gctx.User 对当前请求模型的访问权限
type AuthFilter struct{}

// NewAuthFilter 创建 AuthFilter
func NewAuthFilter() *AuthFilter {
	return &AuthFilter{}
}

func (f *AuthFilter) Name() string { return "auth" }
func (f *AuthFilter) Order() int   { return 10 }

func (f *AuthFilter) OnRequest(gctx *core.GatewayContext) error {
	// 1. 确保认证已前置完成，UserID 或 Tenant 至少存在一个
	if gctx.UserID == "" && gctx.Tenant == "" {
		return &HTTPError{Code: http.StatusUnauthorized, Message: "missing authentication user or tenant"}
	}

	// 2. 确保已注入策略对象
	policy := gctx.Policy
	if policy == nil {
		return &HTTPError{Code: http.StatusForbidden, Message: "no policy matched"}
	}

	// 3. 授权校验：匹配 permissions 列表中的模型白名单
	allowed := false
	for _, perm := range policy.Permissions {
		if matcher.MatchWildcard(perm, gctx.Model) {
			allowed = true
			break
		}
	}

	if !allowed {
		return &HTTPError{Code: http.StatusForbidden, Message: "access denied to model: " + gctx.Model}
	}

	return nil
}

// HTTPError HTTP 错误，包含状态码和消息
type HTTPError struct {
	Code    int
	Message string
}

func (e *HTTPError) Error() string {
	return e.Message
}
