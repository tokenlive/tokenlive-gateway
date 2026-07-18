package inbound

import (
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
)

// AuthFilter authorizes user access to the requested model.
type AuthFilter struct{}

// NewAuthFilter creates an AuthFilter.
func NewAuthFilter() *AuthFilter {
	return &AuthFilter{}
}

func (f *AuthFilter) Name() string { return "auth" }
func (f *AuthFilter) Order() int   { return 10 }

func (f *AuthFilter) OnRequest(gctx *core.GatewayContext) error {
	// ensure authentication: UserID or Tenant required
	if gctx.UserID == "" && gctx.Tenant == "" {
		return &HTTPError{Code: http.StatusUnauthorized, Message: "missing authentication user or tenant"}
	}

	// ensure policy is set
	policy := gctx.Policy
	if policy == nil {
		return &HTTPError{Code: http.StatusForbidden, Message: "no policy matched"}
	}

	// check model against policy permission whitelist
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

// HTTPError carries a status code and message.
type HTTPError struct {
	Code    int
	Message string
}

func (e *HTTPError) Error() string {
	return e.Message
}
