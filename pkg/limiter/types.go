package limiter

import (
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// HTTPError is an HTTP-facing error with status code and message.
type HTTPError struct {
	Code         int
	Message      string
	Threshold    *float64
	CurrentValue *float64
}

func (e *HTTPError) Error() string {
	return e.Message
}

func float64Ptr(v float64) *float64 {
	return &v
}

func GetLimitKey(gctx *core.GatewayContext, lp *policy.LimitPolicy) string {
	policyKey := lp.ID
	if policyKey == "" {
		policyKey = lp.Name
	}
	// Backward compat: empty LimitBy → user or tenant.
	if len(lp.LimitBy) == 0 {
		id := gctx.UserID
		if id == "" {
			id = gctx.Tenant
		}
		return id + ":" + gctx.Model + ":" + policyKey
	}

	var parts []string
	for _, field := range lp.LimitBy {
		switch field {
		case "tenant":
			if gctx.Tenant != "" {
				parts = append(parts, gctx.Tenant)
			}
		case "user":
			if gctx.UserID != "" {
				parts = append(parts, gctx.UserID)
			}
		case "model":
			if gctx.Model != "" {
				parts = append(parts, gctx.Model)
			}
		}
	}
	parts = append(parts, policyKey)
	return strings.Join(parts, ":")
}
