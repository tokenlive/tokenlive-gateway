package limiter

import (
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// HTTPError HTTP 错误，包含状态码和消息
type HTTPError struct {
	Code    int
	Message string
}

func (e *HTTPError) Error() string {
	return e.Message
}

func getLimitKey(gctx *core.GatewayContext, lp *policy.LimitPolicy) string {
	policyKey := lp.ID
	if policyKey == "" {
		policyKey = lp.Name
	}
	// 向下兼容：如果未配置或长度为0，按原来的默认逻辑（租户或用户）生成
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
