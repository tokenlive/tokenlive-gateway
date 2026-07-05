package inbound

import (
	"context"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// CreditsChecker 余额检查器接口（用于解耦和测试）
type CreditsChecker interface {
	CheckCredits(ctx context.Context, apiKey string) error
}

// CreditsCheckFilter 余额预检过滤器，在请求开始前检查用户 API Key 的余额是否充足
// 仅对个人用户（UserID != ""）进行余额检查，租户跳过
type CreditsCheckFilter struct {
	creditsChecker CreditsChecker
}

// NewCreditsCheckFilter 创建 CreditsCheckFilter
func NewCreditsCheckFilter(checker CreditsChecker) *CreditsCheckFilter {
	return &CreditsCheckFilter{
		creditsChecker: checker,
	}
}

func (f *CreditsCheckFilter) Name() string { return "credits_check" }
func (f *CreditsCheckFilter) Order() int   { return 15 } // 在 AuthFilter(10) 之后，LimitFilter(20) 之前

func (f *CreditsCheckFilter) OnRequest(gctx *core.GatewayContext) error {
	// 无论个人用户还是租户用户，都必须拥有有效的计费策略配置
	if gctx.Policy == nil || gctx.Policy.Billing == nil {
		return &HTTPError{
			Code:    http.StatusForbidden,
			Message: "Model pricing not configured. Please contact your administrator.",
		}
	}

	// 只对个人用户（UserID != ""）进行余额检查
	// 租户场景（Tenant != ""）跳过余额检查
	if gctx.UserID == "" {
		return nil
	}

	// 检查 API Key 余额
	if err := f.creditsChecker.CheckCredits(gctx.Ctx, gctx.APIKey); err != nil {
		// 余额不足，返回 429 错误
		return &HTTPError{
			Code:    http.StatusTooManyRequests,
			Message: "Credits exceeded. Please contact your administrator to recharge.",
		}
	}

	return nil
}
