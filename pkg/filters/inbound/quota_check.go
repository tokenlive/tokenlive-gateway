package inbound

import (
	"context"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// QuotaChecker 配额检查器接口（用于解耦和测试）
type QuotaChecker interface {
	CheckQuota(ctx context.Context, apiKey string) error
}

// QuotaCheckFilter 配额预检过滤器，在请求开始前检查用户 API Key 的配额是否充足
// 仅对个人用户（UserID != ""）进行配额检查，租户跳过
type QuotaCheckFilter struct {
	quotaChecker QuotaChecker
}

// NewQuotaCheckFilter 创建 QuotaCheckFilter
func NewQuotaCheckFilter(checker QuotaChecker) *QuotaCheckFilter {
	return &QuotaCheckFilter{
		quotaChecker: checker,
	}
}

func (f *QuotaCheckFilter) Name() string { return "quota_check" }
func (f *QuotaCheckFilter) Order() int   { return 15 } // 在 AuthFilter(10) 之后，LimitFilter(20) 之前

func (f *QuotaCheckFilter) OnRequest(gctx *core.GatewayContext) error {
	// 只对个人用户（UserID != ""）进行配额检查
	// 租户场景（Tenant != ""）跳过配额检查
	if gctx.UserID == "" {
		return nil
	}

	// 检查 API Key 配额
	if err := f.quotaChecker.CheckQuota(gctx.Ctx, gctx.APIKey); err != nil {
		// 配额不足，返回 429 错误
		return &HTTPError{
			Code:    http.StatusTooManyRequests,
			Message: "Insufficient quota. Please contact your administrator to increase your quota.",
		}
	}

	return nil
}
