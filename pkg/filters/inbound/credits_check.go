package inbound

import (
	"context"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// CreditsChecker checks API key balance (decoupled for testing).
type CreditsChecker interface {
	CheckCredits(ctx context.Context, apiKey string) error
}

// CreditsCheckFilter pre-checks API key credits before the request starts.
// Only checks for individual users (UserID != ""); tenants are skipped.
type CreditsCheckFilter struct {
	creditsChecker CreditsChecker
}

// NewCreditsCheckFilter creates a CreditsCheckFilter.
func NewCreditsCheckFilter(checker CreditsChecker) *CreditsCheckFilter {
	return &CreditsCheckFilter{
		creditsChecker: checker,
	}
}

func (f *CreditsCheckFilter) Name() string { return "credits_check" }
func (f *CreditsCheckFilter) Order() int   { return 15 } // after AuthFilter(10), before LimitFilter(20)

func (f *CreditsCheckFilter) OnRequest(gctx *core.GatewayContext) error {
	// billing policy is required for all users
	if gctx.Policy == nil || gctx.Policy.Billing == nil {
		return &HTTPError{
			Code:    http.StatusForbidden,
			Message: "Model pricing not configured. Please contact your administrator.",
		}
	}

	// skip credit check for tenants
	if gctx.UserID == "" {
		return nil
	}

	// check API key balance
	if err := f.creditsChecker.CheckCredits(gctx.Ctx, gctx.APIKey); err != nil {
		// insufficient credits
		return &HTTPError{
			Code:    http.StatusTooManyRequests,
			Message: "Credits exceeded. Please contact your administrator to recharge.",
		}
	}

	return nil
}
