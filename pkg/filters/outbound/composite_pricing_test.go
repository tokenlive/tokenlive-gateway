package outbound

import (
	"context"
	"errors"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"testing"
)

func TestCompositeEntryPricingAndOneBusinessSettlement(t *testing.T) {
	for _, mode := range []string{"priority", "rules"} {
		for _, stream := range []bool{false, true} {
			for _, failed := range []bool{false, true} {
				childPrice := 1.0
				g := &core.GatewayContext{Ctx: context.Background(), Model: "smart", OriginalModel: "smart", UserID: "user", APIKey: "test-api-key", IsStream: stream, InputTokens: 100000, OutputTokens: 50000, SmartRouting: &core.SmartRoutingRecord{Mode: mode}, Policy: &policy.Policy{Billing: &policy.BillingPolicy{InputPrice: 10, OutputPrice: 20}}, SelectedEndpoint: &core.Endpoint{InputPrice: &childPrice, OutputPrice: &childPrice}}
				if failed {
					g.Err = errors.New("upstream failed")
				}
				credits := &mockCreditsDeductor{}
				settler := NewTokenSettlementFilter(&mockSettlementStore{}, credits, nil)
				if err := settler.OnResponse(g); err != nil {
					t.Fatal(err)
				}
				if failed {
					if credits.calls != 0 || g.Cost != 0 {
						t.Fatalf("failed answer charged: calls=%v cost=%v", credits.calls, g.Cost)
					}
				} else if credits.calls != 1 || credits.deducted != 2000000 || !almostEqual(g.Cost, 2) {
					t.Fatalf("entry pricing must be 10/20 and charged once: calls=%d credits=%d cost=%v", credits.calls, credits.deducted, g.Cost)
				}
			}
		}
	}
}
func TestCompositePricingDoesNotChangeOrdinaryOrDifficultyPricing(t *testing.T) {
	for _, mode := range []string{"", "difficulty"} {
		price := 1.0
		g := &core.GatewayContext{Policy: &policy.Policy{Billing: &policy.BillingPolicy{InputPrice: 10, OutputPrice: 20}}, SelectedEndpoint: &core.Endpoint{InputPrice: &price, OutputPrice: &price}}
		if mode != "" {
			g.SmartRouting = &core.SmartRoutingRecord{Mode: mode}
		}
		input, output, _, _ := resolvePrices(g)
		if input != 1 || output != 1 {
			t.Fatal("ordinary/legacy endpoint pricing changed")
		}
	}
}
