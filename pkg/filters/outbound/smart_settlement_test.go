package outbound

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

func TestSmartReservationsSettleOriginalBucketOnceAfterModelChanges(t *testing.T) {
	ss := store.NewMemoryStateStore()
	defer ss.Close()
	gctx := &core.GatewayContext{
		Ctx: context.Background(), Model: "smart", UserID: "u",
		RawBody: []byte("12345678"), TrackLimitReservations: true,
		Policy: &policy.Policy{LimitPolicies: []*policy.LimitPolicy{{
			ID: "tokens", Type: "token", SlidingWindows: []*policy.SlidingWindow{{Threshold: 10000, TimeWindowInMs: 60000}},
		}}},
	}
	if err := inbound.NewRateLimitFilter(ss).OnRequest(gctx); err != nil {
		t.Fatal(err)
	}
	gctx.Model = "strong"
	gctx.RawBody = []byte("changed")
	gctx.InputTokens, gctx.OutputTokens = 5, 7
	settler := NewTokenSettlementFilter(ss, nil, nil)
	for i := 0; i < 2; i++ {
		if err := settler.SettleLimits(gctx); err != nil {
			t.Fatal(err)
		}
	}
	original, _ := ss.RateLimitIncr(context.Background(), "u:smart:tokens:1m0s", 0, time.Minute)
	other, _ := ss.RateLimitIncr(context.Background(), "u:strong:tokens:1m0s", 0, time.Minute)
	if original != 12 || other != 0 {
		t.Fatalf("wrong settlement: original=%d target=%d", original, other)
	}
}

func TestSmartReservationFailureRefundsOnlySuccessfulPrecharges(t *testing.T) {
	ss := store.NewMemoryStateStore()
	defer ss.Close()
	gctx := &core.GatewayContext{
		Ctx: context.Background(), Model: "smart", UserID: "u", TrackLimitReservations: true,
		Policy: &policy.Policy{LimitPolicies: []*policy.LimitPolicy{
			{ID: "pass", Type: "token", SlidingWindows: []*policy.SlidingWindow{{Threshold: 1000, TimeWindowInMs: 60000}}},
			{ID: "deny", Type: "token", SlidingWindows: []*policy.SlidingWindow{{Threshold: 1, TimeWindowInMs: 60000}}},
		}},
	}
	if err := inbound.NewRateLimitFilter(ss).OnRequest(gctx); err == nil {
		t.Fatal("expected hard quota rejection")
	}
	gctx.Err = errors.New("rejected")
	if err := NewTokenSettlementFilter(ss, nil, nil).SettleLimits(gctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"pass", "deny"} {
		n, _ := ss.RateLimitIncr(context.Background(), "u:smart:"+id+":1m0s", 0, time.Minute)
		if n != 0 {
			t.Fatalf("bucket %s retained or over-refunded charge: %d", id, n)
		}
	}
}
