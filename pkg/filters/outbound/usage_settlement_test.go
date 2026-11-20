package outbound

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/usage"
)

type settlementSample struct {
	key         string
	output      int64
	quotaReads  int64
	quotaWrites int64
}

type observedSettlementStore struct {
	mockSettlementStore
	reads   atomic.Int64
	writes  atomic.Int64
	samples chan settlementSample
}

func (s *observedSettlementStore) GetEMA(context.Context, string) (float64, error) {
	s.reads.Add(1)
	return 2, nil
}

func (s *observedSettlementStore) UpdateEMA(_ context.Context, key string, output int64, _ float64) (float64, error) {
	s.samples <- settlementSample{key: key, output: output, quotaReads: s.reads.Load(), quotaWrites: s.writes.Load()}
	return float64(output), nil
}

func (s *observedSettlementStore) RateLimitIncr(ctx context.Context, key string, amount int64, window time.Duration) (int64, error) {
	remaining, err := s.mockSettlementStore.RateLimitIncr(ctx, key, amount, window)
	s.writes.Add(1)
	return remaining, err
}

type observedCreditsDeductor struct {
	gctx   *core.GatewayContext
	usage  usage.Tokens
	cost   float64
	amount int64
	calls  int
	ctxErr error
	err    error
}

func (d *observedCreditsDeductor) DeductCredits(ctx context.Context, _ string, amount int64) (int64, error) {
	d.calls++
	d.amount = amount
	d.usage, d.cost, d.ctxErr = d.gctx.Usage(), d.gctx.Cost, ctx.Err()
	return 0, d.err
}

func TestTokenSettlementFinalizesUsageBeforeEveryEffect(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		initial, want                usage.Tokens
		cost                         float64
		credits, tokenAdjustment     int64
		legacyCost, reservationCost  int64
		inputEstimated, outEstimated bool
	}{
		{"missing-usage", usage.Tokens{}, usage.Tokens{InputTokens: 10, OutputTokens: 5}, 0.04, 40000, 3, 12, 12, true, true},
		{"known-cached-input", usage.Tokens{InputTokens: 20, CachedTokens: 8, CacheCreationTokens: 4}, usage.Tokens{InputTokens: 20, OutputTokens: 5, CachedTokens: 8, CacheCreationTokens: 4}, 0.0476, 47600, 13, 33, 19, false, true},
		{"known-usage", usage.Tokens{InputTokens: 20, OutputTokens: 7, CachedTokens: 8, CacheCreationTokens: 4}, usage.Tokens{InputTokens: 20, OutputTokens: 7, CachedTokens: 8, CacheCreationTokens: 4}, 0.0556, 55600, 15, 41, 27, false, false},
	} {
		for _, reserved := range []bool{false, true} {
			mode := "legacy"
			if reserved {
				mode = "reservation"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				g := &core.GatewayContext{
					Ctx: ctx, Err: core.ErrClientDisconnected,
					UserID: "user", APIKey: "test-api-key", Model: "model",
					IsStream: true, RawBody: []byte("12345678901234567890"), TransmittedChars: 10,
					TrackLimitReservations: reserved,
					Policy: &policy.Policy{
						Billing: &policy.BillingPolicy{InputPrice: 2000, OutputPrice: 4000, CachedPrice: 200, CacheCreationPrice: 2500},
					},
				}
				g.MergeUsage(tc.initial)
				for _, kind := range []string{"token", "cost"} {
					g.Policy.LimitPolicies = append(g.Policy.LimitPolicies, &policy.LimitPolicy{
						Name: kind, Type: kind,
						Estimator:      &policy.EstimatorConfig{Type: "length_ratio", Ratio: 0.5},
						SlidingWindows: []*policy.SlidingWindow{{Threshold: 10000, TimeWindowInMs: 60000}},
					})
				}
				if reserved {
					g.LimitReservations = []core.LimitReservation{
						{Key: "user:model:token:1m0s", Type: "token", Estimated: 12, Window: time.Minute},
						{Key: "user:model:cost:1m0s", Type: "cost", Estimated: 28, Window: time.Minute},
					}
				}
				ss := &observedSettlementStore{samples: make(chan settlementSample, 2)}
				credits := &observedCreditsDeductor{gctx: g}
				if err := NewTokenSettlementFilter(ss, credits, nil).OnResponse(g); err != nil {
					t.Fatal(err)
				}
				if credits.calls != 1 || credits.amount != tc.credits || credits.ctxErr != nil {
					t.Fatalf("credits = calls:%d amount:%d context:%v", credits.calls, credits.amount, credits.ctxErr)
				}
				if credits.usage != tc.want || !almostEqual(credits.cost, tc.cost) || g.Usage() != tc.want || !almostEqual(g.Cost, tc.cost) {
					t.Fatalf("different settlement facts: at debit=%+v/%g final=%+v/%g want=%+v/%g", credits.usage, credits.cost, g.Usage(), g.Cost, tc.want, tc.cost)
				}
				if (g.Tags["input_token_estimated"] == "true") != tc.inputEstimated || (g.Tags["completion_token_estimated"] == "true") != tc.outEstimated {
					t.Fatalf("estimation provenance = %v", g.Tags)
				}
				costAdjustment := tc.legacyCost
				wantReads := int64(2)
				if reserved {
					costAdjustment, wantReads = tc.reservationCost, 0
					for _, reservation := range g.LimitReservations {
						if !reservation.Settled {
							t.Fatal("reservation was not settled")
						}
					}
				}
				if ss.incrCalls["user:model:token:1m0s"] != tc.tokenAdjustment || ss.incrCalls["user:model:cost:1m0s"] != costAdjustment || len(ss.refundCalls) != 0 {
					t.Fatalf("quota adjustments = %v refunds=%v", ss.incrCalls, ss.refundCalls)
				}
				keys := make(map[string]bool)
				for range 2 {
					select {
					case sample := <-ss.samples:
						keys[sample.key] = true
						if sample.output != int64(tc.want.OutputTokens) || sample.quotaReads != wantReads || sample.quotaWrites != 2 {
							t.Fatalf("EMA must use finalized output after quota reconciliation: %+v", sample)
						}
					case <-time.After(time.Second):
						t.Fatal("missing EMA sample")
					}
				}
				if !keys["tenant:user:model"] || !keys["model:global:model"] {
					t.Fatalf("EMA scope = %v", keys)
				}
			})
		}
	}
}

func TestTokenSettlementCreditFailureRetainsFinalUsage(t *testing.T) {
	g := &core.GatewayContext{
		UserID: "user", APIKey: "test-api-key", IsStream: true,
		RawBody: []byte("12345678901234567890"), TransmittedChars: 10,
		Err: core.ErrClientDisconnected,
	}
	ss := &observedSettlementStore{samples: make(chan settlementSample, 2)}
	wantErr := errors.New("credits unavailable")
	credits := &observedCreditsDeductor{gctx: g, err: wantErr}
	if err := NewTokenSettlementFilter(ss, credits, nil).OnResponse(g); !errors.Is(err, wantErr) {
		t.Fatalf("expected credits error, got %v", err)
	}
	// Default ratio is 0.6 for output; the default input estimator is length / 4.
	if g.InputTokens != 5 || g.OutputTokens != 6 || !almostEqual(g.Cost, 0.000022) || credits.amount != 22 {
		t.Fatalf("failure lost finalized facts: usage=%+v cost=%g debit=%d", g.Usage(), g.Cost, credits.amount)
	}
	if ss.reads.Load() != 0 || ss.writes.Load() != 0 || len(ss.samples) != 0 {
		t.Fatal("credits failure must be returned before quota and EMA effects")
	}
}

func TestStandaloneLimitSettlementKeepsChildSideEffectsIsolated(t *testing.T) {
	for _, requestErr := range []error{nil, errors.New("child failed")} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		g := &core.GatewayContext{
			Ctx: ctx, Err: requestErr, UserID: "child", APIKey: "test-api-key",
			IsStream: true, InputTokens: 20, TransmittedChars: 10,
			RawBody: []byte("12345678901234567890"), Cost: 99,
			LimitReservations: []core.LimitReservation{{Key: "reserved", Type: "token", Estimated: 100, Window: time.Minute}},
		}
		ss := &observedSettlementStore{samples: make(chan settlementSample, 2)}
		credits := &observedCreditsDeductor{gctx: g}
		f := NewTokenSettlementFilter(ss, credits, nil)
		for range 2 {
			if err := f.SettleLimits(g); err != nil {
				t.Fatal(err)
			}
		}
		wantRefund := int64(80)
		if requestErr != nil {
			wantRefund = 100
		}
		if ss.refundCalls["reserved"] != wantRefund || !g.LimitReservations[0].Settled {
			t.Fatalf("wrong refund or duplicate settlement: %v", ss.refundCalls)
		}
		if g.InputTokens != 20 || g.OutputTokens != 0 || g.Cost != 99 || len(g.Tags) != 0 || credits.calls != 0 || ss.reads.Load() != 0 || len(ss.samples) != 0 {
			t.Fatal("quota-only child settlement performed outer-response side effects")
		}
	}
}
