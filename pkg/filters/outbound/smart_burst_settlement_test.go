package outbound

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

func smartBurstStores(t *testing.T, test func(*testing.T, core.StateStore)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		ss := store.NewMemoryStateStore()
		t.Cleanup(func() { require.NoError(t, ss.Close()) })
		test(t, ss)
	})
	t.Run("redis", func(t *testing.T) {
		server := miniredis.RunT(t)
		ss := store.NewRedisStateStore(redis.NewClient(&redis.Options{Addr: server.Addr()}), nil)
		t.Cleanup(func() { require.NoError(t, ss.Close()) })
		test(t, ss)
	})
}

// Removing the reconciliation debit must leave free quota and fail this test.
func TestSmartBurstSettlementRecordsActualUsageDebt(t *testing.T) {
	smartBurstStores(t, func(t *testing.T, ss core.StateStore) {
		burst := 1.0
		g := &core.GatewayContext{
			Ctx: context.Background(), Model: "smart", UserID: "user", RawBody: []byte("12345678"),
			TrackLimitReservations: true,
			Policy: &policy.Policy{LimitPolicies: []*policy.LimitPolicy{{
				ID: "tokens", Type: "token",
				SlidingWindows: []*policy.SlidingWindow{{
					Threshold: 500, TimeWindowInMs: 3600000, BurstRatio: &burst,
				}},
			}}},
		}
		require.NoError(t, inbound.NewRateLimitFilter(ss).OnRequest(g))
		require.Len(t, g.LimitReservations, 1)
		require.Equal(t, int64(202), g.LimitReservations[0].Estimated)
		g.Model = "strong"
		g.InputTokens, g.OutputTokens = 100, 900
		settler := NewTokenSettlementFilter(ss, nil, nil)
		require.NoError(t, settler.SettleLimits(g))
		require.True(t, g.LimitReservations[0].Settled)
		require.NoError(t, settler.SettleLimits(g))

		now := time.Now()
		key := "user:smart:tokens:1h0m0s"
		allowed, remaining, err := ss.RateLimitTake(context.Background(), key, 1, 500, 500, time.Hour, now)
		require.NoError(t, err)
		require.False(t, allowed, "actual usage exceeds capacity; next admission must be denied")
		require.InDelta(t, -500, remaining, 1, "only actual minus precharge is debited, once")

		allowed, _, err = ss.RateLimitTake(context.Background(), key, 1, 500, 500, time.Hour, now.Add(30*time.Minute))
		require.NoError(t, err)
		require.False(t, allowed, "partial refill must repay debt before admission")
		allowed, _, err = ss.RateLimitTake(context.Background(), key, 1, 500, 500, time.Hour, now.Add(61*time.Minute))
		require.NoError(t, err)
		require.True(t, allowed, "admission resumes after debt and requested tokens refill")

		_, remaining, err = ss.RateLimitTake(context.Background(), "user:strong:tokens:1h0m0s", 0, 500, 500, time.Hour, now)
		require.NoError(t, err)
		require.Equal(t, int64(500), remaining, "model changes must not move the reservation")
		counter, err := ss.RateLimitIncr(context.Background(), key, 0, time.Hour)
		require.NoError(t, err)
		require.Zero(t, counter, "burst debt must not be written to fixed-window counters")
	})
}

func TestSmartBurstSettlementRefundsExactEstimateOnceWithNilContext(t *testing.T) {
	smartBurstStores(t, func(t *testing.T, ss core.StateStore) {
		// A future last-update time makes refill deterministic during settlement.
		now := time.Now().Add(time.Hour)
		allowed, _, err := ss.RateLimitTake(context.Background(), "reserved", 202, 500, 500, time.Hour, now)
		require.NoError(t, err)
		require.True(t, allowed)
		g := &core.GatewayContext{
			InputTokens: 5, OutputTokens: 7,
			LimitReservations: []core.LimitReservation{{
				Key: "reserved", Type: "token", Estimated: 202, Burst: true,
				Rate: 500, Capacity: 500, Window: time.Hour,
			}},
		}
		settler := NewTokenSettlementFilter(ss, nil, nil)
		require.NoError(t, settler.SettleLimits(g))
		require.NoError(t, settler.SettleLimits(g))
		_, remaining, err := ss.RateLimitTake(context.Background(), "reserved", 0, 500, 500, time.Hour, now)
		require.NoError(t, err)
		require.Equal(t, int64(488), remaining)
	})
}

type failingBurstAdjustmentStore struct {
	core.StateStore
	failKey string
}

func (s *failingBurstAdjustmentStore) RateLimitAdjust(ctx context.Context, key string, tokens, rate, capacity int64, window time.Duration, now time.Time) (int64, error) {
	if key == s.failKey {
		return 0, errors.New("injected adjustment failure")
	}
	adjuster, ok := s.StateStore.(interface {
		RateLimitAdjust(context.Context, string, int64, int64, int64, time.Duration, time.Time) (int64, error)
	})
	if !ok {
		return 0, errors.New("adjustment unavailable")
	}
	return adjuster.RateLimitAdjust(ctx, key, tokens, rate, capacity, window, now)
}

func TestSmartBurstSettlementRetriesOnlyUnsatisfiedReservations(t *testing.T) {
	ss := &failingBurstAdjustmentStore{StateStore: store.NewMemoryStateStore(), failKey: "second"}
	t.Cleanup(func() { require.NoError(t, ss.Close()) })
	now := time.Now().Add(time.Hour)
	g := &core.GatewayContext{Ctx: context.Background(), InputTokens: 1000}
	for _, key := range []string{"first", "second"} {
		allowed, _, err := ss.RateLimitTake(context.Background(), key, 202, 500, 500, time.Hour, now)
		require.NoError(t, err)
		require.True(t, allowed)
		g.LimitReservations = append(g.LimitReservations, core.LimitReservation{
			Key: key, Type: "token", Estimated: 202, Burst: true,
			Rate: 500, Capacity: 500, Window: time.Hour,
		})
	}
	settler := NewTokenSettlementFilter(ss, nil, nil)
	require.Error(t, settler.SettleLimits(g))
	require.True(t, g.LimitReservations[0].Settled)
	require.False(t, g.LimitReservations[1].Settled)
	ss.failKey = ""
	require.NoError(t, settler.SettleLimits(g))
	require.NoError(t, settler.SettleLimits(g))
	for _, key := range []string{"first", "second"} {
		_, remaining, err := ss.RateLimitTake(context.Background(), key, 0, 500, 500, time.Hour, now)
		require.NoError(t, err)
		require.Equal(t, int64(-500), remaining, key)
	}
}

func TestSmartBurstSettlementUnsupportedStoreDoesNotClaimSuccess(t *testing.T) {
	// Hide optional capabilities without changing the backward-compatible interface.
	ss := struct{ core.StateStore }{store.NewMemoryStateStore()}
	t.Cleanup(func() { require.NoError(t, ss.Close()) })
	allowed, _, err := ss.RateLimitTake(context.Background(), "reserved", 202, 500, 500, time.Hour, time.Now())
	require.NoError(t, err)
	require.True(t, allowed)
	g := &core.GatewayContext{
		Ctx: context.Background(), InputTokens: 1000,
		LimitReservations: []core.LimitReservation{{
			Key: "reserved", Type: "token", Estimated: 202, Burst: true,
			Rate: 500, Capacity: 500, Window: time.Hour,
		}},
	}
	require.Error(t, NewTokenSettlementFilter(ss, nil, nil).SettleLimits(g))
	require.False(t, g.LimitReservations[0].Settled)
}
