package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

type testBurstAdjuster interface {
	RateLimitAdjust(context.Context, string, int64, int64, int64, time.Duration, time.Time) (int64, error)
}

func TestBurstAdjustmentDebtAndRefund(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var ss core.StateStore
			if backend == "memory" {
				ss = NewMemoryStateStore()
			} else {
				ss, _ = newTestRedisStore(t)
			}
			t.Cleanup(func() { require.NoError(t, ss.Close()) })
			adjuster, ok := ss.(testBurstAdjuster)
			require.True(t, ok, "store must support actual-use reconciliation")
			now, ctx := time.Now(), context.Background()
			allowed, remaining, err := ss.RateLimitTake(ctx, "quota", 20, 100, 100, time.Minute, now)
			require.NoError(t, err)
			require.True(t, allowed)
			require.Equal(t, int64(80), remaining)

			remaining, err = adjuster.RateLimitAdjust(ctx, "quota", 480, 100, 100, time.Minute, now)
			require.NoError(t, err)
			require.Equal(t, int64(-400), remaining)
			// A rollback must refund even if the balance is below the negative refund.
			allowed, remaining, err = ss.RateLimitTake(ctx, "quota", -20, 100, 100, time.Minute, now)
			require.NoError(t, err)
			require.True(t, allowed)
			require.Equal(t, int64(-380), remaining)
			remaining, err = adjuster.RateLimitAdjust(ctx, "quota", -80, 100, 100, time.Minute, now)
			require.NoError(t, err)
			require.Equal(t, int64(-300), remaining)

			allowed, remaining, err = ss.RateLimitTake(ctx, "quota", 1, 100, 100, time.Minute, now.Add(3*time.Minute))
			require.NoError(t, err)
			require.False(t, allowed)
			require.Zero(t, remaining)
			allowed, remaining, err = ss.RateLimitTake(ctx, "quota", 1, 100, 100, time.Minute, now.Add(4*time.Minute))
			require.NoError(t, err)
			require.True(t, allowed)
			require.Equal(t, int64(99), remaining)

			remaining, err = adjuster.RateLimitAdjust(ctx, "quota", -500, 100, 100, time.Minute, now.Add(4*time.Minute))
			require.NoError(t, err)
			require.Equal(t, int64(100), remaining, "refund cannot exceed capacity")
		})
	}
}

func TestRedisBurstDebtCannotExpireBeforeFullRefill(t *testing.T) {
	ss, server := newTestRedisStore(t)
	t.Cleanup(func() { require.NoError(t, ss.Close()) })
	adjuster, ok := any(ss).(testBurstAdjuster)
	require.True(t, ok, "store must support actual-use reconciliation")
	ctx, now := context.Background(), time.Now()
	remaining, err := adjuster.RateLimitAdjust(ctx, "quota", 500, 100, 100, time.Minute, now)
	require.NoError(t, err)
	require.Equal(t, int64(-400), remaining)
	require.GreaterOrEqual(t, server.TTL(ss.key("tb", "quota")), 5*time.Minute)

	server.FastForward(3 * time.Minute)
	allowed, remaining, err := ss.RateLimitTake(ctx, "quota", 1, 100, 100, time.Minute, now.Add(3*time.Minute))
	require.NoError(t, err)
	require.False(t, allowed, "two-window TTL must not erase debt")
	require.Equal(t, int64(-100), remaining)
	server.FastForward(2 * time.Minute)
	allowed, remaining, err = ss.RateLimitTake(ctx, "quota", 1, 100, 100, time.Minute, now.Add(5*time.Minute))
	require.NoError(t, err)
	require.True(t, allowed)
	require.Equal(t, int64(99), remaining)
}

func TestRedisBurstAdjustmentPreservesOrdinaryBucketExpiry(t *testing.T) {
	ss, server := newTestRedisStore(t)
	t.Cleanup(func() { require.NoError(t, ss.Close()) })
	allowed, _, err := ss.RateLimitTake(context.Background(), "ordinary", 500, 100, 500, time.Minute, time.Now())
	require.NoError(t, err)
	require.True(t, allowed)
	require.Equal(t, 2*time.Minute, server.TTL(ss.key("tb", "ordinary")),
		"untracked admission must retain its existing expiration policy")
}

func TestRedisBurstDebtHorizonSurvivesLaterAdmission(t *testing.T) {
	ss, server := newTestRedisStore(t)
	t.Cleanup(func() { require.NoError(t, ss.Close()) })
	adjuster, ok := any(ss).(testBurstAdjuster)
	require.True(t, ok)
	ctx, now := context.Background(), time.Now()
	remaining, err := adjuster.RateLimitAdjust(ctx, "quota", 1000, 100, 500, time.Minute, now)
	require.NoError(t, err)
	require.Equal(t, int64(-500), remaining)
	server.FastForward(5*time.Minute + time.Second)
	allowed, _, err := ss.RateLimitTake(ctx, "quota", 1, 100, 500, time.Minute, now.Add(5*time.Minute+time.Second))
	require.NoError(t, err)
	require.True(t, allowed)
	server.FastForward(2 * time.Minute)
	allowed, remaining, err = ss.RateLimitTake(ctx, "quota", 300, 100, 500, time.Minute, now.Add(7*time.Minute+time.Second))
	require.NoError(t, err)
	require.False(t, allowed, "later admission must not reset debt-aware expiration to two windows")
	require.Equal(t, int64(200), remaining)
}
