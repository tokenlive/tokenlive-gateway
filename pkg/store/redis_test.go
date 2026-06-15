package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRedisStore 创建一个使用 miniredis 的 RedisStateStore，返回 store 和清理函数。
func newTestRedisStore(t *testing.T) (*RedisStateStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisStateStore(client, nil)
	return store, mr
}

func TestRedisStateStore_ImplementsInterface(t *testing.T) {
	var _ core.StateStore = (*RedisStateStore)(nil)
}

// ==================== 限流 ====================

func TestRedis_RateLimitIncr_Basic(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	current, err := s.RateLimitIncr(ctx, "user:1", 100, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100), current)

	current, err = s.RateLimitIncr(ctx, "user:1", 200, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(300), current)
}

func TestRedis_RateLimitIncr_LargeValues(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	current, err := s.RateLimitIncr(ctx, "user:1", 100000, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100000), current)

	current, err = s.RateLimitIncr(ctx, "user:1", 1, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100001), current)
}

func TestRedis_RateLimitIncr_WindowReset(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 使用极短窗口
	current, err := s.RateLimitIncr(ctx, "user:1", 5000, 50*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(5000), current)

	// 快进 miniredis 时间以触发窗口重置
	mr.FastForward(60 * time.Millisecond)

	current, err = s.RateLimitIncr(ctx, "user:1", 100, 50*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(100), current)
}

func TestRedis_RateLimitIncr_DifferentKeys(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	_, err := s.RateLimitIncr(ctx, "user:1", 500, time.Minute)
	require.NoError(t, err)

	_, err = s.RateLimitIncr(ctx, "user:2", 300, time.Minute)
	require.NoError(t, err)

	r1, _ := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	r2, _ := s.RateLimitIncr(ctx, "user:2", 0, time.Minute)
	assert.Equal(t, int64(500), r1)
	assert.Equal(t, int64(300), r2)
}

func TestRedis_RateLimitRefund(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	_, err := s.RateLimitIncr(ctx, "user:1", 500, time.Minute)
	require.NoError(t, err)

	err = s.RateLimitRefund(ctx, "user:1", 200)
	require.NoError(t, err)

	current, err := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(300), current) // 500 - 200 = 300
}

func TestRedis_RateLimitRefund_NegativeClamp(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 没有消耗就退还，计数应保持为 0
	err := s.RateLimitRefund(ctx, "user:1", 100)
	require.NoError(t, err)

	current, err := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(0), current)
}

func TestRedis_RateLimit_TTLFixedWindow(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 第一次调用应该设置 TTL
	_, err := s.RateLimitIncr(ctx, "user:1", 100, time.Minute)
	require.NoError(t, err)

	redisKey := s.key("rl", "user:1")
	ttl := mr.TTL(redisKey)
	assert.Greater(t, ttl, time.Duration(0), "TTL should be set on first call")
	assert.LessOrEqual(t, ttl, time.Minute, "TTL should not exceed window size")

	// 后续调用不应该刷新 TTL，以确保窗口固定、能正常恢复
	mr.FastForward(30 * time.Second)
	_, err = s.RateLimitIncr(ctx, "user:1", 100, time.Minute)
	require.NoError(t, err)

	ttl = mr.TTL(redisKey)
	assert.LessOrEqual(t, ttl, 30*time.Second, "TTL should not be refreshed (remains in the fixed window)")
	assert.Greater(t, ttl, time.Duration(0), "TTL should still be positive before expiration")
}

func TestRedis_RateLimit_RefundAfterExpiry(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 消耗配额
	_, err := s.RateLimitIncr(ctx, "user:1", 500, time.Minute)
	require.NoError(t, err)

	// key 过期
	mr.FastForward(2 * time.Minute)

	// Refund 不应该重建 key（因为已过期）
	err = s.RateLimitRefund(ctx, "user:1", 200)
	require.NoError(t, err)

	// 验证 key 不存在
	redisKey := s.key("rl", "user:1")
	exists := mr.Exists(redisKey)
	assert.False(t, exists, "expired key should not be recreated by refund")

	// 新请求应该创建新的窗口
	current, err := s.RateLimitIncr(ctx, "user:1", 100, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100), current, "should start fresh window")
}

func TestRedis_RateLimit_RefundPreservesTTL(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 消耗配额
	_, err := s.RateLimitIncr(ctx, "user:1", 500, time.Minute)
	require.NoError(t, err)

	redisKey := s.key("rl", "user:1")
	ttlBefore := mr.TTL(redisKey)

	// Refund
	err = s.RateLimitRefund(ctx, "user:1", 200)
	require.NoError(t, err)

	// TTL 应该保持不变（或非常接近）
	ttlAfter := mr.TTL(redisKey)
	assert.InDelta(t, ttlBefore.Seconds(), ttlAfter.Seconds(), 1.0, "refund should preserve TTL")
}

// ==================== Sticky Session ====================

func TestRedis_Sticky_SetAndGet(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	err := s.StickySet(ctx, "session:abc", "ep:1", 5*time.Minute)
	require.NoError(t, err)

	endpointID, err := s.StickyGet(ctx, "session:abc")
	require.NoError(t, err)
	assert.Equal(t, "ep:1", endpointID)
}

func TestRedis_Sticky_NotFound(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	_, err := s.StickyGet(ctx, "session:nonexistent")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestRedis_Sticky_TTLExpiry(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	err := s.StickySet(ctx, "session:abc", "ep:1", 50*time.Millisecond)
	require.NoError(t, err)

	// 立即获取，应该存在
	endpointID, err := s.StickyGet(ctx, "session:abc")
	require.NoError(t, err)
	assert.Equal(t, "ep:1", endpointID)

	// 快进超过 TTL
	mr.FastForward(60 * time.Millisecond)

	_, err = s.StickyGet(ctx, "session:abc")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestRedis_Sticky_Overwrite(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	s.StickySet(ctx, "session:abc", "ep:1", 5*time.Minute)
	s.StickySet(ctx, "session:abc", "ep:2", 5*time.Minute)

	endpointID, err := s.StickyGet(ctx, "session:abc")
	require.NoError(t, err)
	assert.Equal(t, "ep:2", endpointID)
}

// ==================== 延迟统计 ====================

func TestRedis_Latency_RecordAndGetAvg(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.nowFunc = func() time.Time { return baseTime }

	s.RecordLatency(ctx, "ep:1", 100*time.Millisecond)
	s.RecordLatency(ctx, "ep:1", 200*time.Millisecond)
	s.RecordLatency(ctx, "ep:1", 300*time.Millisecond)

	avg, err := s.GetAvgLatency(ctx, "ep:1", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 200*time.Millisecond, avg) // (100+200+300)/3
}

func TestRedis_Latency_Empty(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	avg, err := s.GetAvgLatency(ctx, "ep:nonexistent", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), avg)
}

func TestRedis_Latency_DifferentEndpoints(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.nowFunc = func() time.Time { return baseTime }

	s.RecordLatency(ctx, "ep:1", 100*time.Millisecond)
	s.RecordLatency(ctx, "ep:2", 500*time.Millisecond)

	avg1, _ := s.GetAvgLatency(ctx, "ep:1", time.Hour)
	avg2, _ := s.GetAvgLatency(ctx, "ep:2", time.Hour)
	assert.Equal(t, 100*time.Millisecond, avg1)
	assert.Equal(t, 500*time.Millisecond, avg2)
}

func TestRedis_Latency_WindowFiltering(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.nowFunc = func() time.Time { return baseTime }

	// 记录一个旧样本
	s.RecordLatency(ctx, "ep:1", 1*time.Second)

	// 时间前进 2 小时（旧样本超出 1 小时窗口）
	s.nowFunc = func() time.Time { return baseTime.Add(2 * time.Hour) }

	// 再记录一个新样本
	s.RecordLatency(ctx, "ep:1", 100*time.Millisecond)

	// 1 小时窗口内只有 100ms
	avg, err := s.GetAvgLatency(ctx, "ep:1", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 100*time.Millisecond, avg)
}

func TestRedis_Latency_TTLIsSet(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 记录延迟样本
	err := s.RecordLatency(ctx, "ep:1", 100*time.Millisecond)
	require.NoError(t, err)

	// 验证 key 存在且有 TTL
	redisKey := s.key("latency", "ep:1")
	exists := mr.Exists(redisKey)
	assert.True(t, exists, "latency key should exist")

	ttl := mr.TTL(redisKey)
	expectedTTL := redisLatencyKeyTTL
	assert.Greater(t, ttl, time.Duration(0), "TTL should be set")
	assert.LessOrEqual(t, ttl, expectedTTL, "TTL should not exceed configured value")

	// 快进超过 TTL，key 应该被清理
	mr.FastForward(redisLatencyKeyTTL + time.Second)

	exists = mr.Exists(redisKey)
	assert.False(t, exists, "latency key should expire after TTL")
}

func TestRedis_Latency_TTLRefreshOnUpdate(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 记录第一次延迟样本
	err := s.RecordLatency(ctx, "ep:1", 100*time.Millisecond)
	require.NoError(t, err)

	// 快进 1 天
	mr.FastForward(24 * time.Hour)

	// 再记录一次，应该刷新 TTL
	err = s.RecordLatency(ctx, "ep:1", 200*time.Millisecond)
	require.NoError(t, err)

	// 验证 TTL 被刷新（应该接近 7 天）
	redisKey := s.key("latency", "ep:1")
	ttl := mr.TTL(redisKey)
	assert.Greater(t, ttl, 6*24*time.Hour, "TTL should be refreshed to ~7 days")
}

func TestRedis_Latency_MaxSamplesCleaned(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	// 记录超过最大样本数的延迟
	maxSamples := s.latencyMax
	for i := 0; i < maxSamples+100; i++ {
		err := s.RecordLatency(ctx, "ep:1", time.Duration(i+1)*time.Millisecond)
		require.NoError(t, err)
	}

	// 验证样本数被限制在 maxSamples
	redisKey := s.key("latency", "ep:1")
	count, err := s.client.ZCard(ctx, redisKey).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, count, int64(maxSamples), "sample count should not exceed max")
}

// ==================== Close ====================

func TestRedis_Close(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := NewRedisStateStore(client, nil)

	err := s.Close()
	assert.NoError(t, err)
}

func TestRedis_Close_NilCloser(t *testing.T) {
	s := NewRedisStateStore(nil, nil)
	err := s.Close()
	assert.NoError(t, err)
}

// ==================== Config ====================

func TestRedis_CustomConfig(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := &RedisStateStoreConfig{
		KeyPrefix:         "test:",
		LatencyMaxSamples: 50,
	}
	s := NewRedisStateStore(client, cfg)
	defer s.Close()
	ctx := context.Background()

	// 验证 KeyPrefix
	_, err := s.RateLimitIncr(ctx, "user:1", 100, time.Minute)
	require.NoError(t, err)

	redisKey := s.key("rl", "user:1")
	assert.True(t, mr.Exists(redisKey))
	assert.True(t, strings.HasPrefix(redisKey, "test:"))
}

// ==================== 并发测试 ====================

func TestRedis_RateLimitIncr_Concurrent(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			s.RateLimitIncr(ctx, "user:1", 1, time.Minute)
		}()
	}
	for i := 0; i < 50; i++ {
		<-done
	}

	current, err := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(50), current)
}

// ==================== EMA (指数移动平均) ====================

func TestRedis_EMA_Basic(t *testing.T) {
	s, mr := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	key := "test:ema"

	// 1. 获取不存在的 EMA，应为 0
	val, err := s.GetEMA(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, float64(0), val)

	// 2. 第一次更新，由于无旧值，EMA 应直接等于本次 actual 值
	val, err = s.UpdateEMA(ctx, key, 100, 0.1)
	require.NoError(t, err)
	assert.Equal(t, float64(100), val)

	// 3. 第二次更新：actual = 200, alpha = 0.1
	// 期望：200 * 0.1 + 100 * 0.9 = 110
	val, err = s.UpdateEMA(ctx, key, 200, 0.1)
	require.NoError(t, err)
	assert.Equal(t, float64(110), val)

	// 4. 获取当前最新的 EMA
	val, err = s.GetEMA(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, float64(110), val)

	// 5. 校验 TTL 是否设置在 7 天内
	redisKey := s.key("ema", key)
	ttl := mr.TTL(redisKey)
	assert.Greater(t, ttl, time.Duration(0))
	assert.LessOrEqual(t, ttl, 7*24*time.Hour)
}

func TestRedis_EMA_Concurrent(t *testing.T) {
	s, _ := newTestRedisStore(t)
	defer s.Close()
	ctx := context.Background()

	done := make(chan struct{})
	key := "concurrent:ema"

	// 注入初始值
	_, _ = s.UpdateEMA(ctx, key, 100, 0.1)

	for i := 0; i < 50; i++ {
		go func(val int) {
			defer func() { done <- struct{}{} }()
			_, _ = s.UpdateEMA(ctx, key, int64(val), 0.1)
			_, _ = s.GetEMA(ctx, key)
		}(i)
	}
	for i := 0; i < 50; i++ {
		<-done
	}

	finalVal, err := s.GetEMA(ctx, key)
	require.NoError(t, err)
	assert.Greater(t, finalVal, float64(0))
}
