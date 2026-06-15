package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryStateStore_ImplementsInterface(t *testing.T) {
	var _ core.StateStore = (*MemoryStateStore)(nil)
	var _ core.StateStore = NewMemoryStateStore()
}

// ==================== 限流 ====================

func TestRateLimitIncr_Basic(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	current, err := s.RateLimitIncr(ctx, "user:1", 100, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100), current)

	current, err = s.RateLimitIncr(ctx, "user:1", 200, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(300), current)
}

func TestRateLimitIncr_LargeValues(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	current, err := s.RateLimitIncr(ctx, "user:1", 100000, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100000), current)

	current, err = s.RateLimitIncr(ctx, "user:1", 1, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100001), current)
}

func TestRateLimitIncr_WindowReset(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	// 使用极短窗口
	current, err := s.RateLimitIncr(ctx, "user:1", 5000, 50*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(5000), current)

	// 等待窗口重置
	time.Sleep(60 * time.Millisecond)

	current, err = s.RateLimitIncr(ctx, "user:1", 100, 50*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(100), current)
}

func TestRateLimitRefund(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	_, err := s.RateLimitIncr(ctx, "user:1", 500, time.Minute)
	require.NoError(t, err)

	err = s.RateLimitRefund(ctx, "user:1", 200)
	require.NoError(t, err)

	// 再次增加，应该能反映出退还后的已扣量
	current, err := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(300), current) // 500 - 200 = 300
}

func TestRateLimitRefund_NegativeClamp(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	// 没有消耗就退还，count 应保持为 0
	err := s.RateLimitRefund(ctx, "user:1", 100)
	require.NoError(t, err)

	current, err := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(0), current)
}

func TestRateLimitIncr_DifferentKeys(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	_, err := s.RateLimitIncr(ctx, "user:1", 500, time.Minute)
	require.NoError(t, err)

	_, err = s.RateLimitIncr(ctx, "user:2", 300, time.Minute)
	require.NoError(t, err)

	// 各自独立
	r1, _ := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	r2, _ := s.RateLimitIncr(ctx, "user:2", 0, time.Minute)
	assert.Equal(t, int64(500), r1)
	assert.Equal(t, int64(300), r2)
}

// ==================== Sticky Session ====================

func TestSticky_SetAndGet(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	err := s.StickySet(ctx, "session:abc", "ep:1", 5*time.Minute)
	require.NoError(t, err)

	endpointID, err := s.StickyGet(ctx, "session:abc")
	require.NoError(t, err)
	assert.Equal(t, "ep:1", endpointID)
}

func TestSticky_NotFound(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	_, err := s.StickyGet(ctx, "session:nonexistent")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestSticky_TTLExpiry(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	err := s.StickySet(ctx, "session:abc", "ep:1", 50*time.Millisecond)
	require.NoError(t, err)

	// 立即获取，应该存在
	endpointID, err := s.StickyGet(ctx, "session:abc")
	require.NoError(t, err)
	assert.Equal(t, "ep:1", endpointID)

	// 等待过期
	time.Sleep(60 * time.Millisecond)

	_, err = s.StickyGet(ctx, "session:abc")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestSticky_Overwrite(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	s.StickySet(ctx, "session:abc", "ep:1", 5*time.Minute)
	s.StickySet(ctx, "session:abc", "ep:2", 5*time.Minute)

	endpointID, err := s.StickyGet(ctx, "session:abc")
	require.NoError(t, err)
	assert.Equal(t, "ep:2", endpointID)
}

// ==================== 延迟统计 ====================

func TestLatency_RecordAndGetAvg(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	s.RecordLatency(ctx, "ep:1", 100*time.Millisecond)
	s.RecordLatency(ctx, "ep:1", 200*time.Millisecond)
	s.RecordLatency(ctx, "ep:1", 300*time.Millisecond)

	avg, err := s.GetAvgLatency(ctx, "ep:1", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 200*time.Millisecond, avg) // (100+200+300)/3
}

func TestLatency_WindowFiltering(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	// 直接注入旧样本
	r := s.getOrCreateLatencyRing("ep:1")
	r.add(1*time.Second, time.Now().Add(-2*time.Hour))
	r.add(100*time.Millisecond, time.Now())

	// 1 小时窗口内只有 100ms
	avg, err := s.GetAvgLatency(ctx, "ep:1", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 100*time.Millisecond, avg)
}

func TestLatency_Empty(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	avg, err := s.GetAvgLatency(ctx, "ep:nonexistent", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), avg)
}

func TestLatency_RingBufferOverflow(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	// 写入超过容量的样本
	for i := 0; i < latencyRingCapacity+100; i++ {
		s.RecordLatency(ctx, "ep:1", time.Duration(i)*time.Millisecond)
	}

	// 环形缓冲区应只保留最新的 latencyRingCapacity 个样本
	r := s.latencies["ep:1"]
	r.mu.Lock()
	assert.Equal(t, latencyRingCapacity, r.count)
	r.mu.Unlock()

	// 平均值应该在合理范围内
	avg, err := s.GetAvgLatency(ctx, "ep:1", time.Hour)
	require.NoError(t, err)
	// 最新的 1000 个样本：100ms ~ 1099ms，平均 ~600ms
	assert.Greater(t, avg, time.Duration(0))
}

func TestLatency_DifferentEndpoints(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	s.RecordLatency(ctx, "ep:1", 100*time.Millisecond)
	s.RecordLatency(ctx, "ep:2", 500*time.Millisecond)

	avg1, _ := s.GetAvgLatency(ctx, "ep:1", time.Hour)
	avg2, _ := s.GetAvgLatency(ctx, "ep:2", time.Hour)
	assert.Equal(t, 100*time.Millisecond, avg1)
	assert.Equal(t, 500*time.Millisecond, avg2)
}

// ==================== Close ====================

func TestClose(t *testing.T) {
	s := NewMemoryStateStore()
	err := s.Close()
	assert.NoError(t, err)
}

// ==================== 并发压力测试 ====================

func TestRateLimitIncr_Concurrent(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.RateLimitIncr(ctx, "user:1", 1, time.Minute)
		}()
	}
	wg.Wait()

	current, err := s.RateLimitIncr(ctx, "user:1", 0, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(100), current)
}

func TestSticky_Concurrent(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("session:%d", i%10)
			s.StickySet(ctx, key, fmt.Sprintf("ep:%d", i), time.Minute)
			s.StickyGet(ctx, key)
		}(i)
	}
	wg.Wait()
}

// ==================== EMA (指数移动平均) ====================

func TestEMA_Basic(t *testing.T) {
	s := NewMemoryStateStore()
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
}

func TestEMA_Concurrent(t *testing.T) {
	s := NewMemoryStateStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	key := "concurrent:ema"

	// 注入初始值
	_, _ = s.UpdateEMA(ctx, key, 100, 0.1)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			_, _ = s.UpdateEMA(ctx, key, int64(val), 0.1)
			_, _ = s.GetEMA(ctx, key)
		}(i)
	}
	wg.Wait()

	finalVal, err := s.GetEMA(ctx, key)
	require.NoError(t, err)
	assert.Greater(t, finalVal, float64(0))
}
