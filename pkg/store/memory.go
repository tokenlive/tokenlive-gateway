package store

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrKeyNotFound 键不存在
	ErrKeyNotFound = errors.New("key not found")

	latencyRingCapacity = 1000 // 延迟环形缓冲区容量
)

// ---------- 限流 ----------

type rateLimitEntry struct {
	mu      sync.Mutex
	count   int64
	window  time.Duration
	resetAt time.Time
}

func (e *rateLimitEntry) incr(tokens int64, now time.Time) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if now.After(e.resetAt) || e.resetAt.IsZero() {
		e.count = 0
		e.resetAt = now.Add(e.window)
	}

	e.count += tokens
	return e.count
}

func (e *rateLimitEntry) refund(tokens int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.count -= tokens
	if e.count < 0 {
		e.count = 0
	}
}

type tokenBucketEntry struct {
	mu          sync.Mutex
	tokens      float64
	lastUpdated time.Time
}

func (e *tokenBucketEntry) take(requested int64, rate int64, capacity int64, window time.Duration, now time.Time) (bool, int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	limitCap := float64(capacity)
	if e.lastUpdated.IsZero() {
		e.tokens = limitCap
		e.lastUpdated = now
	} else {
		delta := now.Sub(e.lastUpdated)
		if delta > 0 {
			genTokens := (float64(delta) / float64(window)) * float64(rate)
			e.tokens = e.tokens + genTokens
			if e.tokens > limitCap {
				e.tokens = limitCap
			}
			e.lastUpdated = now
		}
	}

	reqFloat := float64(requested)
	if e.tokens >= reqFloat {
		e.tokens -= reqFloat
		if e.tokens > limitCap {
			e.tokens = limitCap
		}
		return true, int64(e.tokens)
	}
	return false, int64(e.tokens)
}

// ---------- Sticky Session ----------

type stickyEntry struct {
	endpointID string
	expiresAt  time.Time
}

// ---------- 延迟 ----------

type latencySample struct {
	latency   time.Duration
	timestamp time.Time
}

type latencyRing struct {
	mu       sync.Mutex
	samples  []latencySample
	writePos int
	count    int
	capacity int
}

func newLatencyRing(capacity int) *latencyRing {
	return &latencyRing{
		samples:  make([]latencySample, capacity),
		capacity: capacity,
	}
}

func (r *latencyRing) add(latency time.Duration, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples[r.writePos] = latencySample{latency: latency, timestamp: now}
	r.writePos = (r.writePos + 1) % r.capacity
	if r.count < r.capacity {
		r.count++
	}
}

func (r *latencyRing) avg(window time.Duration, now time.Time) (time.Duration, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 {
		return 0, false
	}

	since := now.Add(-window)
	var total time.Duration
	var n int

	// 从最旧到最新遍历
	start := 0
	if r.count == r.capacity {
		start = r.writePos // writePos 指向最旧的样本
	}
	for i := 0; i < r.count; i++ {
		idx := (start + i) % r.capacity
		s := r.samples[idx]
		if window > 0 && s.timestamp.Before(since) {
			continue
		}
		total += s.latency
		n++
	}

	if n == 0 {
		return 0, false
	}
	return total / time.Duration(n), true
}

type emaEntry struct {
	mu  sync.Mutex
	val float64
}

// ---------- MemoryStateStore ----------

// MemoryStateStore 基于内存的 StateStore 实现，适用于单实例部署和测试。
type MemoryStateStore struct {
	mu           sync.RWMutex
	rateLimits   map[string]*rateLimitEntry
	tokenBuckets map[string]*tokenBucketEntry
	sticky       map[string]*stickyEntry
	latencies    map[string]*latencyRing
	emas         map[string]*emaEntry
}

// NewMemoryStateStore 创建一个新的 MemoryStateStore。
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		rateLimits:   make(map[string]*rateLimitEntry),
		tokenBuckets: make(map[string]*tokenBucketEntry),
		sticky:       make(map[string]*stickyEntry),
		latencies:    make(map[string]*latencyRing),
		emas:         make(map[string]*emaEntry),
	}
}

// --- 限流 ---

func (s *MemoryStateStore) getOrCreateRateLimitEntry(key string, window time.Duration) *rateLimitEntry {
	s.mu.RLock()
	if e, ok := s.rateLimits[key]; ok {
		s.mu.RUnlock()
		return e
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.rateLimits[key]; ok {
		return e
	}
	e := &rateLimitEntry{window: window}
	s.rateLimits[key] = e
	return e
}

// RateLimitIncr 将 key 的计数增加 tokens，并返回窗口内剩余配额。
// window 用于设置窗口大小；若 key 已存在且 window 不同，会在下一个重置周期生效。
func (s *MemoryStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	e := s.getOrCreateRateLimitEntry(key, window)
	remaining := e.incr(tokens, time.Now())
	return remaining, nil
}

// RateLimitRefund 退还 tokens 到 key 的计数中。
func (s *MemoryStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	e := s.getOrCreateRateLimitEntry(key, 0)
	e.refund(tokens)
	return nil
}

func (s *MemoryStateStore) getOrCreateTokenBucketEntry(key string) *tokenBucketEntry {
	s.mu.RLock()
	if e, ok := s.tokenBuckets[key]; ok {
		s.mu.RUnlock()
		return e
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.tokenBuckets[key]; ok {
		return e
	}
	e := &tokenBucketEntry{}
	s.tokenBuckets[key] = e
	return e
}

// RateLimitTake 尝试从令牌桶中消费令牌（平滑爆发限流）。
func (s *MemoryStateStore) RateLimitTake(ctx context.Context, key string, tokens int64, rate int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	e := s.getOrCreateTokenBucketEntry(key)
	allowed, remaining := e.take(tokens, rate, capacity, window, now)
	return allowed, remaining, nil
}

// --- Sticky Session ---

// StickyGet 获取 sessionKey 关联的 endpointID。若已过期或不存在则返回 ErrKeyNotFound。
func (s *MemoryStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
	s.mu.RLock()
	e, ok := s.sticky[sessionKey]
	s.mu.RUnlock()

	if !ok {
		return "", ErrKeyNotFound
	}
	if time.Now().After(e.expiresAt) {
		// 惰性清理：删除前验证 entry 指针未被替换，避免误删新值
		s.mu.Lock()
		if current, exists := s.sticky[sessionKey]; exists && current == e {
			delete(s.sticky, sessionKey)
		}
		s.mu.Unlock()
		return "", ErrKeyNotFound
	}
	return e.endpointID, nil
}

// StickySet 设置 sessionKey 到 endpointID 的映射，ttl 为过期时间。
func (s *MemoryStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sticky[sessionKey] = &stickyEntry{
		endpointID: endpointID,
		expiresAt:  time.Now().Add(ttl),
	}
	return nil
}

// --- 延迟统计 ---

func (s *MemoryStateStore) getOrCreateLatencyRing(endpointID string) *latencyRing {
	s.mu.RLock()
	if r, ok := s.latencies[endpointID]; ok {
		s.mu.RUnlock()
		return r
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if r, ok := s.latencies[endpointID]; ok {
		return r
	}
	r := newLatencyRing(latencyRingCapacity)
	s.latencies[endpointID] = r
	return r
}

// RecordLatency 记录一次延迟采样。
func (s *MemoryStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	r := s.getOrCreateLatencyRing(endpointID)
	r.add(latency, time.Now())
	return nil
}

// GetAvgLatency 返回 endpointID 在 window 时间窗口内的平均延迟。
// 若没有样本则返回 0 和 false。
func (s *MemoryStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	r := s.getOrCreateLatencyRing(endpointID)
	avg, ok := r.avg(window, time.Now())
	if !ok {
		return 0, nil
	}
	return avg, nil
}

func (s *MemoryStateStore) getOrCreateEMAEntry(key string) *emaEntry {
	s.mu.RLock()
	if e, ok := s.emas[key]; ok {
		s.mu.RUnlock()
		return e
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.emas[key]; ok {
		return e
	}
	e := &emaEntry{}
	s.emas[key] = e
	return e
}

// UpdateEMA 滚动更新指定 key 的 EMA 值并返回最新值。
func (s *MemoryStateStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	e := s.getOrCreateEMAEntry(key)
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.val == 0 {
		e.val = float64(actual)
	} else {
		e.val = float64(actual)*alpha + e.val*(1.0-alpha)
	}
	return e.val, nil
}

// GetEMA 获取指定 key 的 EMA 缓存值。
func (s *MemoryStateStore) GetEMA(ctx context.Context, key string) (float64, error) {
	s.mu.RLock()
	e, ok := s.emas[key]
	s.mu.RUnlock()

	if !ok {
		return 0, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.val, nil
}

// Close 释放资源。MemoryStateStore 无需特殊清理。
func (s *MemoryStateStore) Close() error {
	return nil
}
