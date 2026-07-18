package store

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrKeyNotFound is returned when a key does not exist.
	ErrKeyNotFound = errors.New("key not found")

	latencyRingCapacity = 1000
)

// ---------- rate limit ----------

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

// ---------- sticky session ----------

type stickyEntry struct {
	endpointID string
	expiresAt  time.Time
}

// ---------- latency ----------

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

	// oldest → newest
	start := 0
	if r.count == r.capacity {
		start = r.writePos // writePos is the oldest slot when full
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

// MemoryStateStore is an in-memory StateStore for single-instance and tests.
type MemoryStateStore struct {
	mu           sync.RWMutex
	rateLimits   map[string]*rateLimitEntry
	tokenBuckets map[string]*tokenBucketEntry
	sticky       map[string]*stickyEntry
	latencies    map[string]*latencyRing
	ttfts        map[string]*latencyRing // TTFT series, separate from total latency
	emas         map[string]*emaEntry
}

// NewMemoryStateStore creates a MemoryStateStore.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		rateLimits:   make(map[string]*rateLimitEntry),
		tokenBuckets: make(map[string]*tokenBucketEntry),
		sticky:       make(map[string]*stickyEntry),
		latencies:    make(map[string]*latencyRing),
		ttfts:        make(map[string]*latencyRing),
		emas:         make(map[string]*emaEntry),
	}
}

// --- rate limit ---

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

// RateLimitIncr adds tokens to the key counter and returns usage in the window.
// window sets the window size; if the key exists with a different window, it takes effect on the next reset.
func (s *MemoryStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	e := s.getOrCreateRateLimitEntry(key, window)
	remaining := e.incr(tokens, time.Now())
	return remaining, nil
}

// RateLimitRefund refunds tokens to the key counter.
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

// RateLimitTake tries to consume tokens from a token bucket (smooth burst limit).
func (s *MemoryStateStore) RateLimitTake(ctx context.Context, key string, tokens int64, rate int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	e := s.getOrCreateTokenBucketEntry(key)
	allowed, remaining := e.take(tokens, rate, capacity, window, now)
	return allowed, remaining, nil
}

// --- sticky session ---

// StickyGet returns the endpointID for sessionKey, or ErrKeyNotFound if expired/missing.
func (s *MemoryStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
	s.mu.RLock()
	e, ok := s.sticky[sessionKey]
	s.mu.RUnlock()

	if !ok {
		return "", ErrKeyNotFound
	}
	if time.Now().After(e.expiresAt) {
		// lazy delete only if entry pointer is still the same (avoid clobbering a refresh)
		s.mu.Lock()
		if current, exists := s.sticky[sessionKey]; exists && current == e {
			delete(s.sticky, sessionKey)
		}
		s.mu.Unlock()
		return "", ErrKeyNotFound
	}
	return e.endpointID, nil
}

// StickySet maps sessionKey to endpointID with the given TTL.
func (s *MemoryStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sticky[sessionKey] = &stickyEntry{
		endpointID: endpointID,
		expiresAt:  time.Now().Add(ttl),
	}
	return nil
}

// --- latency ---

// getOrCreateLatencyRing gets or creates a ring for endpointID in m.
// m is latencies (total) or ttfts (TTFT) so both series share one ring implementation.
func (s *MemoryStateStore) getOrCreateLatencyRing(m map[string]*latencyRing, endpointID string) *latencyRing {
	s.mu.RLock()
	if r, ok := m[endpointID]; ok {
		s.mu.RUnlock()
		return r
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if r, ok := m[endpointID]; ok {
		return r
	}
	r := newLatencyRing(latencyRingCapacity)
	m[endpointID] = r
	return r
}

// RecordLatency records a latency sample.
func (s *MemoryStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	r := s.getOrCreateLatencyRing(s.latencies, endpointID)
	r.add(latency, time.Now())
	return nil
}

// GetAvgLatency returns average latency for endpointID within window, or 0 if none.
func (s *MemoryStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	r := s.getOrCreateLatencyRing(s.latencies, endpointID)
	avg, ok := r.avg(window, time.Now())
	if !ok {
		return 0, nil
	}
	return avg, nil
}

// RecordTTFT records a TTFT sample on a series separate from total latency.
func (s *MemoryStateStore) RecordTTFT(ctx context.Context, endpointID string, ttft time.Duration) error {
	r := s.getOrCreateLatencyRing(s.ttfts, endpointID)
	r.add(ttft, time.Now())
	return nil
}

// GetAvgTTFT returns average TTFT for endpointID within window, or 0 if none.
func (s *MemoryStateStore) GetAvgTTFT(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	r := s.getOrCreateLatencyRing(s.ttfts, endpointID)
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

// UpdateEMA updates the EMA for key and returns the new value.
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

// GetEMA returns the cached EMA for key, or 0 if missing.
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

// Close is a no-op for MemoryStateStore.
func (s *MemoryStateStore) Close() error {
	return nil
}
