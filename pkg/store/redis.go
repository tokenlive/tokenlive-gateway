package store

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/rate_limit_incr.lua
var rateLimitIncrScript string

//go:embed lua/rate_limit_refund.lua
var rateLimitRefundScript string

//go:embed lua/rate_limit_take.lua
var rateLimitTakeScript string

//go:embed lua/record_latency.lua
var recordLatencyScript string

//go:embed lua/update_ema.lua
var updateEMAScript string

const (
	redisLatencyMaxSamples = 1000
	redisLatencyKeyTTL     = 7 * 24 * time.Hour // drop latency keys idle for 7 days
)

// RedisStateStoreConfig configures RedisStateStore.
type RedisStateStoreConfig struct {
	// KeyPrefix is the Redis key namespace (default "aigw").
	KeyPrefix string

	// LatencyMaxSamples is the max latency samples retained (default 1000).
	LatencyMaxSamples int
}

// RedisStateStore is a Redis-backed StateStore for multi-instance production.
// Uses redis.Cmdable (works with redis.Client and redis.ClusterClient).
type RedisStateStore struct {
	client redis.Cmdable
	closer io.Closer
	prefix string

	latencyMax int

	// nowFunc returns current time; overridable in tests (default time.Now).
	nowFunc func() time.Time

	rateLimitIncrScript   *redis.Script
	rateLimitRefundScript *redis.Script
	rateLimitTakeScript   *redis.Script
	recordLatencyScript   *redis.Script
	updateEMAScript       *redis.Script
}

// NewRedisStateStore creates a RedisStateStore.
// client may be *redis.Client or *redis.ClusterClient.
func NewRedisStateStore(client redis.Cmdable, cfg *RedisStateStoreConfig) *RedisStateStore {
	if cfg == nil {
		cfg = &RedisStateStoreConfig{}
	}

	prefix := strings.TrimRight(cfg.KeyPrefix, ":")
	if prefix == "" {
		prefix = "aigw"
	}
	latencyMax := cfg.LatencyMaxSamples
	if latencyMax == 0 {
		latencyMax = redisLatencyMaxSamples
	}

	// *redis.Client and *redis.ClusterClient both implement io.Closer.
	var closer io.Closer
	if c, ok := client.(io.Closer); ok {
		closer = c
	}

	return &RedisStateStore{
		client: client,
		closer: closer,
		prefix: prefix,

		latencyMax: latencyMax,
		nowFunc:    time.Now,

		rateLimitIncrScript:   redis.NewScript(rateLimitIncrScript),
		rateLimitRefundScript: redis.NewScript(rateLimitRefundScript),
		rateLimitTakeScript:   redis.NewScript(rateLimitTakeScript),
		recordLatencyScript:   redis.NewScript(recordLatencyScript),
		updateEMAScript:       redis.NewScript(updateEMAScript),
	}
}

// key builds a prefixed Redis key: {prefix}:{part1}:{part2}:...
func (s *RedisStateStore) key(parts ...string) string {
	key := s.prefix
	for _, p := range parts {
		key += ":" + p
	}
	return key
}

// --- rate limit ---

// RateLimitIncr adds tokens to the key counter and returns usage in the window.
func (s *RedisStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	redisKey := s.key("rl", key)
	windowMs := window.Milliseconds()

	result, err := s.rateLimitIncrScript.Eval(ctx, s.client,
		[]string{redisKey},
		tokens, windowMs,
	).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis rate limit incr: %w", err)
	}
	return result, nil
}

// RateLimitRefund refunds tokens to the key counter.
func (s *RedisStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	redisKey := s.key("rl", key)

	_, err := s.rateLimitRefundScript.Eval(ctx, s.client,
		[]string{redisKey},
		tokens,
	).Int64()
	if err != nil {
		return fmt.Errorf("redis rate limit refund: %w", err)
	}
	return nil
}

// RateLimitTake tries to consume tokens from a token bucket (smooth burst limit).
func (s *RedisStateStore) RateLimitTake(ctx context.Context, key string, tokens int64, rate int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	redisKey := s.key("tb", key)
	windowMs := window.Milliseconds()
	nowMs := now.UnixMilli()

	res, err := s.rateLimitTakeScript.Eval(ctx, s.client,
		[]string{redisKey},
		tokens, float64(rate), float64(capacity), float64(windowMs), float64(nowMs),
	).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("redis rate limit take: %w", err)
	}

	if len(res) < 2 {
		return false, 0, fmt.Errorf("redis rate limit take invalid response: %v", res)
	}

	allowed := res[0] == 1
	remaining := res[1]
	return allowed, remaining, nil
}

// --- sticky session ---

// StickyGet returns the endpointID for sessionKey, or ErrKeyNotFound.
func (s *RedisStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
	redisKey := s.key("sticky", sessionKey)

	val, err := s.client.Get(ctx, redisKey).Result()
	if err == redis.Nil {
		return "", ErrKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("redis sticky get: %w", err)
	}
	return val, nil
}

// StickySet maps sessionKey to endpointID with the given TTL.
func (s *RedisStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	redisKey := s.key("sticky", sessionKey)

	err := s.client.Set(ctx, redisKey, endpointID, ttl).Err()
	if err != nil {
		return fmt.Errorf("redis sticky set: %w", err)
	}
	return nil
}

// --- latency ---

// RecordLatency records a latency sample in a Redis sorted set.
// Score is timestamp (ms); member is "<latency_ns>:<rand>".
// Atomically adds the sample, sets TTL, and trims old samples.
func (s *RedisStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	return s.recordLatencyTo(ctx, s.key("latency", endpointID), latency)
}

// GetAvgLatency returns average latency for endpointID within window, or 0 if none.
func (s *RedisStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	return s.getAvgLatencyFrom(ctx, s.key("latency", endpointID), window)
}

// RecordTTFT records a time-to-first-token sample on a key separate from total latency.
func (s *RedisStateStore) RecordTTFT(ctx context.Context, endpointID string, ttft time.Duration) error {
	return s.recordLatencyTo(ctx, s.key("latency_ttft", endpointID), ttft)
}

// GetAvgTTFT returns average TTFT for endpointID within window, or 0 if none.
func (s *RedisStateStore) GetAvgTTFT(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	return s.getAvgLatencyFrom(ctx, s.key("latency_ttft", endpointID), window)
}

// recordLatencyTo records a sample at redisKey.
// Member format "<latency_ns>:<rand>", score is ms timestamp; Lua does ZADD+PEXPIRE+ZREMRANGEBYRANK.
func (s *RedisStateStore) recordLatencyTo(ctx context.Context, redisKey string, latency time.Duration) error {
	now := float64(s.nowFunc().UnixMilli())
	member := fmt.Sprintf("%d:%d", latency.Nanoseconds(), rand.Int63())
	ttlMs := redisLatencyKeyTTL.Milliseconds()

	_, err := s.recordLatencyScript.Eval(ctx, s.client,
		[]string{redisKey},
		now, member, s.latencyMax, ttlMs,
	).Result()
	if err != nil {
		return fmt.Errorf("redis record latency: %w", err)
	}
	return nil
}

// getAvgLatencyFrom returns average latency from redisKey within window.
func (s *RedisStateStore) getAvgLatencyFrom(ctx context.Context, redisKey string, window time.Duration) (time.Duration, error) {
	now := s.nowFunc()
	since := float64(now.Add(-window).UnixMilli())
	to := float64(now.UnixMilli())

	samples, err := s.client.ZRangeByScore(ctx, redisKey, &redis.ZRangeBy{
		Min: strconv.FormatFloat(since, 'f', 0, 64),
		Max: strconv.FormatFloat(to, 'f', 0, 64),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("redis get avg latency: %w", err)
	}

	if len(samples) == 0 {
		return 0, nil
	}

	// member format "<latency_ns>:<rand>"
	var total int64
	var valid int
	for _, member := range samples {
		idx := strings.IndexByte(member, ':')
		if idx < 0 {
			continue
		}
		ns, err := strconv.ParseInt(member[:idx], 10, 64)
		if err != nil {
			continue
		}
		total += ns
		valid++
	}

	if valid == 0 {
		return 0, nil
	}
	avg := total / int64(valid)
	return time.Duration(avg), nil
}

// UpdateEMA updates the EMA estimate in Redis and returns the new value.
func (s *RedisStateStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	redisKey := s.key("ema", key)
	// 7-day TTL so idle keys are reclaimed
	ttlSec := int64(7 * 24 * 3600)
	valStr, err := s.updateEMAScript.Run(ctx, s.client, []string{redisKey}, actual, alpha, ttlSec).Text()
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// GetEMA returns the EMA estimate from Redis, or 0 if missing.
func (s *RedisStateStore) GetEMA(ctx context.Context, key string) (float64, error) {
	redisKey := s.key("ema", key)
	valStr, err := s.client.Get(ctx, redisKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// Close closes the Redis connection if the client is closable.
func (s *RedisStateStore) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}
