// Package store 提供跨请求状态抽象，用于限流、熔断、会话粘滞和延迟统计。
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

// redisStateStoreDefaults 定义 RedisStateStore 的默认参数。
const (
	redisLatencyMaxSamples = 1000
	redisLatencyKeyTTL     = 7 * 24 * time.Hour // 延迟统计 key 过期时间（7 天未更新则清理）
)

// RedisStateStoreConfig RedisStateStore 的配置选项。
type RedisStateStoreConfig struct {
	// KeyPrefix Redis 键前缀，用于命名空间隔离（默认 "aigw"）
	KeyPrefix string

	// LatencyMaxSamples 延迟统计最大样本数（默认 1000）
	LatencyMaxSamples int
}

// RedisStateStore 基于 Redis 的 StateStore 实现，适用于多实例生产部署。
// 使用 redis.Cmdable 接口，兼容 redis.Client 和 redis.ClusterClient。
type RedisStateStore struct {
	client redis.Cmdable
	closer io.Closer
	prefix string

	latencyMax int

	// nowFunc 返回当前时间，测试时可替换。默认为 time.Now。
	nowFunc func() time.Time

	// Lua 脚本
	rateLimitIncrScript   *redis.Script
	rateLimitRefundScript *redis.Script
	rateLimitTakeScript   *redis.Script
	recordLatencyScript   *redis.Script
	updateEMAScript       *redis.Script
}

// NewRedisStateStore 创建一个新的 RedisStateStore。
// client 可以是 *redis.Client 或 *redis.ClusterClient。
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

	// 尝试将 client 作为 io.Closer（*redis.Client 和 *redis.ClusterClient 都实现了）
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

// key 构建带前缀的 Redis 键，格式为 {prefix}:{part1}:{part2}:...。
func (s *RedisStateStore) key(parts ...string) string {
	key := s.prefix
	for _, p := range parts {
		key += ":" + p
	}
	return key
}

// --- 限流 ---

// RateLimitIncr 将 key 的计数增加 tokens，并返回窗口内当前已消耗量。
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

// RateLimitRefund 退还 tokens 到 key 的计数中。
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

// RateLimitTake 尝试从令牌桶中消费令牌（平滑爆发限流）。
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

// --- Sticky Session ---

// StickyGet 获取 sessionKey 关联的 endpointID。若不存在则返回 ErrKeyNotFound。
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

// StickySet 设置 sessionKey 到 endpointID 的映射，ttl 为过期时间。
func (s *RedisStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	redisKey := s.key("sticky", sessionKey)

	err := s.client.Set(ctx, redisKey, endpointID, ttl).Err()
	if err != nil {
		return fmt.Errorf("redis sticky set: %w", err)
	}
	return nil
}

// --- 延迟统计 ---

// RecordLatency 记录一次延迟采样到 Redis Sorted Set。
// 使用时间戳（毫秒）作为 score，latency（纳秒）编码在 member 中："<latency_ns>:<rand>"。
// 原子性地完成：添加样本、设置 TTL、清理旧样本。
func (s *RedisStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	redisKey := s.key("latency", endpointID)
	now := float64(s.nowFunc().UnixMilli())
	member := fmt.Sprintf("%d:%d", latency.Nanoseconds(), rand.Int63())
	ttlMs := redisLatencyKeyTTL.Milliseconds()

	// 使用 Lua 脚本原子性地完成：ZADD + PEXPIRE + ZREMRANGEBYRANK
	_, err := s.recordLatencyScript.Eval(ctx, s.client,
		[]string{redisKey},
		now, member, s.latencyMax, ttlMs,
	).Result()
	if err != nil {
		return fmt.Errorf("redis record latency: %w", err)
	}

	return nil
}

// GetAvgLatency 返回 endpointID 在 window 时间窗口内的平均延迟。
// 若没有样本则返回 0。
func (s *RedisStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	redisKey := s.key("latency", endpointID)

	now := s.nowFunc()
	since := float64(now.Add(-window).UnixMilli())
	to := float64(now.UnixMilli())

	// ZRANGEBYSCORE 获取窗口内的样本
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

	// member 格式 "<latency_ns>:<rand>"，解析 latency_ns
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

// UpdateEMA 滚动更新 Redis 中的 EMA 估算值并返回最新值。
func (s *RedisStateStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	redisKey := s.key("ema", key)
	// 默认缓存 7 天以防长时间没有请求占用空间
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

// GetEMA 获取 Redis 中的 EMA 估算值。
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

// Close 关闭 Redis 连接。
func (s *RedisStateStore) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}
