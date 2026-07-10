package repository

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// ========== Redis ==========

// RedisConfig 统一 Redis 配置，从 viper 解析，避免各处重复读取。
type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
}

// LoadRedisConfig 从 viper 加载 Redis 配置，仅在 data.redis 存在时返回非 nil。
func LoadRedisConfig(conf *viper.Viper) *RedisConfig {
	if !conf.IsSet("data.redis") {
		return nil
	}
	cfg := &RedisConfig{
		Addr:         conf.GetString("data.redis.addr"),
		Password:     conf.GetString("data.redis.password"),
		DB:           conf.GetInt("data.redis.db"),
		ReadTimeout:  conf.GetDuration("data.redis.read_timeout"),
		WriteTimeout: conf.GetDuration("data.redis.write_timeout"),
		PoolSize:     conf.GetInt("data.redis.pool_size"),
		MinIdleConns: conf.GetInt("data.redis.min_idle_conns"),
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 10 * runtime.GOMAXPROCS(0)
	}
	return cfg
}

// ToOptions 将 RedisConfig 转换为 go-redis Options。
func (c *RedisConfig) ToOptions() *redis.Options {
	opts := &redis.Options{
		Addr:         c.Addr,
		Password:     c.Password,
		DB:           c.DB,
		PoolSize:     c.PoolSize,
		MinIdleConns: c.MinIdleConns,
	}
	if c.ReadTimeout > 0 {
		opts.ReadTimeout = c.ReadTimeout
	}
	if c.WriteTimeout > 0 {
		opts.WriteTimeout = c.WriteTimeout
	}
	return opts
}

// NewRedis 创建共享的 *redis.Client 单例，带健康检查和 cleanup 函数。
// cfg 为 nil 时返回 (nil, func() {}, nil)，调用方可据此做 graceful degradation。
func NewRedis(cfg *RedisConfig) (*redis.Client, func(), error) {
	if cfg == nil {
		return nil, func() {}, nil
	}

	rdb := redis.NewClient(cfg.ToOptions())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("Warning: redis ping failed: %v. Degrading to memory mode.\n", err)
		return nil, func() {}, nil
	}

	cleanup := func() {
		_ = rdb.Close()
	}

	return rdb, cleanup, nil
}

