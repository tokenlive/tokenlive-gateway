package core

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// Invoker 统一的"可被调用"抽象
type Invoker interface {
	Invoke(gctx *GatewayContext) error
	Endpoint() *Endpoint
}

// InvokerDependencyResolver 提供构建 Invoker 所需的底层依赖，由 Engine 实现
type InvokerDependencyResolver interface {
	Discovery() Discovery
	StateStore() StateStore
	CircuitBreakerManager() *CircuitBreakerManager
	Logger() *zap.Logger
	ResolveRouters(names []string) []Router
	ResolveLoadBalancer(name string) LoadBalancer
	EnableActiveHealthCheck() bool
}

// InvokerBuilder 用于在外部构建 Invoker 具体实现，规避循环依赖
type InvokerBuilder interface {
	BuildInvoker(cfg *InvokerConfig, r InvokerDependencyResolver) (Invoker, error)
}

// StateStore 本地状态存储接口，避免 gateway -> store 的循环依赖。
type StateStore interface {
	// 限流：投机预扣 + 精确结算
	RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (remaining int64, err error)
	RateLimitRefund(ctx context.Context, key string, tokens int64) error

	// 令牌桶（平滑爆发限流）：高精度浮点数原子消费
	RateLimitTake(ctx context.Context, key string, tokens int64, rate int64, capacity int64, window time.Duration, now time.Time) (allowed bool, remaining int64, err error)

	// Sticky Session
	StickyGet(ctx context.Context, sessionKey string) (endpointID string, err error)
	StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error

	// 延迟统计
	RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error
	GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error)

	// EMA (指数移动平均) 统计与获取
	UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error)
	GetEMA(ctx context.Context, key string) (float64, error)

	// 生命周期
	Close() error
}
