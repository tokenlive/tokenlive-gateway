package core

import (
	"context"
	"errors"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/events"

	"go.uber.org/zap"
)

// ErrNoAvailableEndpoint means no endpoint is available.
var ErrNoAvailableEndpoint = errors.New("no available endpoint")

// ErrFatalNoAvailableEndpoint is non-degradable (e.g. affinity miss).
var ErrFatalNoAvailableEndpoint = errors.New("fatal: no available endpoint")

// Invoker is the unified callable abstraction.
type Invoker interface {
	Invoke(gctx *GatewayContext) error
	Endpoint() *Endpoint
}

// InvokerDependencyResolver supplies deps for building invokers (Engine).
type InvokerDependencyResolver interface {
	Discovery() Discovery
	StateStore() StateStore
	CircuitBreakerManager() *CircuitBreakerManager
	Logger() *zap.Logger
	ResolveRouters(names []string) []Router
	ResolveLoadBalancer(name string) LoadBalancer
	EnableActiveHealthCheck() bool
	Publisher() events.Publisher
}

// InvokerBuilder builds invokers externally to avoid import cycles.
type InvokerBuilder interface {
	BuildInvoker(cfg *InvokerConfig, r InvokerDependencyResolver) (Invoker, error)
}

// StateStore is local state storage (avoids gateway→store cycles).
type StateStore interface {
	// Rate limit: speculative debit + precise settlement
	RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (remaining int64, err error)
	RateLimitRefund(ctx context.Context, key string, tokens int64) error

	// Token bucket (smooth burst): high-precision atomic consume
	RateLimitTake(ctx context.Context, key string, tokens int64, rate int64, capacity int64, window time.Duration, now time.Time) (allowed bool, remaining int64, err error)

	// Sticky Session
	StickyGet(ctx context.Context, sessionKey string) (endpointID string, err error)
	StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error

	// Latency stats (full request)
	RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error
	GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error)

	// Latency stats (TTFT; separate series)
	RecordTTFT(ctx context.Context, endpointID string, ttft time.Duration) error
	GetAvgTTFT(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error)

	// EMA (exponential moving average)
	UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error)
	GetEMA(ctx context.Context, key string) (float64, error)

	// Lifecycle
	Close() error
}
