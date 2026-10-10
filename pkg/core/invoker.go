package core

import (
	"context"
	"errors"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

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

// FallbackInvoker 持有完整请求级模型降级，原始调用器仅在入口选择一次。
type FallbackInvoker struct{ pipeline *Pipeline }

func NewFallbackInvoker(pipeline *Pipeline) *FallbackInvoker {
	return &FallbackInvoker{pipeline: pipeline}
}

func (*FallbackInvoker) Endpoint() *Endpoint { return nil }

func (f *FallbackInvoker) Invoke(g *GatewayContext) error {
	invoke := f.pipeline.SelectInvoker(g)
	fallback := getFallbackPolicy(g)
	if fallback == nil || len(fallback.Targets) == 0 {
		return invoke.Invoke(g)
	}
	models := append([]string{g.Model}, fallback.Targets...)
	var err error
	for _, model := range models {
		g.Model = model
		g.FallbackChain = append(g.FallbackChain, model)
		err = invoke.Invoke(g)
		if !CanFallbackModel(g, err) {
			break
		}
	}
	return err
}

func getFallbackPolicy(g *GatewayContext) *policy.FallbackPolicy {
	if g != nil && g.Policy != nil && g.Policy.InvocationPolicy != nil {
		return g.Policy.InvocationPolicy.FallbackPolicy
	}
	return nil
}

// CanFallbackModel 仅允许无端点降级，保留亲和性与首字节保护。
func CanFallbackModel(g *GatewayContext, err error) bool {
	if err == nil || errors.Is(err, ErrFatalNoAvailableEndpoint) {
		return false
	}
	if g != nil && (g.FatalErr != nil || g.TTFT > 0 || isAffinityNoDegrade(g)) {
		return false
	}
	return errors.Is(err, ErrNoAvailableEndpoint)
}

// StateStore is local state storage (avoids gateway→store cycles).
type StateStore interface {
	// Rate limit: speculative debit + precise settlement
	RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (remaining int64, err error)
	RateLimitRefund(ctx context.Context, key string, tokens int64) error

	// Token bucket (smooth burst): high-precision atomic consume
	RateLimitTake(ctx context.Context, key string, tokens int64, rate int64, capacity int64, window time.Duration, now time.Time) (allowed bool, remaining int64, err error)
	// RateLimitAdjust records usage that already happened. Positive tokens may
	// create debt; negative tokens refund without exceeding capacity.
	RateLimitAdjust(ctx context.Context, key string, tokens int64, rate int64, capacity int64, window time.Duration, now time.Time) (remaining int64, err error)

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

func isAffinityNoDegrade(gctx *GatewayContext) bool {
	if gctx == nil || gctx.Policy == nil || gctx.Policy.LoadBalancePolicy == nil {
		return false
	}
	lbPolicy := gctx.Policy.LoadBalancePolicy
	if lbPolicy.Type == "endpoint_affinity" {
		if lbPolicy.Params != nil {
			var allowDegrade bool
			if v, ok := lbPolicy.Params["allow_degrade"]; ok {
				switch x := v.(type) {
				case bool:
					allowDegrade = x
				case string:
					allowDegrade = (x == "true")
				case float64:
					allowDegrade = (x != 0)
				case int:
					allowDegrade = (x != 0)
				}
			} else if v, ok := lbPolicy.Params["allowDegrade"]; ok {
				switch x := v.(type) {
				case bool:
					allowDegrade = x
				case string:
					allowDegrade = (x == "true")
				case float64:
					allowDegrade = (x != 0)
				case int:
					allowDegrade = (x != 0)
				}
			}
			return !allowDegrade
		}
	}
	return false
}
