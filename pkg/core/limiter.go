package core

import (
	"context"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// LimitExecutor is the rate-limit executor contract.
type LimitExecutor interface {
	// Execute enforces the limit; non-nil error if limited.
	Execute(ctx context.Context, gctx *GatewayContext, lp *policy.LimitPolicy) error
	// Refund returns pre-debited tokens on failure/unused.
	Refund(ctx context.Context, gctx *GatewayContext, lp *policy.LimitPolicy) error
}

// LimitExecutorFactory builds LimitExecutors.
type LimitExecutorFactory struct {
	executors map[string]LimitExecutor
}

// DefaultLimitExecutorFactory is the global factory singleton.
var DefaultLimitExecutorFactory = &LimitExecutorFactory{
	executors: make(map[string]LimitExecutor),
}

func (f *LimitExecutorFactory) Register(limitType string, exec LimitExecutor) {
	f.executors[limitType] = exec
}

func (f *LimitExecutorFactory) Get(limitType string) LimitExecutor {
	return f.executors[limitType]
}
