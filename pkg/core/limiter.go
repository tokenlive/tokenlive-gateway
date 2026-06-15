package core

import (
	"context"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// LimitExecutor 限流执行器契约
type LimitExecutor interface {
	// Execute 执行限流判断。若限流，返回非 nil 错误
	Execute(ctx context.Context, gctx *GatewayContext, lp *policy.LimitPolicy) error
	// Refund 用于在失败/未消耗时进行预扣 Token 退还
	Refund(ctx context.Context, gctx *GatewayContext, lp *policy.LimitPolicy) error
}

// LimitExecutorFactory 限流执行器工厂
type LimitExecutorFactory struct {
	executors map[string]LimitExecutor
}

// DefaultLimitExecutorFactory 全局单例工厂
var DefaultLimitExecutorFactory = &LimitExecutorFactory{
	executors: make(map[string]LimitExecutor),
}

func (f *LimitExecutorFactory) Register(limitType string, exec LimitExecutor) {
	f.executors[limitType] = exec
}

func (f *LimitExecutorFactory) Get(limitType string) LimitExecutor {
	return f.executors[limitType]
}
