package core

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrCompositeAttemptBudget = errors.New("composite upstream attempt budget exhausted")

type compositeBudgetKey struct{}

// WithCompositeAttemptBudget shares an attempt counter through retries, model
// changes and hedging contexts. Ordinary requests have no counter and are unchanged.
func WithCompositeAttemptBudget(ctx context.Context, attempts int64) context.Context {
	remaining := &atomic.Int64{}
	remaining.Store(attempts)
	return context.WithValue(ctx, compositeBudgetKey{}, remaining)
}

func ConsumeCompositeAttempt(ctx context.Context) error {
	remaining, ok := ctx.Value(compositeBudgetKey{}).(*atomic.Int64)
	if !ok {
		return nil
	}
	for {
		old := remaining.Load()
		if old <= 0 {
			return ErrCompositeAttemptBudget
		}
		if remaining.CompareAndSwap(old, old-1) {
			return nil
		}
	}
}
