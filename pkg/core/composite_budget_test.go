package core

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestCompositeBudgetLimitsConcurrentCalls(t *testing.T) {
	ctx := WithCompositeAttemptBudget(t.Context(), 6)
	var started atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ConsumeCompositeAttempt(ctx) == nil {
				started.Add(1)
			}
		}()
	}
	wg.Wait()
	if started.Load() != 6 {
		t.Fatalf("started %d calls with budget 6", started.Load())
	}
	if err := ConsumeCompositeAttempt(t.Context()); err != nil {
		t.Fatalf("ordinary request affected: %v", err)
	}
}
