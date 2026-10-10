package outbound

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
)

func TestStateStoreDeclaresBurstReconciliation(t *testing.T) {
	var contract core.StateStore = store.NewMemoryStateStore()
	now := time.Now()
	_, err := contract.RateLimitAdjust(context.Background(), "quota", 120, 100, 100, time.Minute, now)
	require.NoError(t, err)
	allowed, remaining, err := contract.RateLimitTake(context.Background(), "quota", 1, 100, 100, time.Minute, now)
	require.NoError(t, err)
	require.False(t, allowed)
	require.Equal(t, int64(-20), remaining)
}
