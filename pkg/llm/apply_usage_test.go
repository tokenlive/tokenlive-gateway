package llm

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestApplyUsage_WritesAllFields(t *testing.T) {
	gctx := &core.GatewayContext{}
	ApplyUsage(gctx, 100, 20, 60, 40)
	if gctx.InputTokens != 100 || gctx.OutputTokens != 20 || gctx.CachedTokens != 60 || gctx.CacheCreationTokens != 40 {
		t.Errorf("got (%d, %d, %d, %d)", gctx.InputTokens, gctx.OutputTokens, gctx.CachedTokens, gctx.CacheCreationTokens)
	}
}

// >0 守卫：0 值不得覆盖已有的真值（跨帧场景 message_delta 只带 output 时不应清零 input）。
func TestApplyUsage_ZeroDoesNotOverwrite(t *testing.T) {
	gctx := &core.GatewayContext{}
	ApplyUsage(gctx, 100, 0, 0, 0) // message_start：只带 input
	ApplyUsage(gctx, 0, 20, 0, 0)  // message_delta：只带 output
	if gctx.InputTokens != 100 {
		t.Errorf("InputTokens overwritten by zero frame: got %d, want 100", gctx.InputTokens)
	}
	if gctx.OutputTokens != 20 {
		t.Errorf("OutputTokens = %d, want 20", gctx.OutputTokens)
	}
}
