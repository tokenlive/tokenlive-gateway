package core_test

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/usage"
)

func TestGatewayContextMergeUsage_WritesAllFields(t *testing.T) {
	gctx := &core.GatewayContext{}
	gctx.MergeUsage(usage.Tokens{InputTokens: 100, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 40})
	if gctx.InputTokens != 100 || gctx.OutputTokens != 20 || gctx.CachedTokens != 60 || gctx.CacheCreationTokens != 40 {
		t.Errorf("got (%d, %d, %d, %d)", gctx.InputTokens, gctx.OutputTokens, gctx.CachedTokens, gctx.CacheCreationTokens)
	}
}

// >0 守卫：0 值不得覆盖已有的真值（跨帧场景 message_delta 只带 output 时不应清零 input）。
func TestGatewayContextMergeUsage_ZeroDoesNotOverwrite(t *testing.T) {
	gctx := &core.GatewayContext{}
	gctx.MergeUsage(usage.Tokens{InputTokens: 100}) // message_start：只带 input
	gctx.MergeUsage(usage.Tokens{OutputTokens: 20}) // message_delta：只带 output
	if gctx.InputTokens != 100 {
		t.Errorf("InputTokens overwritten by zero frame: got %d, want 100", gctx.InputTokens)
	}
	if gctx.OutputTokens != 20 {
		t.Errorf("OutputTokens = %d, want 20", gctx.OutputTokens)
	}
}

func TestGatewayContextObserveUsage_ResetAttempt(t *testing.T) {
	gctx := &core.GatewayContext{}
	gctx.ObserveUsage(usage.Anthropic(`{"usage":{"input_tokens":100,"cache_read_input_tokens":40}}`))
	gctx.ObserveUsage(usage.Anthropic(`{"usage":{"cache_read_input_tokens":60}}`))
	if got := gctx.Usage(); got != (usage.Tokens{InputTokens: 160, CachedTokens: 60}) {
		t.Fatalf("partial usage = %+v", got)
	}
	gctx.ResetAttempt()
	gctx.ObserveUsage(usage.Anthropic(`{"usage":{"input_tokens":10,"output_tokens":2}}`))
	if got := gctx.Usage(); got != (usage.Tokens{InputTokens: 10, OutputTokens: 2}) {
		t.Fatalf("previous attempt leaked usage: %+v", got)
	}
}
