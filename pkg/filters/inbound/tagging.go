package inbound

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/tagging"
)

// TaggingFilter applies tagging policies from Policy and injects tags into GatewayContext.Tags.
// Order=12, after auth(10), before rate_limit(20).
type TaggingFilter struct {
	engine *tagging.TaggingEngine
}

// NewTaggingFilter creates a TaggingFilter.
func NewTaggingFilter() *TaggingFilter {
	return &TaggingFilter{engine: tagging.NewTaggingEngine()}
}

func (f *TaggingFilter) Name() string { return "tagging" }
func (f *TaggingFilter) Order() int   { return 12 }

func (f *TaggingFilter) OnRequest(gctx *core.GatewayContext) error {
	if gctx.Policy == nil || len(gctx.Policy.TaggingPolicies) == 0 {
		return nil
	}
	f.engine.Process(gctx.Ctx, gctx, gctx.Policy.TaggingPolicies)
	return nil
}
