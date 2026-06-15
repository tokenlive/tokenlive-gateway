package inbound

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/tagging"
)

// TaggingFilter 打标过滤器（InboundFilter）
// 读取 Policy 中的 TaggingPolicies，按条件匹配后将标签注入 GatewayContext.Tags
// Order=12，位于 auth(10) 之后、rate_limit(20) 之前
type TaggingFilter struct {
	engine *tagging.TaggingEngine
}

// NewTaggingFilter 创建 TaggingFilter
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
