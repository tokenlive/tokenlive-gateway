package tagging

import (
	"context"
	"sort"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// TaggingEngine 染色打标规则引擎
// 按 Order 顺序执行 TaggingPolicy，命中条件时将标签注入 GatewayContext.Tags
type TaggingEngine struct {
	interpolator *Interpolator
}

// NewTaggingEngine 创建 TaggingEngine
func NewTaggingEngine() *TaggingEngine {
	return &TaggingEngine{interpolator: &Interpolator{}}
}

// Process 按 Order 顺序执行所有染色打标规则，命中条件的 Action 写入 gctx.Tags
func (e *TaggingEngine) Process(ctx context.Context, gctx *core.GatewayContext, taggingPolicies []*policy.TaggingPolicy) {
	if len(taggingPolicies) == 0 {
		return
	}
	if gctx.Tags == nil {
		gctx.Tags = make(map[string]string)
	}

	// 按 Order 排序（稳定排序，保持同 Order 的原始顺序）
	rules := make([]*policy.TaggingPolicy, len(taggingPolicies))
	copy(rules, taggingPolicies)
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].Order < rules[j].Order
	})

	for _, rule := range rules {
		if e.matchConditions(ctx, gctx, rule) {
			for _, action := range rule.Actions {
				value := e.interpolator.Interpolate(gctx, action.Value)
				gctx.Tags[action.Key] = value
			}
		}
	}
}

// matchConditions 判断 TaggingPolicy 的所有条件是否满足
// Relation 默认为 AND（全部满足），可设为 OR（任一满足）
func (e *TaggingEngine) matchConditions(ctx context.Context, gctx *core.GatewayContext, rule *policy.TaggingPolicy) bool {
	var validConds []*matcher.TagCondition
	for _, cond := range rule.Conditions {
		if cond.Type != "" {
			validConds = append(validConds, cond)
		}
	}
	if len(validConds) == 0 {
		return true // 空条件默认命中
	}

	isOr := strings.EqualFold(rule.Relation, "OR")

	for _, cond := range validConds {
		m := matcher.DefaultTagMatcherFactory.Get(strings.ToLower(cond.Type))
		matched := m != nil && m.Match(ctx, cond, gctx)

		if isOr && matched {
			return true // OR 模式：任一命中即返回
		}
		if !isOr && !matched {
			return false // AND 模式：任一未命中即返回
		}
	}

	return !isOr // AND 全部命中 → true；OR 全部未命中 → false
}
