package matcher

import (
	"context"
)

// SystemTagMatcher 匹配内置的系统上下文参数 (model, user)
type SystemTagMatcher struct{}

func (m *SystemTagMatcher) Match(ctx context.Context, cond *TagCondition, reqCtx RequestContext) bool {
	if reqCtx == nil {
		return false
	}
	val := reqCtx.GetSystemValue(cond.Key)
	if val == "" {
		return cond.MatchValues(nil)
	}
	return cond.MatchValues([]string{val})
}

func init() {
	DefaultTagMatcherFactory.Register("system", &SystemTagMatcher{})
}
