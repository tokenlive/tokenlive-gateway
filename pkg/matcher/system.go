package matcher

import (
	"context"
)

// SystemTagMatcher matches built-in context params (model, user).
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
