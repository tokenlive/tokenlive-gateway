package matcher

import (
	"context"
)

// QueryTagMatcher 匹配 URL Query 参数
type QueryTagMatcher struct{}

func (m *QueryTagMatcher) Match(ctx context.Context, cond *TagCondition, reqCtx RequestContext) bool {
	if reqCtx == nil {
		return false
	}
	actual := reqCtx.GetQuery(cond.Key)
	return cond.MatchValues(actual)
}

func init() {
	DefaultTagMatcherFactory.Register("query", &QueryTagMatcher{})
}
