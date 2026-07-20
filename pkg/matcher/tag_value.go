package matcher

import (
	"context"
)

// TagValueMatcher matches dynamic tags from GatewayContext.Tags.
type TagValueMatcher struct{}

func (m *TagValueMatcher) Match(ctx context.Context, cond *TagCondition, reqCtx RequestContext) bool {
	if reqCtx == nil {
		return false
	}
	val := reqCtx.GetTagValue(cond.Key)
	if val == "" {
		return cond.MatchValues(nil)
	}
	return cond.MatchValues([]string{val})
}

func init() {
	DefaultTagMatcherFactory.Register("tag", &TagValueMatcher{})
}
