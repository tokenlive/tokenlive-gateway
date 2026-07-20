package matcher

import (
	"context"
)

// HeaderTagMatcher matches HTTP headers.
type HeaderTagMatcher struct{}

func (m *HeaderTagMatcher) Match(ctx context.Context, cond *TagCondition, reqCtx RequestContext) bool {
	if reqCtx == nil {
		return false
	}
	actual := reqCtx.GetHeader(cond.Key)
	return cond.MatchValues(actual)
}

func init() {
	DefaultTagMatcherFactory.Register("header", &HeaderTagMatcher{})
}
