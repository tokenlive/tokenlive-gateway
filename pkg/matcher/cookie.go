package matcher

import (
	"context"
)

// CookieTagMatcher 匹配 HTTP Cookie
type CookieTagMatcher struct{}

func (m *CookieTagMatcher) Match(ctx context.Context, cond *TagCondition, reqCtx RequestContext) bool {
	if reqCtx == nil {
		return false
	}
	cookieVal := reqCtx.GetCookie(cond.Key)
	if cookieVal == "" {
		return cond.MatchValues(nil)
	}
	return cond.MatchValues([]string{cookieVal})
}

func init() {
	DefaultTagMatcherFactory.Register("cookie", &CookieTagMatcher{})
}
