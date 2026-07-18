package matcher

import (
	"context"
	"strings"
)

// RequestContext is a read-only request context contract.
type RequestContext interface {
	GetHeader(key string) []string
	GetQuery(key string) []string
	GetCookie(key string) string
	GetSystemValue(key string) string
	GetTagValue(key string) string
}

// TagMatcher is the tag matching contract.
type TagMatcher interface {
	// Match reports whether the request satisfies the TagCondition.
	Match(ctx context.Context, condition *TagCondition, reqCtx RequestContext) bool
}

// TagMatcherFactory registers matchers.
type TagMatcherFactory struct {
	matchers map[string]TagMatcher
}

// DefaultTagMatcherFactory is the global factory singleton.
var DefaultTagMatcherFactory = &TagMatcherFactory{
	matchers: make(map[string]TagMatcher),
}

func (f *TagMatcherFactory) Register(matcherType string, m TagMatcher) {
	f.matchers[matcherType] = m
}

func (f *TagMatcherFactory) Get(matcherType string) TagMatcher {
	return f.matchers[matcherType]
}

// IsWildcard reports whether the pattern is wildcard or empty.
func IsWildcard(pattern string) bool {
	return pattern == "" || pattern == "*" || strings.Contains(pattern, "*")
}

// MatchWildcard does simple prefix/suffix wildcard match.
func MatchWildcard(pattern, val string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	if strings.Contains(pattern, "*") {
		if strings.HasSuffix(pattern, "*") {
			return strings.HasPrefix(val, pattern[:len(pattern)-1])
		}
		if strings.HasPrefix(pattern, "*") {
			return strings.HasSuffix(val, pattern[1:])
		}
	}
	return pattern == val
}
