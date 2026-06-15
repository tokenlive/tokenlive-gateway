package matcher

import (
	"context"
	"strings"
)

// RequestContext 通用只读请求上下文契约
type RequestContext interface {
	GetHeader(key string) []string
	GetQuery(key string) []string
	GetCookie(key string) string
	GetSystemValue(key string) string
	GetTagValue(key string) string
}

// TagMatcher 标签匹配契约
type TagMatcher interface {
	// Match 判断当前请求是否满足指定的 TagCondition 条件
	Match(ctx context.Context, condition *TagCondition, reqCtx RequestContext) bool
}

// TagMatcherFactory 匹配器注册工厂
type TagMatcherFactory struct {
	matchers map[string]TagMatcher
}

// DefaultTagMatcherFactory 匹配器全局工厂单例
var DefaultTagMatcherFactory = &TagMatcherFactory{
	matchers: make(map[string]TagMatcher),
}

func (f *TagMatcherFactory) Register(matcherType string, m TagMatcher) {
	f.matchers[matcherType] = m
}

func (f *TagMatcherFactory) Get(matcherType string) TagMatcher {
	return f.matchers[matcherType]
}

// IsWildcard 判定一个模式是否为通配符或未指定
func IsWildcard(pattern string) bool {
	return pattern == "" || pattern == "*" || strings.Contains(pattern, "*")
}

// MatchWildcard 通用简单通配符前缀/后缀匹配
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
