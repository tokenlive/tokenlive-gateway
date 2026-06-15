package core

import "regexp"

// ErrorMatcher 错误识别原语
type ErrorMatcher struct {
	StatusCodes     []int
	ErrorCodes      []string
	MessagePatterns []string // regex
	compiledRegexps []*regexp.Regexp
}

// Match 检查错误是否匹配
func (em *ErrorMatcher) Match(statusCode int, errCode string, errMsg string) bool {
	for _, code := range em.StatusCodes {
		if code == statusCode {
			return true
		}
	}
	for _, code := range em.ErrorCodes {
		if code == errCode {
			return true
		}
	}
	matched, _ := em.FindMatchedPattern(errMsg)
	return matched
}

// FindMatchedPattern 查找匹配错误的第一个 MessagePattern。如果匹配，返回 true 和匹配的模式；否则返回 false, ""
func (em *ErrorMatcher) FindMatchedPattern(errMsg string) (bool, string) {
	// 防御性检查：如果设置了 MessagePatterns 但未编译，自动编译
	if em.compiledRegexps == nil && len(em.MessagePatterns) > 0 {
		_ = em.Compile()
	}
	for i, re := range em.compiledRegexps {
		if re != nil && re.MatchString(errMsg) {
			return true, em.MessagePatterns[i]
		}
	}
	return false, ""
}

// Compile 编译正则表达式
func (em *ErrorMatcher) Compile() error {
	// 先构建新切片，全部成功后再替换，避免部分失败导致状态不一致
	compiled := make([]*regexp.Regexp, len(em.MessagePatterns))
	for i, pattern := range em.MessagePatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		compiled[i] = re
	}
	em.compiledRegexps = compiled
	return nil
}

// RetryRule 重试规则
type RetryRule struct {
	Matcher ErrorMatcher
	Retry   bool
}

// CircuitBreakerRule 熔断规则
type CircuitBreakerRule struct {
	Matcher ErrorMatcher
	Failure bool
}
