package core

import "regexp"

// ErrorMatcher matches errors against configured patterns.
type ErrorMatcher struct {
	StatusCodes     []int
	ErrorCodes      []string
	MessagePatterns []string // regex
	compiledRegexps []*regexp.Regexp
}

// Match reports whether err matches this matcher.
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

// FindMatchedPattern returns the first matching MessagePattern, or false.
func (em *ErrorMatcher) FindMatchedPattern(errMsg string) (bool, string) {
	// Auto-compile MessagePatterns if not yet compiled.
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

// Compile compiles MessagePatterns into regexes.
func (em *ErrorMatcher) Compile() error {
	// Build new slice first; replace only after all succeed.
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

// RetryRule is a retry rule.
type RetryRule struct {
	Matcher ErrorMatcher
	Retry   bool
}

// CircuitBreakerRule is a circuit-breaker rule.
type CircuitBreakerRule struct {
	Matcher ErrorMatcher
	Failure bool
}
