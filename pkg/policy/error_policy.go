package policy

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// ErrorPolicy is the common contract for error matching (RetryPolicy, CircuitBreakPolicy).
type ErrorPolicy interface {
	GetErrorCodes() []string
	GetErrorMessages() []string
	GetCodePolicy() *ErrorParserPolicy
	GetMessagePolicy() *ErrorParserPolicy
}

// ContainsErrorCode reports whether the policy lists the given error code.
func ContainsErrorCode(p ErrorPolicy, errorCode string) bool {
	if p == nil {
		return false
	}
	for _, ec := range p.GetErrorCodes() {
		if ec == errorCode {
			return true
		}
	}
	return false
}

var regexCache sync.Map // cacheKey (joined messages) -> []*regexp.Regexp

// FindMatchedErrorMessage checks substring then precompiled regex; returns match and rule text.
func FindMatchedErrorMessage(p ErrorPolicy, errMsg string) (bool, string) {
	if p == nil || errMsg == "" {
		return false, ""
	}
	messages := p.GetErrorMessages()
	if len(messages) == 0 {
		return false, ""
	}

	// Substring first to avoid regex for simple rules
	for _, em := range messages {
		if strings.Contains(errMsg, em) {
			return true, em
		}
	}

	cacheKey := strings.Join(messages, "\n")
	var regexps []*regexp.Regexp
	if val, ok := regexCache.Load(cacheKey); ok {
		regexps = val.([]*regexp.Regexp)
	} else {
		compiled := make([]*regexp.Regexp, 0, len(messages))
		for _, em := range messages {
			if re, err := regexp.Compile(em); err == nil {
				compiled = append(compiled, re)
			} else {
				compiled = append(compiled, nil)
			}
		}
		regexCache.Store(cacheKey, compiled)
		regexps = compiled
	}

	for i, re := range regexps {
		if re != nil {
			if re.MatchString(errMsg) {
				return true, messages[i]
			}
		} else {
			// Compile failed: fall back to substring
			em := messages[i]
			if strings.Contains(errMsg, em) {
				return true, em
			}
		}
	}

	return false, ""
}

// ContainsErrorMessage reports whether any error-message rule matches.
func ContainsErrorMessage(p ErrorPolicy, errMsg string) bool {
	matched, _ := FindMatchedErrorMessage(p, errMsg)
	return matched
}

// IsBodyRequired reports whether body parsing is needed for error matching.
func IsBodyRequired(p ErrorPolicy) bool {
	if p == nil {
		return false
	}
	codePolicy := p.GetCodePolicy()
	messagePolicy := p.GetMessagePolicy()
	return (codePolicy != nil && codePolicy.Parser != "" && codePolicy.Expression != "") ||
		(messagePolicy != nil && messagePolicy.Parser != "" && messagePolicy.Expression != "")
}

// Match is a coarse filter on status and content type via parser policies.
func Match(p ErrorPolicy, status int, contentType string) bool {
	if p == nil {
		return false
	}
	statusStr := strconv.Itoa(status)
	codePolicy := p.GetCodePolicy()
	messagePolicy := p.GetMessagePolicy()

	codeMatch := false
	if codePolicy != nil && codePolicy.Parser != "" {
		codeMatch = codePolicy.Match(statusStr, contentType, "200")
	}
	messageMatch := false
	if messagePolicy != nil && messagePolicy.Parser != "" {
		messageMatch = messagePolicy.Match(statusStr, contentType, "200")
	}

	return codeMatch || messageMatch
}

// MatchErrorWithReason decides if the error should trigger retry/circuit-break and why.
func MatchErrorWithReason(p ErrorPolicy, statusCode int, contentType string, errMsg string, body []byte) (bool, string) {
	if p == nil {
		return false, "policy is nil"
	}

	errorCodes := p.GetErrorCodes()
	errorMessages := p.GetErrorMessages()
	codePolicy := p.GetCodePolicy()
	messagePolicy := p.GetMessagePolicy()

	// No rules configured: match all errors (backward-compatible default)
	if len(errorCodes) == 0 && len(errorMessages) == 0 &&
		(codePolicy == nil || codePolicy.Parser == "") &&
		(messagePolicy == nil || messagePolicy.Parser == "") {
		return true, "no rules configured, matching all errors by default"
	}

	statusStr := strconv.Itoa(statusCode)

	if codePolicy != nil && codePolicy.Parser != "" && codePolicy.Expression != "" {
		if codePolicy.Match(statusStr, contentType, "200") {
			if parsedCode, err := codePolicy.ParseValue(body); err == nil && parsedCode != "" {
				if ContainsErrorCode(p, parsedCode) {
					return true, fmt.Sprintf("matched parsed error code '%s' via code policy", parsedCode)
				}
			}
		}
	} else if len(errorCodes) > 0 {
		// No CodePolicy: match status code against ErrorCodes
		if ContainsErrorCode(p, statusStr) {
			return true, fmt.Sprintf("matched status code '%s' in error codes list", statusStr)
		}
	}

	matchedMsg := false
	matchReason := ""
	if messagePolicy != nil && messagePolicy.Parser != "" && messagePolicy.Expression != "" {
		if messagePolicy.Match(statusStr, contentType, "200") {
			if parsedMsg, err := messagePolicy.ParseValue(body); err == nil && parsedMsg != "" {
				if matched, pat := FindMatchedErrorMessage(p, parsedMsg); matched {
					matchedMsg = true
					matchReason = fmt.Sprintf("matched parsed error message '%s' via message policy (pattern: '%s')", parsedMsg, pat)
				}
			}
		}
	}
	if !matchedMsg && len(errorMessages) > 0 && errMsg != "" {
		if matched, pat := FindMatchedErrorMessage(p, errMsg); matched {
			matchedMsg = true
			matchReason = fmt.Sprintf("matched error message pattern '%s' (error: '%s')", pat, errMsg)
		}
	}
	if matchedMsg {
		return true, matchReason
	}

	return false, "no dynamic rule matched"
}

// MatchError is MatchErrorWithReason without the reason string.
func MatchError(p ErrorPolicy, statusCode int, contentType string, errMsg string, body []byte) bool {
	matched, _ := MatchErrorWithReason(p, statusCode, contentType, errMsg, body)
	return matched
}

// MatchErrorWithReason delegates to the package-level matcher.
func (r *RetryPolicy) MatchErrorWithReason(statusCode int, contentType string, errMsg string, body []byte) (bool, string) {
	return MatchErrorWithReason(r, statusCode, contentType, errMsg, body)
}

// MatchErrorWithReason delegates to the package-level matcher.
func (c *CircuitBreakPolicy) MatchErrorWithReason(statusCode int, contentType string, errMsg string, body []byte) (bool, string) {
	return MatchErrorWithReason(c, statusCode, contentType, errMsg, body)
}

// MatchError delegates to the package-level matcher.
func (r *RetryPolicy) MatchError(statusCode int, contentType string, errMsg string, body []byte) bool {
	return MatchError(r, statusCode, contentType, errMsg, body)
}

// MatchError delegates to the package-level matcher.
func (c *CircuitBreakPolicy) MatchError(statusCode int, contentType string, errMsg string, body []byte) bool {
	return MatchError(c, statusCode, contentType, errMsg, body)
}
