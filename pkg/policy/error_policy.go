package policy

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// ErrorPolicy 定义了错误检测和解析匹配的通用契约接口，RetryPolicy 和 CircuitBreakPolicy 均实现此接口。
type ErrorPolicy interface {
	GetErrorCodes() []string
	GetErrorMessages() []string
	GetCodePolicy() *ErrorParserPolicy
	GetMessagePolicy() *ErrorParserPolicy
}

// ContainsErrorCode 检查策略中是否包含指定的错误码
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

var regexCache sync.Map // 缓存 strings.Join(messages, "\n") -> []*regexp.Regexp

// FindMatchedErrorMessage 检查策略中是否包含指定的错误消息（支持普通子串与预编译正则），并返回是否匹配以及匹配到的规则子串
func FindMatchedErrorMessage(p ErrorPolicy, errMsg string) (bool, string) {
	if p == nil || errMsg == "" {
		return false, ""
	}
	messages := p.GetErrorMessages()
	if len(messages) == 0 {
		return false, ""
	}

	// 1. 先进行普通的子串包含检查，避免对简单非正则规则启动正则匹配引擎以提高性能
	for _, em := range messages {
		if strings.Contains(errMsg, em) {
			return true, em
		}
	}

	// 2. 正则匹配，基于 sync.Map 并发安全预编译缓存以降低运行时性能开销
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
			// fallback 机制：若正则编译失败则尝试常规子串匹配
			em := messages[i]
			if strings.Contains(errMsg, em) {
				return true, em
			}
		}
	}

	return false, ""
}

// ContainsErrorMessage 检查策略中是否包含指定的错误消息（支持普通子串与预编译正则）
func ContainsErrorMessage(p ErrorPolicy, errMsg string) bool {
	matched, _ := FindMatchedErrorMessage(p, errMsg)
	return matched
}

// IsBodyRequired 判断策略是否需要响应体内容进行错误解析
func IsBodyRequired(p ErrorPolicy) bool {
	if p == nil {
		return false
	}
	codePolicy := p.GetCodePolicy()
	messagePolicy := p.GetMessagePolicy()
	return (codePolicy != nil && codePolicy.Parser != "" && codePolicy.Expression != "") ||
		(messagePolicy != nil && messagePolicy.Parser != "" && messagePolicy.Expression != "")
}

// Match 检查策略是否匹配指定的状态码和内容类型（用于初筛）
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

// MatchErrorWithReason 根据动态重试或熔断策略匹配错误，决定是否需要执行相应的熔断/重试动作，并返回匹配详情
func MatchErrorWithReason(p ErrorPolicy, statusCode int, contentType string, errMsg string, body []byte) (bool, string) {
	if p == nil {
		return false, "policy is nil"
	}

	errorCodes := p.GetErrorCodes()
	errorMessages := p.GetErrorMessages()
	codePolicy := p.GetCodePolicy()
	messagePolicy := p.GetMessagePolicy()

	// 特殊判定：若各项错误判定规则均未配置（全为空），默认直接判定为匹配成功（返回 true）
	// 这有利于在未配置熔断过滤码时，默认对所有的异常执行无条件熔断，保证向后兼容
	if len(errorCodes) == 0 && len(errorMessages) == 0 &&
		(codePolicy == nil || codePolicy.Parser == "") &&
		(messagePolicy == nil || messagePolicy.Parser == "") {
		return true, "no rules configured, matching all errors by default"
	}

	statusStr := strconv.Itoa(statusCode)

	// 1. 检查 CodePolicy (错误码解析策略)
	if codePolicy != nil && codePolicy.Parser != "" && codePolicy.Expression != "" {
		if codePolicy.Match(statusStr, contentType, "200") {
			if parsedCode, err := codePolicy.ParseValue(body); err == nil && parsedCode != "" {
				if ContainsErrorCode(p, parsedCode) {
					return true, fmt.Sprintf("matched parsed error code '%s' via code policy", parsedCode)
				}
			}
		}
	} else if len(errorCodes) > 0 {
		// 没有 CodePolicy，且有特定的错误码需求，则直接对齐状态码
		if ContainsErrorCode(p, statusStr) {
			return true, fmt.Sprintf("matched status code '%s' in error codes list", statusStr)
		}
	}

	// 2. 检查 MessagePolicy (错误消息解析策略) 与 ErrorMessages
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

// MatchError 根据动态重试或熔断策略匹配错误，决定是否需要执行相应的熔断/重试动作
func MatchError(p ErrorPolicy, statusCode int, contentType string, errMsg string, body []byte) bool {
	matched, _ := MatchErrorWithReason(p, statusCode, contentType, errMsg, body)
	return matched
}

// MatchErrorWithReason 代理成员方法，使 RetryPolicy 支持获取重试匹配原因
func (r *RetryPolicy) MatchErrorWithReason(statusCode int, contentType string, errMsg string, body []byte) (bool, string) {
	return MatchErrorWithReason(r, statusCode, contentType, errMsg, body)
}

// MatchErrorWithReason 代理成员方法，使 CircuitBreakPolicy 同样支持获取熔断匹配原因
func (c *CircuitBreakPolicy) MatchErrorWithReason(statusCode int, contentType string, errMsg string, body []byte) (bool, string) {
	return MatchErrorWithReason(c, statusCode, contentType, errMsg, body)
}

// MatchError 代理成员方法，使 RetryPolicy 完全向下兼容已有的 MatchError 调用方式
func (r *RetryPolicy) MatchError(statusCode int, contentType string, errMsg string, body []byte) bool {
	return MatchError(r, statusCode, contentType, errMsg, body)
}

// MatchError 代理成员方法，使 CircuitBreakPolicy 同样支持快捷调用方式
func (c *CircuitBreakPolicy) MatchError(statusCode int, contentType string, errMsg string, body []byte) bool {
	return MatchError(c, statusCode, contentType, errMsg, body)
}
