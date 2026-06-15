package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

// writeError 写 JSON 错误响应
func (e *Engine) writeError(w http.ResponseWriter, code int, err error, gctx *GatewayContext) {
	if code == 0 {
		code = http.StatusInternalServerError
	}

	// 动态检测熔断降级返回配置
	if gctx != nil && gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
		degrade := gctx.Policy.CircuitBreakPolicies[0].DegradeConfig
		if degrade != nil && degrade.ResponseBody != "" {
			w.Header().Set("Content-Type", "application/json")
			if degrade.ResponseCode > 0 {
				w.WriteHeader(degrade.ResponseCode)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			_, _ = w.Write([]byte(degrade.ResponseBody))
			return
		}
	}

	var rt RequestType
	if gctx != nil {
		rt = gctx.RequestType
	}
	formatter := ErrorFormatterForRequestType(rt)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(formatter.Format(code, err))
}

// writeJSON 写 JSON 响应，强制写入 Content-Length 头确保传输完整
func (e *Engine) writeJSON(w http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		e.writeError(w, http.StatusInternalServerError, err, nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// getErrorCode 从 error 中提取 HTTP 状态码
func (e *Engine) getErrorCode(err error) int {
	// 1. 检查是否实现了 Code() int 方法（接口断言）
	type codeGetter interface {
		Code() int
	}
	if cg, ok := err.(codeGetter); ok {
		code := cg.Code()
		if code >= 400 && code < 600 {
			return code
		}
	}

	// 2. 通过 reflect 检查 error 是否有 Code int 字段（如 filters.HTTPError）
	v := reflect.ValueOf(err)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		codeField := v.FieldByName("Code")
		if codeField.IsValid() && codeField.Kind() == reflect.Int {
			code := int(codeField.Int())
			if code >= 400 && code < 600 {
				return code
			}
		}
	}

	// 3. 直接检查常见错误类型
	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "upstream error: status "):
		var code int
		if _, scanErr := fmt.Sscanf(errMsg, "upstream error: status %d", &code); scanErr == nil {
			if code >= 400 && code < 600 {
				return code
			}
		}
		return http.StatusInternalServerError
	case strings.Contains(errMsg, "no available endpoint"):
		return http.StatusServiceUnavailable
	case strings.Contains(errMsg, "all fallback"):
		return http.StatusServiceUnavailable
	case strings.Contains(errMsg, "no pipeline"):
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

// getInboundFilter 从注册表获取 InboundFilter
func (e *Engine) getInboundFilter(name string) (InboundFilter, bool) {
	f, ok := e.filterRegistry[name]
	if !ok {
		return nil, false
	}
	inf, ok := f.(InboundFilter)
	return inf, ok
}

// getOutboundFilter 从注册表获取 OutboundFilter
func (e *Engine) getOutboundFilter(name string) (OutboundFilter, bool) {
	f, ok := e.filterRegistry[name]
	if !ok {
		return nil, false
	}
	outf, ok := f.(OutboundFilter)
	return outf, ok
}
