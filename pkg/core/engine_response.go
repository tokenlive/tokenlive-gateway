package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

// writeError writes a JSON error response.
func (e *Engine) writeError(w http.ResponseWriter, code int, err error, gctx *GatewayContext) {
	if code == 0 {
		code = http.StatusInternalServerError
	}

	// Dynamic circuit breaker degrade response config
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

// writeJSON writes a JSON response, forcing Content-Length header for transfer integrity.
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

// getErrorCode extracts the HTTP status code from an error.
func (e *Engine) getErrorCode(err error) int {
	// 1. Check if error implements Code() int method (interface assertion)
	type codeGetter interface {
		Code() int
	}
	if cg, ok := err.(codeGetter); ok {
		code := cg.Code()
		if code >= 400 && code < 600 {
			return code
		}
	}

	// 2. Use reflect to check if error has a Code int field (e.g. filters.HTTPError)
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

	// 3. Check common error types directly
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

// getInboundFilter retrieves an InboundFilter from the registry.
func (e *Engine) getInboundFilter(name string) (InboundFilter, bool) {
	f, ok := e.filterRegistry[name]
	if !ok {
		return nil, false
	}
	inf, ok := f.(InboundFilter)
	return inf, ok
}

// getOutboundFilter retrieves an OutboundFilter from the registry.
func (e *Engine) getOutboundFilter(name string) (OutboundFilter, bool) {
	f, ok := e.filterRegistry[name]
	if !ok {
		return nil, false
	}
	outf, ok := f.(OutboundFilter)
	return outf, ok
}
