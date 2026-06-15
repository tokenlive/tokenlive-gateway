package inbound

import "github.com/tokenlive/tokenlive-gateway/pkg/core"

// SessionReaderFilter 从请求头中读取 SessionID，用于 Sticky Session 路由。
// 若请求头中没有 SessionID，则回退使用 UserID/Tenent 作为 SessionID。
type SessionReaderFilter struct {
	headerName string
}

// NewSessionReaderFilter 创建 SessionReaderFilter
// headerName: 用于读取 SessionID 的请求头名称（如 "X-Session-ID"）
func NewSessionReaderFilter(headerName string) *SessionReaderFilter {
	return &SessionReaderFilter{headerName: headerName}
}

func (f *SessionReaderFilter) Name() string { return "session_reader" }
func (f *SessionReaderFilter) Order() int   { return 15 }

func (f *SessionReaderFilter) OnRequest(gctx *core.GatewayContext) error {
	headerName := f.headerName
	if gctx.Policy != nil && gctx.Policy.LoadBalancePolicy != nil && gctx.Policy.LoadBalancePolicy.Params != nil {
		if val, ok := gctx.Policy.LoadBalancePolicy.Params["session_header"]; ok {
			if strVal, ok := val.(string); ok && strVal != "" {
				headerName = strVal
			}
		} else if val, ok := gctx.Policy.LoadBalancePolicy.Params["session_header_name"]; ok {
			if strVal, ok := val.(string); ok && strVal != "" {
				headerName = strVal
			}
		}
	}

	sessionID := gctx.Request.Header.Get(headerName)
	if sessionID != "" {
		gctx.SessionID = sessionID
	} else if gctx.UserID != "" {
		gctx.SessionID = gctx.UserID
	} else if gctx.Tenant != "" {
		gctx.SessionID = gctx.Tenant
	}
	return nil
}
