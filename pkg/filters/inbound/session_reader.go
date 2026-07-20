package inbound

import "github.com/tokenlive/tokenlive-gateway/pkg/core"

// SessionReaderFilter reads SessionID from request headers for sticky session routing.
// Falls back to UserID/Tenant if the header is absent.
type SessionReaderFilter struct {
	headerName string
}

// NewSessionReaderFilter creates a SessionReaderFilter.
// headerName is the request header for SessionID (e.g. "X-Session-ID").
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
