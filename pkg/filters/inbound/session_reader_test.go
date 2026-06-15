package inbound

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func TestSessionReaderFilter_ReadsSessionID(t *testing.T) {
	f := NewSessionReaderFilter("X-Session-ID")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set("X-Session-ID", "sess-abc-123")

	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gctx.SessionID != "sess-abc-123" {
		t.Errorf("expected SessionID 'sess-abc-123', got '%s'", gctx.SessionID)
	}
}

func TestSessionReaderFilter_NoHeader(t *testing.T) {
	f := NewSessionReaderFilter("X-Session-ID")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gctx.SessionID != "" {
		t.Errorf("expected SessionID '', got '%s'", gctx.SessionID)
	}
}

func TestSessionReaderFilter_FallbackToUserID(t *testing.T) {
	f := NewSessionReaderFilter("X-Session-ID")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)
	gctx.UserID = "user-42"

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gctx.SessionID != "user-42" {
		t.Errorf("expected SessionID 'user-42' (fallback to UserID), got '%s'", gctx.SessionID)
	}
}

func TestSessionReaderFilter_ReadsFromLoadBalanceParams(t *testing.T) {
	f := NewSessionReaderFilter("X-Session-ID")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set("X-Custom-Session-ID", "sess-custom-abc")
	r.Header.Set("X-Session-ID", "sess-def-123") // 默认的应该被忽略

	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	gctx.Policy = &policy.Policy{
		LoadBalancePolicy: &policy.LoadBalancePolicy{
			Type:   "STICKY",
			Params: map[string]interface{}{"session_header": "X-Custom-Session-ID"},
		},
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gctx.SessionID != "sess-custom-abc" {
		t.Errorf("expected SessionID 'sess-custom-abc', got '%s'", gctx.SessionID)
	}
}

func TestSessionReaderFilter_ReadsFromLoadBalanceParams_NameKey(t *testing.T) {
	f := NewSessionReaderFilter("X-Session-ID")

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set("X-Custom-Name", "sess-name-abc")

	gctx := core.AcquireContext(w, r)
	defer core.ReleaseContext(gctx)

	gctx.Policy = &policy.Policy{
		LoadBalancePolicy: &policy.LoadBalancePolicy{
			Type:   "STICKY",
			Params: map[string]interface{}{"session_header_name": "X-Custom-Name"},
		},
	}

	err := f.OnRequest(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if gctx.SessionID != "sess-name-abc" {
		t.Errorf("expected SessionID 'sess-name-abc', got '%s'", gctx.SessionID)
	}
}
