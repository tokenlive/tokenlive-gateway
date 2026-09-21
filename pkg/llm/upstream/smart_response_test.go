package upstream

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestSmartUpstreamResponseIsBoundedAndErrorDoesNotExposeBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/failure" {
			w.WriteHeader(500)
		}
		_, _ = io.WriteString(w, strings.Repeat("private-prompt", 100))
	}))
	defer server.Close()
	for _, path := range []string{"/success", "/failure"} {
		g := core.AcquireContext(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
		g.Ctx = context.Background()
		g.MaxResponseBytes = 32
		g.Sensitive = true
		resp, err := Call(g, Request{Client: server.Client(), URL: server.URL + path, Body: []byte(`{"secret":"private-prompt"}`)})
		if path == "/success" {
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil || len(body) > 32 {
				t.Fatalf("unbounded body: bytes=%d error=%v", len(body), readErr)
			}
		} else if err == nil || strings.Contains(err.Error(), "private-prompt") || len(g.UpstreamBody) > 32 {
			t.Fatalf("unbounded or leaked error: %v body=%d", err, len(g.UpstreamBody))
		}
		core.ReleaseContext(g)
	}
}
