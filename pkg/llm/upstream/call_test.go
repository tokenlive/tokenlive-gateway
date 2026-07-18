package upstream

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func newGctx(isStream bool) *core.GatewayContext {
	return &core.GatewayContext{
		Ctx:       context.Background(),
		IsStream:  isStream,
		StartTime: time.Now(),
		Request:   httptest.NewRequest(http.MethodPost, "/v1/chat", nil),
	}
}

func TestCall_NonStream_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("auth = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("User-Agent") == "" {
			t.Error("expected User-Agent")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	gctx := newGctx(false)
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	h.Set("Authorization", "Bearer k")

	resp, err := Call(gctx, Request{
		Client: server.Client(),
		URL:    server.URL,
		Body:   []byte(`{"m":1}`),
		Header: h,
		Stream: Consume,
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	defer resp.Body.Close()

	if gctx.UpstreamResponse != resp {
		t.Error("expected UpstreamResponse set")
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"ok":true}` {
		t.Errorf("body = %s", body)
	}
}

func TestCall_4xx_WritesUpstreamBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad"}`))
	}))
	defer server.Close()

	gctx := newGctx(false)
	h := make(http.Header)
	h.Set("Content-Type", "application/json")

	resp, err := Call(gctx, Request{
		Client: server.Client(),
		URL:    server.URL,
		Body:   []byte(`{}`),
		Header: h,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if resp != nil {
		t.Fatal("expected nil resp on 4xx")
	}
	if string(gctx.UpstreamBody) != `{"error":"bad"}` {
		t.Errorf("UpstreamBody = %s", gctx.UpstreamBody)
	}
	if !strings.Contains(err.Error(), "status 400") {
		t.Errorf("err = %v", err)
	}
}

func TestCall_Stream_NonSSE_Rejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"not":"sse"}`))
	}))
	defer server.Close()

	gctx := newGctx(true)
	h := make(http.Header)
	h.Set("Content-Type", "application/json")

	_, err := Call(gctx, Request{
		Client: server.Client(),
		URL:    server.URL,
		Body:   []byte(`{}`),
		Header: h,
		Stream: Consume,
	})
	if err == nil {
		t.Fatal("expected non-sse error")
	}
	if !strings.Contains(err.Error(), "non-stream content-type") {
		t.Errorf("err = %v", err)
	}
	if string(gctx.UpstreamBody) != `{"not":"sse"}` {
		t.Errorf("UpstreamBody = %s", gctx.UpstreamBody)
	}
}

func TestCall_Stream_Handoff_BodyReadableAfterReturn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hi\n\n"))
	}))
	defer server.Close()

	gctx := newGctx(true)
	h := make(http.Header)
	h.Set("Content-Type", "application/json")

	resp, err := Call(gctx, Request{
		Client: server.Client(),
		URL:    server.URL,
		Body:   []byte(`{}`),
		Header: h,
		Stream: Handoff,
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	// 模拟翻译层：返回后仍可读
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(body), "data: hi") {
		t.Errorf("body = %q", body)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestCall_MergesEndpointAndInjectedHeaders(t *testing.T) {
	var got http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	gctx := newGctx(false)
	gctx.SelectedEndpoint = &core.Endpoint{
		ID: "ep1",
		Headers: map[string]string{
			"X-Ep": "from-endpoint",
		},
	}
	gctx.InjectedHeaders = map[string]string{
		"X-Injected": "from-dye",
	}
	// 客户端 UA 应透传
	gctx.Request.Header.Set("User-Agent", "ClientUA/1.0")

	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	h.Set("Authorization", "Bearer k")
	h.Set("X-Ep", "from-auth-should-be-overwritten")

	resp, err := Call(gctx, Request{
		Client: server.Client(),
		URL:    server.URL,
		Body:   []byte(`{}`),
		Header: h,
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	resp.Body.Close()

	if got.Get("User-Agent") != "ClientUA/1.0" {
		t.Errorf("UA = %q", got.Get("User-Agent"))
	}
	if got.Get("X-Ep") != "from-endpoint" {
		t.Errorf("X-Ep = %q", got.Get("X-Ep"))
	}
	if got.Get("X-Injected") != "from-dye" {
		t.Errorf("X-Injected = %q", got.Get("X-Injected"))
	}
	if got.Get("Authorization") != "Bearer k" {
		t.Errorf("Authorization = %q", got.Get("Authorization"))
	}
}

func TestCall_FirstByteTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	gctx := newGctx(false)
	gctx.Policy = &policy.Policy{
		InvocationPolicy: &policy.InvocationPolicy{
			RetryPolicy: &policy.RetryPolicy{
				ConnectTimeout: 1,
				TtftTimeout:    1, // 2ms first-byte
				TotalTimeout:   5000,
			},
		},
	}

	h := make(http.Header)
	h.Set("Content-Type", "application/json")

	_, err := Call(gctx, Request{
		Client: server.Client(),
		URL:    server.URL,
		Body:   []byte(`{}`),
		Header: h,
	})
	if err == nil {
		t.Fatal("expected first-byte timeout")
	}
	if !strings.Contains(err.Error(), "first byte timeout") {
		t.Errorf("err = %v", err)
	}
}
