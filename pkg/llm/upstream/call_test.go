package upstream

import (
	"context"
	"errors"
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

func TestCall_Stream_MissingContentType_ShortEventReturnsPromptly(t *testing.T) {
	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header()["Content-Type"] = nil
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hi\n\n"))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()

	type callResult struct {
		resp *http.Response
		err  error
	}
	resultCh := make(chan callResult, 1)
	go func() {
		resp, err := Call(newGctx(true), Request{
			Client: server.Client(),
			URL:    server.URL,
			Header: make(http.Header),
			Stream: Handoff,
		})
		resultCh <- callResult{resp: resp, err: err}
	}()

	var result callResult
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Call blocked after upstream flushed a complete SSE line")
	}
	if result.err != nil {
		t.Fatalf("Call: %v", result.err)
	}
	defer result.resp.Body.Close()

	want := "data: hi\n\n"
	got := make([]byte, len(want))
	if _, err := io.ReadFull(result.resp.Body, got); err != nil {
		t.Fatalf("read replayed event: %v", err)
	}
	if string(got) != want {
		t.Fatalf("replayed event = %q, want %q", got, want)
	}
}

func TestCall_Stream_MissingContentType_ClosePropagates(t *testing.T) {
	closed := make(chan struct{})
	body := &trackingReadCloser{
		Reader: strings.NewReader("data: hi\n\n"),
		closed: closed,
	}
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    r,
		}, nil
	})}

	resp, err := Call(newGctx(true), Request{
		Client: client,
		URL:    "http://upstream.test/v1/responses",
		Header: make(http.Header),
		Stream: Handoff,
	})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case <-closed:
	default:
		t.Fatal("closing returned body did not close upstream body")
	}
}

func TestCall_Stream_MissingContentType_PartialReadError(t *testing.T) {
	errProbeRead := errors.New("probe read failed")
	body := &partialErrorReadCloser{
		data: []byte("data: partial"),
		err:  errProbeRead,
	}
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    r,
		}, nil
	})}
	gctx := newGctx(true)

	resp, err := Call(gctx, Request{
		Client: client,
		URL:    "http://upstream.test/v1/responses",
		Header: make(http.Header),
		Stream: Handoff,
	})
	if err == nil {
		t.Fatal("expected probe read error")
	}
	if resp != nil {
		t.Fatal("expected nil response")
	}
	if !errors.Is(err, errProbeRead) {
		t.Fatalf("error = %v, want wrapped probe error", err)
	}
	if got := string(gctx.UpstreamBody); got != "data: partial" {
		t.Fatalf("UpstreamBody = %q, want %q", got, "data: partial")
	}
}

func TestCall_Stream_MissingContentType_ValidPreambleAndLargeFirstLine(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "large data line",
			body: "data: " + strings.Repeat("x", maxSSEProbeLineSize) + "\n\n",
		},
		{
			name: "blank and unknown fields before data",
			body: "\nunknown: ignored\ndata: hi\n\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(tt.body)),
					Request:    r,
				}, nil
			})}

			resp, err := Call(newGctx(true), Request{
				Client: client,
				URL:    "http://upstream.test/v1/responses",
				Header: make(http.Header),
				Stream: Handoff,
			})
			if err != nil {
				t.Fatalf("Call: %v", err)
			}
			defer resp.Body.Close()

			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read replayed stream: %v", err)
			}
			if string(got) != tt.body {
				t.Fatalf("replayed stream length = %d, want %d", len(got), len(tt.body))
			}
		})
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

func TestLooksLikeSSELine(t *testing.T) {
	tests := []struct {
		name string
		line string
		want bool
	}{
		{name: "comment heartbeat", line: ": heartbeat\n", want: true},
		{name: "data", line: "data: hi\n", want: true},
		{name: "event", line: "event: message\n", want: true},
		{name: "id", line: "id: 42\n", want: true},
		{name: "retry", line: "retry: 1000\n", want: true},
		{name: "field without colon", line: "data\n", want: true},
		{name: "utf8 bom", line: "\ufeffdata: hi\n", want: true},
		{name: "leading whitespace", line: "  event: message\n", want: true},
		{name: "similar field", line: "database: value\n", want: false},
		{name: "json", line: `{"not":"sse"}`, want: false},
		{name: "empty", line: "\n", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := looksLikeSSELine([]byte(tt.line)); got != tt.want {
				t.Fatalf("looksLikeSSELine(%q) = %v, want %v", tt.line, got, tt.want)
			}
		})
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type trackingReadCloser struct {
	io.Reader
	closed chan struct{}
}

type partialErrorReadCloser struct {
	data []byte
	err  error
}

func (b *partialErrorReadCloser) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, b.err
}

func (b *partialErrorReadCloser) Close() error {
	return nil
}

func (b *trackingReadCloser) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}
