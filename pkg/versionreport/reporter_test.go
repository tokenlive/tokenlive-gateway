package versionreport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type recordingSender struct{ calls chan Node }

func (s recordingSender) Send(_ context.Context, n Node) error {
	s.calls <- n
	return nil
}

func TestRunReportsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan Node, 1)
	done := make(chan struct{})
	n := Node{1, "default", "00000000-0000-4000-8000-000000000001", "v1.2.3", "release"}
	go func() {
		Run(ctx, recordingSender{ch}, n, func(time.Duration) <-chan time.Time { return make(chan time.Time) })
		close(done)
	}()
	select {
	case got := <-ch:
		if got != n {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("no initial report")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not stop")
	}
}

type failingSender struct{ calls chan Node }

func (s failingSender) Send(_ context.Context, n Node) error {
	s.calls <- n
	return errors.New("unavailable")
}

func TestRunReportsSameInstanceAfterFailureAndRateLimitsErrors(t *testing.T) {
	logCore, logs := observer.New(zap.WarnLevel)
	restore := zap.ReplaceGlobals(zap.New(logCore))
	defer restore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := Node{1, "default", "00000000-0000-4000-8000-000000000001", "dev", "dev"}
	calls := make(chan Node, 3)
	ticks := make(chan time.Time)
	waits := make(chan time.Duration, 3)
	done := make(chan struct{})
	go func() {
		Run(ctx, failingSender{calls}, n, func(d time.Duration) <-chan time.Time {
			waits <- d
			return ticks
		})
		close(done)
	}()
	for i := 0; i < 3; i++ {
		select {
		case got := <-calls:
			if got != n {
				t.Fatalf("report identity changed: %+v", got)
			}
		case <-time.After(time.Second):
			t.Fatal("reporter stopped after a failure")
		}
		select {
		case interval := <-waits:
			if interval != 30*time.Second {
				t.Fatalf("interval = %v", interval)
			}
		case <-time.After(time.Second):
			t.Fatal("reporter did not schedule its next report")
		}
		if i < 2 {
			ticks <- time.Now()
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("did not stop")
	}
	if logs.Len() != 1 {
		t.Fatalf("expected one rate-limited warning, got %d", logs.Len())
	}
}

func TestRunWithoutSenderOrWithCanceledContextDoesNotSchedule(t *testing.T) {
	after := func(time.Duration) <-chan time.Time {
		t.Fatal("disabled reporter scheduled a timer")
		return nil
	}
	Run(context.Background(), nil, Node{}, after)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := make(chan Node, 1)
	Run(ctx, recordingSender{calls}, Node{}, after)
	if len(calls) != 0 {
		t.Fatal("canceled reporter sent a report")
	}
}

func fixtureNode(t *testing.T) (Node, []byte) {
	t.Helper()
	data, err := os.ReadFile("testdata/node-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var n Node
	if err := json.Unmarshal(data, &n); err != nil {
		t.Fatal(err)
	}
	return n, bytes.TrimSpace(data)
}

func TestRedisSenderWritesFixtureRefreshesTTLAndExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	sender := NewRedisSender(client)
	n, fixture := fixtureNode(t)
	const key = "tokenlive:gateway-versions:default:00000000-0000-4000-8000-000000000001"
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	payload, err := mr.Get(key)
	if err != nil || payload != string(fixture) {
		t.Fatalf("wire fixture mismatch: %q, %v", payload, err)
	}
	if ttl := mr.TTL(key); ttl != 3*time.Minute {
		t.Fatalf("TTL = %v", ttl)
	}
	mr.FastForward(time.Minute)
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if len(mr.Keys()) != 1 || mr.TTL(key) != 3*time.Minute {
		t.Fatalf("repeat report did not refresh the same record: %v", mr.Keys())
	}
	mr.FastForward(3 * time.Minute)
	if mr.Exists(key) {
		t.Fatal("report did not expire")
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("sender closed its borrowed Redis client: %v", err)
	}
}

func TestRedisSenderPreservesNamespaceAndCanonicalizesUUID(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	n := Node{1, "Production_A", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", "unknown", "dev"}
	if err := NewRedisSender(client).Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	const key = "tokenlive:gateway-versions:Production_A:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	payload, err := mr.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	var got Node
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatal(err)
	}
	if got.Namespace != "Production_A" || got.InstanceID != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" {
		t.Fatalf("invalid identity: %+v", got)
	}
	if err := client.Del(context.Background(), key).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestSendersRejectInvalidWireRecords(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	valid, _ := fixtureNode(t)
	for _, tc := range []struct {
		name   string
		change func(*Node)
	}{
		{"schema", func(n *Node) { n.SchemaVersion = 2 }},
		{"empty_namespace", func(n *Node) { n.Namespace = "" }},
		{"invalid_namespace", func(n *Node) { n.Namespace = "bad:*" }},
		{"long_namespace", func(n *Node) { n.Namespace = strings.Repeat("n", 65) }},
		{"uuid", func(n *Node) { n.InstanceID = "not-a-uuid" }},
		{"compact_uuid", func(n *Node) { n.InstanceID = "00000000000040008000000000000001" }},
		{"long_version", func(n *Node) { n.Version = strings.Repeat("x", 129) }},
		{"build_kind", func(n *Node) { n.BuildKind = "professional" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := valid
			tc.change(&n)
			for _, sender := range []Sender{NewRedisSender(client), NewHTTPSender(server.Client(), server.URL, "test-token")} {
				if err := sender.Send(context.Background(), n); err == nil {
					t.Fatal("invalid wire record accepted")
				}
			}
		})
	}
	if calls.Load() != 0 || len(mr.Keys()) != 0 {
		t.Fatal("invalid wire records reached the transport")
	}
}

func TestSelectSenderPrefersRedisWithoutHTTPFallback(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	defer client.Close()
	var httpCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	sender := SelectSender(client, server.Client(), server.URL, "test-token")
	n, _ := fixtureNode(t)
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	mr.SetError("ERR unavailable")
	if err := sender.Send(context.Background(), n); err == nil {
		t.Fatal("expected Redis failure")
	}
	if httpCalls.Load() != 0 {
		t.Fatal("Redis-only sender called HTTP")
	}
}

func TestHTTPSenderUsesSyncTokenAndWireFixture(t *testing.T) {
	n, fixture := fixtureNode(t)
	requests := make(chan *http.Request, 1)
	bodies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, _ := io.ReadAll(r.Body)
		requests <- r
		bodies <- payload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	sender := SelectSender(nil, server.Client(), server.URL+"/", "test-token")
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	req := <-requests
	if req.Method != http.MethodPost || req.URL.Path != "/api/v1/gateway/version" {
		t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
	}
	if req.Header.Get("X-Sync-Token") != "test-token" || req.Header.Get("Authorization") != "" || req.Header.Get("Cookie") != "" {
		t.Fatalf("wrong authentication headers: %v", req.Header)
	}
	if req.Header.Get("Content-Type") != "application/json" || !bytes.Equal(<-bodies, fixture) {
		t.Fatal("wrong JSON content type or wire payload")
	}
}

func TestSelectSenderRequiresTokenAndValidURL(t *testing.T) {
	for _, tc := range []struct{ url, token string }{
		{"", ""},
		{"http://example.invalid", ""},
		{"http://example.invalid", " \t"},
		{"", "test-token"},
		{"not-a-url", "test-token"},
		{"ftp://example.invalid", "test-token"},
		{"http://user:password@example.invalid", "test-token"},
		{"http://example.invalid?secret=value", "test-token"},
	} {
		if SelectSender(nil, nil, tc.url, tc.token) != nil {
			t.Fatalf("enabled unconfigured HTTP sender for %q", tc.url)
		}
		if NewHTTPSender(nil, tc.url, tc.token) != nil {
			t.Fatalf("constructor enabled unsafe HTTP sender for %q", tc.url)
		}
	}
	if NewRedisSender(nil) != nil {
		t.Fatal("nil Redis client enabled a sender")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHTTPSenderEnforcesFiveSecondDeadlineWithoutMutatingClient(t *testing.T) {
	client := &http.Client{
		Timeout: time.Minute,
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			deadline, ok := r.Context().Deadline()
			if !ok || time.Until(deadline) > 5*time.Second || time.Until(deadline) < 4*time.Second {
				t.Errorf("missing five-second report deadline: %v, %v", deadline, ok)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		}),
	}
	n, _ := fixtureNode(t)
	if err := NewHTTPSender(client, "http://example.invalid", "test-token").Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if client.Timeout != time.Minute {
		t.Fatal("sender mutated borrowed HTTP client")
	}
}

func TestHTTPSenderDoesNotForwardTokenOnRedirect(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	n, _ := fixtureNode(t)
	err := NewHTTPSender(server.Client(), server.URL, "test-token").Send(context.Background(), n)
	if err == nil || calls.Load() != 0 {
		t.Fatal("version sender followed a redirect with its sync token")
	}
}

func TestReportContinuesAfterHTTPFailureAndStops(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusNotFound, 0} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if status == 0 {
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			defer close(release)
			client := server.Client()
			client.Timeout = 20 * time.Millisecond
			n, _ := fixtureNode(t)
			sender := NewHTTPSender(client, server.URL, "test-token")
			if err := sender.Send(context.Background(), n); err == nil {
				t.Fatal("HTTP error was not surfaced")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waits := make(chan time.Duration, 1)
			done := make(chan struct{})
			go func() {
				Run(ctx, sender, n, func(d time.Duration) <-chan time.Time {
					waits <- d
					return make(chan time.Time)
				})
				close(done)
			}()
			select {
			case <-waits:
			case <-time.After(time.Second):
				t.Fatal("HTTP failure blocked the next report")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("reporter did not stop")
			}
		})
	}
}
