package versionreport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
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

// pipeRedis exercises real go-redis socket reads/writes without an external
// server. Its SET response can stall until the actual client connection closes.
type pipeRedis struct {
	started chan struct{}
	release chan struct{}
	stall   atomic.Bool
	write   atomic.Bool
	active  atomic.Int32
	dials   atomic.Int32
	wg      sync.WaitGroup
}

type pipeRedisConn struct {
	net.Conn
	server *pipeRedis
	closed chan struct{}
	once   sync.Once
}

func (c *pipeRedisConn) Write(payload []byte) (int, error) {
	if c.server.write.Load() && bytes.Contains(payload, []byte("\r\nset\r\n")) {
		select {
		case c.server.started <- struct{}{}:
		default:
		}
	}
	return c.Conn.Write(payload)
}

func (c *pipeRedisConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		c.server.active.Add(-1)
		close(c.closed)
	})
	return err
}

func newPipeRedis(t *testing.T, timeout time.Duration) (*pipeRedis, *redis.Client) {
	t.Helper()
	server := &pipeRedis{started: make(chan struct{}, 1), release: make(chan struct{})}
	server.stall.Store(true)
	client := redis.NewClient(&redis.Options{
		Protocol: 2, DisableIdentity: true, MaxRetries: -1,
		ReadTimeout: timeout, WriteTimeout: timeout,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			conn := &pipeRedisConn{Conn: clientConn, server: server, closed: make(chan struct{})}
			server.active.Add(1)
			server.dials.Add(1)
			server.wg.Add(1)
			go func() {
				defer server.wg.Done()
				defer serverConn.Close()
				reader := bufio.NewReader(serverConn)
				for {
					args, err := readRedisCommand(reader)
					if err != nil {
						return
					}
					switch strings.ToLower(args[0]) {
					case "hello":
						_, err = io.WriteString(serverConn, "-ERR unknown command 'hello'\r\n")
						if server.write.Load() && server.stall.Load() {
							// Do not read SET: net.Pipe.Write must actually block.
							select {
							case <-server.release:
							case <-conn.closed:
								return
							}
						}
					case "set":
						if server.stall.Load() {
							select {
							case server.started <- struct{}{}:
							default:
							}
							select {
							case <-server.release:
							case <-conn.closed:
								return
							}
						}
						_, err = io.WriteString(serverConn, "+OK\r\n")
					default:
						_, err = io.WriteString(serverConn, "+OK\r\n")
					}
					if err != nil {
						return
					}
				}
			}()
			return conn, nil
		},
	})
	t.Cleanup(func() {
		close(server.release)
		_ = client.Close()
		done := make(chan struct{})
		go func() { server.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Redis fixture still has an unclosed client connection")
		}
	})
	return server, client
}

func readRedisCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}
	args := make([]string, count)
	for i := range args {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
		if err != nil {
			return nil, err
		}
		value := make([]byte, size+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		args[i] = string(value[:size])
	}
	return args, nil
}

func TestRunStalledRedisCancellationPreservesBorrowedClient(t *testing.T) {
	for _, timeout := range []time.Duration{30 * time.Second, -2} {
		t.Run(timeout.String(), func(t *testing.T) {
			server, borrowed := newPipeRedis(t, timeout)
			if err := borrowed.Ping(context.Background()).Err(); err != nil {
				t.Fatal(err)
			}
			options := *borrowed.Options()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			n, _ := fixtureNode(t)
			done := make(chan struct{})
			go func() {
				defer close(done)
				Run(ctx, NewRedisSender(borrowed), n, nil)
			}()
			select {
			case <-server.started:
			case <-time.After(time.Second):
				t.Fatal("SET never reached the stalled Redis server")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("reporter is blocked on Redis I/O after cancellation")
				return
			}
			if server.active.Load() != 1 {
				t.Fatalf("reporter did not close only its private connection: active=%d", server.active.Load())
			}
			if err := borrowed.Ping(context.Background()).Err(); err != nil {
				t.Fatalf("reporter closed the borrowed Redis client: %v", err)
			}
			if borrowed.Options().ContextTimeoutEnabled != options.ContextTimeoutEnabled ||
				borrowed.Options().ReadTimeout != options.ReadTimeout ||
				borrowed.Options().WriteTimeout != options.WriteTimeout ||
				borrowed.Options().PoolSize != options.PoolSize {
				t.Fatal("reporter mutated borrowed Redis options")
			}
		})
	}
}

func TestRedisSenderDeadlineAndRecovery(t *testing.T) {
	server, borrowed := newPipeRedis(t, -2)
	sender := NewRedisSender(borrowed)
	n, _ := fixtureNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- sender.Send(ctx, n) }()
	select {
	case <-server.started:
	case <-time.After(time.Second):
		t.Fatal("SET did not start")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("stalled Redis report did not time out")
		}
	case <-time.After(time.Second):
		t.Error("Redis report ignored its deadline")
		return
	}
	if server.active.Load() != 0 {
		t.Fatal("timed-out report retained its private Redis connection")
	}
	server.stall.Store(false)
	if err := sender.Send(context.Background(), n); err != nil {
		t.Fatalf("sender failed to recover after its previous timeout: %v", err)
	}
	if server.active.Load() != 0 {
		t.Fatal("successful report retained its private Redis connection")
	}
}

func TestRedisSenderCapsIOWithoutCallerDeadline(t *testing.T) {
	server, borrowed := newPipeRedis(t, -2)
	n, _ := fixtureNode(t)
	result := make(chan error, 1)
	go func() { result <- NewRedisSender(borrowed).Send(context.Background(), n) }()
	select {
	case <-server.started:
	case <-time.After(time.Second):
		t.Fatal("SET did not start")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("stalled Redis report should time out")
		}
	case <-time.After(6 * time.Second):
		t.Error("Redis report exceeded its five-second I/O budget")
		return
	}
	if server.active.Load() != 0 {
		t.Fatal("expired report left a Redis connection active")
	}
}

func TestRedisSenderClosesOwnedConnectionAndPreservesDatabaseAuthentication(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.RequireAuth("test-password")
	borrowed := redis.NewClient(&redis.Options{Addr: mr.Addr(), Password: "test-password", DB: 3})
	defer borrowed.Close()
	n, _ := fixtureNode(t)
	if err := NewRedisSender(borrowed).Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	const key = "tokenlive:gateway-versions:default:00000000-0000-4000-8000-000000000001"
	if !mr.DB(3).Exists(key) || mr.DB(0).Exists(key) {
		t.Fatal("private reporter client did not preserve the configured database")
	}
	deadline := time.Now().Add(time.Second)
	for mr.CurrentConnectionCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if mr.CurrentConnectionCount() != 0 {
		t.Fatal("completed report left its owned Redis connection open")
	}
	if err := borrowed.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisSenderStalledWriteCancellation(t *testing.T) {
	server, borrowed := newPipeRedis(t, -2)
	server.write.Store(true)
	n, _ := fixtureNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- NewRedisSender(borrowed).Send(ctx, n) }()
	select {
	case <-server.started:
	case <-time.After(time.Second):
		t.Fatal("SET write never started")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled SET returned %v", err)
		}
	case <-time.After(time.Second):
		t.Error("Redis SET write remained blocked after cancellation")
		return
	}
	if server.active.Load() != 0 {
		t.Fatal("canceled SET retained its private Redis connection")
	}
}

func TestRedisSenderLimitsConcurrentReportsToOneConnection(t *testing.T) {
	server, borrowed := newPipeRedis(t, -2)
	sender := NewRedisSender(borrowed)
	n, _ := fixtureNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sender.Send(ctx, n) }()
	select {
	case <-server.started:
	case <-time.After(time.Second):
		t.Fatal("SET did not start")
	}
	short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelShort()
	if err := sender.Send(short, n); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued report did not honor its shorter deadline: %v", err)
	}
	if server.dials.Load() != 1 || server.active.Load() != 1 {
		t.Fatalf("sender opened concurrent reporting connections: dialed=%d active=%d", server.dials.Load(), server.active.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("reporter did not join its canceled I/O")
		return
	}
}
