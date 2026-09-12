package versionreport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

const redisReportTimeout = 5 * time.Second

type redisSender struct {
	options redis.Options
	dialer  func(context.Context, string, string) (net.Conn, error)
	active  chan struct{}
}

// NewRedisSender copies connection settings without modifying or closing client.
// It uses context-aware TCP/unix and TLS dialing from Network, Addr and TLSConfig;
// Options.Dialer is intentionally not reused, because go-redis's default dialer
// closes over the original options and its TLS path ignores caller cancellation.
func NewRedisSender(client *redis.Client) Sender {
	return newRedisSender(client, nil)
}

// NewRedisSenderWithDialer explicitly opts into a custom transport. The dialer
// must honor ctx and return a fully established, exclusively owned connection,
// including any custom TLS handshake. The reporter does not wrap it in TLS again.
// Send calls it synchronously and does not release the send slot until it exits.
// A nil client or dialer disables reporting.
func NewRedisSenderWithDialer(client *redis.Client, dialer func(context.Context, string, string) (net.Conn, error)) Sender {
	if dialer == nil {
		return nil
	}
	return newRedisSender(client, dialer)
}

func newRedisSender(client *redis.Client, dialer func(context.Context, string, string) (net.Conn, error)) Sender {
	if client == nil {
		return nil
	}
	return &redisSender{options: *client.Options(), dialer: dialer, active: make(chan struct{}, 1)}
}

func (s *redisSender) Send(ctx context.Context, node Node) error {
	node, err := normalizeNode(node)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("versionreport: encode record: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, redisReportTimeout)
	defer cancel()
	select {
	case s.active <- struct{}{}:
		defer func() { <-s.active }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	options := s.options
	options.ContextTimeoutEnabled = true
	options.ReadTimeout = boundedRedisTimeout(options.ReadTimeout)
	options.WriteTimeout = boundedRedisTimeout(options.WriteTimeout)
	options.DialTimeout = boundedRedisTimeout(options.DialTimeout)
	options.PoolTimeout = boundedRedisTimeout(options.PoolTimeout)
	options.MaxRetries = -1
	options.DialerRetries = 1
	options.PoolSize = 1
	options.MaxActiveConns = 1
	options.MaxConcurrentDials = 1
	options.MinIdleConns = 0
	options.MaxIdleConns = 1
	// This short-lived client neither needs maintenance handoff workers nor
	// shares the borrowed client's mutable notification configuration/processor.
	options.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	options.PushNotificationProcessor = nil
	if options.TLSConfig != nil {
		options.TLSConfig = options.TLSConfig.Clone()
	}

	// Establish the complete transport in this goroutine. go-redis's pool dials
	// asynchronously with a background-derived context and Close does not join
	// those dials, so it must never own real connection establishment here.
	dialCtx, cancelDial := context.WithTimeout(ctx, options.DialTimeout)
	var conn net.Conn
	if s.dialer != nil {
		conn, err = s.dialer(dialCtx, options.Network, options.Addr)
	} else {
		dialer := &net.Dialer{Timeout: options.DialTimeout, KeepAlive: 5 * time.Minute}
		if options.TLSConfig != nil {
			// tls.Dialer uses HandshakeContext, retaining normal certificate,
			// ServerName/SNI and mTLS behavior while honoring cancellation.
			tlsDialer := &tls.Dialer{NetDialer: dialer, Config: options.TLSConfig}
			conn, err = tlsDialer.DialContext(dialCtx, options.Network, options.Addr)
		} else {
			conn, err = dialer.DialContext(dialCtx, options.Network, options.Addr)
		}
	}
	cancelDial()
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}
	if conn == nil {
		return errors.New("versionreport: dialer returned no connection")
	}
	defer conn.Close()
	if err := ctx.Err(); err != nil {
		return err
	}
	var handedOff atomic.Bool
	options.Dialer = func(context.Context, string, string) (net.Conn, error) {
		if handedOff.Swap(true) {
			return nil, errors.New("versionreport: reporting connection already assigned")
		}
		return conn, nil
	}
	reportClient := redis.NewClient(&options)
	closeOwned := func() {
		// Also close before pool registration, including the narrow handoff race.
		_ = conn.Close()
		_ = reportClient.Close()
	}
	closed := make(chan struct{})
	stopClose := context.AfterFunc(ctx, func() {
		// ContextTimeoutEnabled sets socket deadlines but does not interrupt
		// in-flight I/O on manual cancellation. Closing our own pool does.
		closeOwned()
		close(closed)
	})
	defer func() {
		if stopClose() {
			closeOwned()
		} else {
			// Join the cancellation callback before releasing the send slot.
			<-closed
		}
	}()
	key := "tokenlive:gateway-versions:" + node.Namespace + ":" + node.InstanceID
	err = reportClient.Set(ctx, key, payload, reportTTL).Err()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func boundedRedisTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 || timeout > redisReportTimeout {
		return redisReportTimeout
	}
	return timeout
}

type httpSender struct {
	client   *http.Client
	endpoint string
	token    string
}

// NewHTTPSender requires an explicit sync token. The client is copied to keep
// caller settings unchanged, with a five-second cap and redirects disabled so
// the internal token never follows an Admin redirect to another destination.
func NewHTTPSender(client *http.Client, adminURL, token string) Sender {
	if strings.TrimSpace(token) == "" {
		return nil
	}
	base, err := url.Parse(adminURL)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") ||
		base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil
	}
	reportClient := http.Client{}
	if client != nil {
		reportClient = *client
	}
	if reportClient.Timeout <= 0 || reportClient.Timeout > 5*time.Second {
		reportClient.Timeout = 5 * time.Second
	}
	reportClient.Jar = nil
	reportClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base.Path = strings.TrimRight(base.Path, "/") + "/api/v1/gateway/version"
	base.RawPath = ""
	return &httpSender{client: &reportClient, endpoint: base.String(), token: token}
}

func (s *httpSender) Send(ctx context.Context, node Node) error {
	node, err := normalizeNode(node)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("versionreport: encode record: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("versionreport: create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Sync-Token", s.token)
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("versionreport: send request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("versionreport: Admin returned HTTP %d", response.StatusCode)
	}
	return nil
}

// SelectSender chooses one channel for the reporter's lifetime. A configured
// Redis client takes precedence; send failures never trigger HTTP fallback.
func SelectSender(rdb *redis.Client, client *http.Client, adminURL, token string) Sender {
	if rdb != nil {
		return NewRedisSender(rdb)
	}
	return NewHTTPSender(client, adminURL, token)
}
