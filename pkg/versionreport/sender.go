package versionreport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisSender struct{ client *redis.Client }

// NewRedisSender borrows the existing Redis client without owning its lifecycle.
func NewRedisSender(client *redis.Client) Sender {
	if client == nil {
		return nil
	}
	return &redisSender{client: client}
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
	key := "tokenlive:gateway-versions:" + node.Namespace + ":" + node.InstanceID
	return s.client.Set(ctx, key, payload, reportTTL).Err()
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
