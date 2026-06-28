package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HTTPPublisher implements Publisher to send events to the admin console via HTTP POST.
type HTTPPublisher struct {
	adminURL   string
	syncToken  string
	httpClient *http.Client
}

// NewHTTPPublisher creates a new HTTPPublisher.
func NewHTTPPublisher(adminURL string, syncToken string) *HTTPPublisher {
	return &HTTPPublisher{
		adminURL:   adminURL,
		syncToken:  syncToken,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
}

func (p *HTTPPublisher) Publish(ctx context.Context, event *OpsEvent) error {
	if p.adminURL == "" {
		return nil
	}

	jsonData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	urlStr := p.adminURL + "/api/v1/gateway/events"
	req, err := http.NewRequestWithContext(ctx, "POST", urlStr, bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sync-Token", p.syncToken)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send event to admin: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("admin returned non-ok status: %d", resp.StatusCode)
	}

	return nil
}

func (p *HTTPPublisher) Close() error {
	return nil
}
