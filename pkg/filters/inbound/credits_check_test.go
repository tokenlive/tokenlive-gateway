package inbound

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type mockCreditsChecker struct {
	err error
}

func (m *mockCreditsChecker) CheckCredits(ctx context.Context, apiKey string) error {
	return m.err
}

func TestCreditsCheckFilter(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:  1.0,
			OutputPrice: 2.0,
		},
	}

	t.Run("missing billing policy should fail with 403", func(t *testing.T) {
		checker := &mockCreditsChecker{err: nil}
		f := NewCreditsCheckFilter(checker)
		gctx := &core.GatewayContext{
			UserID: "user-001",
			APIKey: "key-001",
			Policy: nil, // no policy
		}
		err := f.OnRequest(gctx)
		if err == nil {
			t.Fatal("expected err, got nil")
		}
		httpErr, ok := err.(*HTTPError)
		if !ok {
			t.Fatalf("expected *HTTPError, got %T", err)
		}
		if httpErr.Code != http.StatusForbidden {
			t.Errorf("expected 403 status, got %d", httpErr.Code)
		}
	})

	t.Run("tenant user should skip check", func(t *testing.T) {
		checker := &mockCreditsChecker{err: errors.New("should not be called")}
		f := NewCreditsCheckFilter(checker)
		gctx := &core.GatewayContext{
			UserID: "", // tenant user
			Policy: p,
		}
		err := f.OnRequest(gctx)
		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
	})

	t.Run("personal user with sufficient credits", func(t *testing.T) {
		checker := &mockCreditsChecker{err: nil}
		f := NewCreditsCheckFilter(checker)
		gctx := &core.GatewayContext{
			UserID:  "user-001",
			APIKey:  "key-001",
			Policy:  p,
			Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		}
		err := f.OnRequest(gctx)
		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
	})

	t.Run("personal user with insufficient credits", func(t *testing.T) {
		checker := &mockCreditsChecker{err: errors.New("credits exceeded")}
		f := NewCreditsCheckFilter(checker)
		gctx := &core.GatewayContext{
			UserID:  "user-001",
			APIKey:  "key-001",
			Policy:  p,
			Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		}
		err := f.OnRequest(gctx)
		if err == nil {
			t.Fatal("expected err, got nil")
		}
		httpErr, ok := err.(*HTTPError)
		if !ok {
			t.Fatalf("expected *HTTPError, got %T", err)
		}
		if httpErr.Code != http.StatusTooManyRequests {
			t.Errorf("expected 429 status, got %d", httpErr.Code)
		}
	})
}
