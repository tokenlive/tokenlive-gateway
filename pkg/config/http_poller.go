package config

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type HTTPConfigPoller struct {
	provider     *HTTPGatewayProvider
	pollInterval time.Duration
	logger       *zap.Logger
	httpClient   *http.Client

	configETag   string
	policiesETag string
	apiKeysETag  string
}

func NewHTTPConfigPoller(provider *HTTPGatewayProvider, pollInterval time.Duration, logger *zap.Logger) *HTTPConfigPoller {
	if pollInterval <= 0 {
		pollInterval = 10 * time.Second
	}
	return &HTTPConfigPoller{
		provider:     provider,
		pollInterval: pollInterval,
		logger:       logger,
		httpClient:   provider.httpClient,
	}
}

func (p *HTTPConfigPoller) Start(
	ctx context.Context,
	onConfigUpdate func(ctx context.Context, gwCfg *GatewayConfig) error,
	onPoliciesUpdate func(ctx context.Context) error,
	onApiKeysUpdate func(ctx context.Context) error,
) {
	p.logger.Info("starting HTTP config poller",
		zap.String("admin_url", p.provider.adminURL),
		zap.String("config_endpoint", p.provider.adminURL+"/api/v1/gateway/config"),
		zap.String("policies_endpoint", p.provider.adminURL+"/api/v1/gateway/policies"),
		zap.String("apikeys_endpoint", p.provider.adminURL+"/api/v1/gateway/apikeys"),
		zap.Duration("interval", p.pollInterval),
	)

	// Run first poll immediately
	if err := p.pollAll(ctx, onConfigUpdate, onPoliciesUpdate, onApiKeysUpdate); err != nil {
		p.logger.Warn("initial config poll failed, will retry", zap.Error(err))
	}

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("HTTP config poller stopped")
			return
		case <-ticker.C:
			if err := p.pollAll(ctx, onConfigUpdate, onPoliciesUpdate, onApiKeysUpdate); err != nil {
				p.logger.Error("config poll failed", zap.Error(err))
			}
		}
	}
}

func (p *HTTPConfigPoller) pollAll(
	ctx context.Context,
	onConfigUpdate func(ctx context.Context, gwCfg *GatewayConfig) error,
	onPoliciesUpdate func(ctx context.Context) error,
	onApiKeysUpdate func(ctx context.Context) error,
) error {
	// 1. Poll Config
	if err := p.pollConfig(ctx, onConfigUpdate); err != nil {
		p.logger.Error("poll config failed", zap.Error(err))
	}

	// 2. Poll Policies
	if err := p.pollPolicies(ctx, onPoliciesUpdate); err != nil {
		p.logger.Error("poll policies failed", zap.Error(err))
	}

	// 3. Poll API Keys
	if err := p.pollApiKeys(ctx, onApiKeysUpdate); err != nil {
		p.logger.Error("poll api keys failed", zap.Error(err))
	}

	return nil
}

func (p *HTTPConfigPoller) pollConfig(ctx context.Context, onUpdate func(ctx context.Context, gwCfg *GatewayConfig) error) error {
	urlStr := p.provider.adminURL + "/api/v1/gateway/config"
	req, err := p.newRequest(ctx, "/api/v1/gateway/config", p.configETag)
	if err != nil {
		return err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		p.logger.Debug("polled config: no changes (304)", zap.String("url", urlStr), zap.String("etag", p.configETag))
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var gwCfg GatewayConfig
	if err := json.NewDecoder(resp.Body).Decode(&gwCfg); err != nil {
		return err
	}

	p.provider.UpdateConfig(&gwCfg)

	if err := onUpdate(ctx, &gwCfg); err != nil {
		return err
	}

	p.configETag = resp.Header.Get("ETag")
	p.logger.Info("polled config: successfully updated", zap.String("url", urlStr), zap.String("etag", p.configETag))
	return nil
}

func (p *HTTPConfigPoller) pollPolicies(ctx context.Context, onUpdate func(ctx context.Context) error) error {
	urlStr := p.provider.adminURL + "/api/v1/gateway/policies"
	req, err := p.newRequest(ctx, "/api/v1/gateway/policies", p.policiesETag)
	if err != nil {
		return err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		p.logger.Debug("polled policies: no changes (304)", zap.String("url", urlStr), zap.String("etag", p.policiesETag))
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var policies []HTTPPolicyItem
	if err := json.NewDecoder(resp.Body).Decode(&policies); err != nil {
		return err
	}

	p.provider.UpdatePolicies(policies)

	if err := onUpdate(ctx); err != nil {
		return err
	}

	p.policiesETag = resp.Header.Get("ETag")
	p.logger.Info("polled policies: successfully updated", zap.String("url", urlStr), zap.String("etag", p.policiesETag))
	return nil
}

func (p *HTTPConfigPoller) pollApiKeys(ctx context.Context, onUpdate func(ctx context.Context) error) error {
	urlStr := p.provider.adminURL + "/api/v1/gateway/apikeys"
	req, err := p.newRequest(ctx, "/api/v1/gateway/apikeys", p.apiKeysETag)
	if err != nil {
		return err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		p.logger.Debug("polled api keys: no changes (304)", zap.String("url", urlStr), zap.String("etag", p.apiKeysETag))
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var apiKeys []HTTPApiKeyItem
	if err := json.NewDecoder(resp.Body).Decode(&apiKeys); err != nil {
		return err
	}

	p.provider.UpdateApiKeys(apiKeys)

	if err := onUpdate(ctx); err != nil {
		return err
	}

	p.apiKeysETag = resp.Header.Get("ETag")
	p.logger.Info("polled api keys: successfully updated", zap.String("url", urlStr), zap.String("etag", p.apiKeysETag))
	return nil
}

func (p *HTTPConfigPoller) newRequest(ctx context.Context, path string, etag string) (*http.Request, error) {
	targetURL := fmt.Sprintf("%s%s", p.provider.adminURL, path)
	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Sync-Token", p.provider.syncToken)
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	return req, nil
}
