package config

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type HTTPGatewayProvider struct {
	adminURL   string
	syncToken  string
	httpClient *http.Client

	// 本地内存缓存，由 HTTP 轮询器定期更新
	mu             sync.RWMutex
	cachedConfig   *GatewayConfig
	cachedPolicies []HTTPPolicyItem
	cachedApiKeys  map[string]*HTTPApiKeyItem
}

func NewHTTPGatewayProvider(adminURL string, syncToken string) *HTTPGatewayProvider {
	return &HTTPGatewayProvider{
		adminURL:      adminURL,
		syncToken:     syncToken,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		cachedApiKeys: make(map[string]*HTTPApiKeyItem),
	}
}

// UpdateConfig 更新本地缓存的网关路由配置
func (p *HTTPGatewayProvider) UpdateConfig(gwCfg *GatewayConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cachedConfig = gwCfg
}

// UpdatePolicies 更新本地缓存的治理策略
func (p *HTTPGatewayProvider) UpdatePolicies(policies []HTTPPolicyItem) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cachedPolicies = policies
}

// UpdateApiKeys 更新本地缓存的 API Keys
func (p *HTTPGatewayProvider) UpdateApiKeys(apiKeys []HTTPApiKeyItem) {
	p.mu.Lock()
	defer p.mu.Unlock()
	newKeys := make(map[string]*HTTPApiKeyItem)
	for i := range apiKeys {
		newKeys[apiKeys[i].APIKey] = &apiKeys[i]
	}
	p.cachedApiKeys = newKeys
}

func (p *HTTPGatewayProvider) GetConfig(ctx context.Context, modelCode string) (*GatewayConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.cachedConfig != nil {
		return p.cachedConfig, nil
	}
	return nil, fmt.Errorf("gateway config not loaded yet")
}

func (p *HTTPGatewayProvider) GetPolicies(ctx context.Context, modelCode, userID, tenantCode string) ([]HTTPPolicyItem, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cachedPolicies, nil
}

func (p *HTTPGatewayProvider) GetApiKey(ctx context.Context, apiKey string) (*HTTPApiKeyItem, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("api key cannot be empty")
	}

	p.mu.RLock()
	item, ok := p.cachedApiKeys[apiKey]
	p.mu.RUnlock()
	if ok {
		return item, nil
	}

	path := fmt.Sprintf("/api/v1/gateway/apikeys?apikey=%s", url.QueryEscape(apiKey))
	req, err := p.newRequest(ctx, path)
	if err != nil {
		return nil, err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status from admin: %d", resp.StatusCode)
	}

	var apiKeys []HTTPApiKeyItem
	if err := json.NewDecoder(resp.Body).Decode(&apiKeys); err != nil {
		return nil, err
	}

	if len(apiKeys) == 0 {
		return nil, fmt.Errorf("api key not found on admin console")
	}

	p.mu.Lock()
	p.cachedApiKeys[apiKey] = &apiKeys[0]
	p.mu.Unlock()

	return &apiKeys[0], nil
}

func (p *HTTPGatewayProvider) GetUserModels(ctx context.Context, userID string) ([]string, error) {
	return []string{"*"}, nil
}

func (p *HTTPGatewayProvider) GetTenantModels(ctx context.Context, tenantCode string) ([]string, error) {
	return []string{"*"}, nil
}

func (p *HTTPGatewayProvider) DeductQuota(ctx context.Context, apiKey string, tokens int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	item, ok := p.cachedApiKeys[apiKey]
	if !ok {
		return 0, fmt.Errorf("api key not found in memory")
	}

	if item.Quota == -1 {
		return -1, nil
	}

	item.Quota -= tokens
	return item.Quota, nil
}

func (p *HTTPGatewayProvider) newRequest(ctx context.Context, path string) (*http.Request, error) {
	targetURL := fmt.Sprintf("%s%s", p.adminURL, path)
	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Sync-Token", p.syncToken)
	return req, nil
}
