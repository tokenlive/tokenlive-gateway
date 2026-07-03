package core

import (
	"context"
	"sync"
	"time"
)

// ===== mockStateStore =====
type mockStateStore struct {
	mu      sync.Mutex
	latency map[string]time.Duration
	ttft    map[string]time.Duration
}

func newMockStateStore() *mockStateStore {
	return &mockStateStore{
		latency: make(map[string]time.Duration),
		ttft:    make(map[string]time.Duration),
	}
}

func (m *mockStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency[endpointID] = latency
	return nil
}

func (m *mockStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
	return "", nil
}

func (m *mockStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	return nil
}

func (m *mockStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lat, ok := m.latency[endpointID]; ok {
		return lat, nil
	}
	return 0, nil
}

func (m *mockStateStore) RecordTTFT(ctx context.Context, endpointID string, ttft time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ttft[endpointID] = ttft
	return nil
}

func (m *mockStateStore) GetAvgTTFT(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.ttft[endpointID]; ok {
		return v, nil
	}
	return 0, nil
}

func (m *mockStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	return 10000, nil
}

func (m *mockStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	return nil
}

func (m *mockStateStore) RateLimitTake(ctx context.Context, key string, tokens int64, limit int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	return true, capacity - tokens, nil
}

func (m *mockStateStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0, nil
}

func (m *mockStateStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func (m *mockStateStore) Close() error { return nil }

// ===== mockDiscovery =====
type mockDiscovery struct {
	endpoints []*Endpoint
	err       error
}

func (m *mockDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	return m.endpoints, m.err
}

func (m *mockDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	ch := make(chan []*Endpoint)
	close(ch)
	return ch, nil
}

func (m *mockDiscovery) Close() error { return nil }

// ===== mockProvider =====
type mockProvider struct {
	name      string
	invokeErr error
}

func (p *mockProvider) Name() string                { return p.name }
func (p *mockProvider) Type() ProviderType          { return ProviderOpenAI }
func (p *mockProvider) RequestTypes() []RequestType { return []RequestType{RequestTypeChatCompletion} }
func (p *mockProvider) Invoke(gctx *GatewayContext) error {
	return p.invokeErr
}
func (p *mockProvider) HealthCheck(ctx context.Context) error { return nil }
func (p *mockProvider) ValidateConfig() error                 { return nil }

// ===== mockLoadBalancer =====
type mockLoadBalancer struct {
	provider  Provider
	callCount int
}

func (m *mockLoadBalancer) Select(gctx *GatewayContext, endpoints []*Endpoint) Invoker {
	m.callCount++
	if len(endpoints) == 0 {
		return nil
	}
	return &ProviderInvoker{Provider: m.provider, endpoint: endpoints[0]}
}

// ===== ProviderInvoker =====
type ProviderInvoker struct {
	Provider Provider
	endpoint *Endpoint
}

func (pi *ProviderInvoker) Invoke(gctx *GatewayContext) error {
	gctx.SelectedInvoker = pi
	gctx.SelectedEndpoint = pi.endpoint
	gctx.UpstreamConnect = time.Now()
	if pi.Provider == nil {
		return nil
	}
	return pi.Provider.Invoke(gctx)
}

func (pi *ProviderInvoker) Endpoint() *Endpoint {
	return pi.endpoint
}
