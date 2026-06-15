package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// MockDiscoveryForTest mock core.Discovery
type mockDiscoveryForTest struct {
	endpoints map[string][]*Endpoint
	err       error
}

func (m *mockDiscoveryForTest) List(ctx context.Context, model string) ([]*Endpoint, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.endpoints[model], nil
}

func (m *mockDiscoveryForTest) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	ch := make(chan []*Endpoint, 1)
	ch <- m.endpoints[model]
	return ch, nil
}

func (m *mockDiscoveryForTest) Close() error {
	return nil
}

// StubProvider stub core.Provider 实现
type stubProvider struct {
	name string
}

func (s *stubProvider) Name() string                          { return s.name }
func (s *stubProvider) Type() ProviderType                    { return ProviderOpenAI }
func (s *stubProvider) RequestTypes() []RequestType           { return []RequestType{RequestTypeChatCompletion} }
func (s *stubProvider) Invoke(gctx *GatewayContext) error     { return nil }
func (s *stubProvider) HealthCheck(ctx context.Context) error { return nil }
func (s *stubProvider) ValidateConfig() error                 { return nil }

func TestAssemblingDiscovery_List(t *testing.T) {
	inner := &mockDiscoveryForTest{
		endpoints: map[string][]*Endpoint{
			"gpt-4": {
				{
					ID:               "inst-1",
					URL:              "http://127.0.0.1:8000",
					Provider:         "openai-official",
					ProviderProtocol: "openai",
					APIKey:           "sk-test-key",
					Model:            "gpt-4",
					UpstreamModel:    "gpt-4-actual",
					Healthy:          true,
					Weight:           100,
				},
			},
		},
	}

	pImpl := &stubProvider{name: "openai-official"}
	registry := NewProviderRegistry(map[string]Provider{
		"openai-official|openai|http://127.0.0.1:8000|sk-test-key": pImpl,
	})

	assembling := NewAssemblingDiscovery(inner, registry)
	eps, err := assembling.List(context.Background(), "gpt-4")
	assert.NoError(t, err)
	assert.Len(t, eps, 1)
	assert.Equal(t, "inst-1", eps[0].ID)
	assert.Equal(t, pImpl, eps[0].ProviderImpl)
	assert.Equal(t, "gpt-4", eps[0].Model)
	assert.Equal(t, "gpt-4-actual", eps[0].UpstreamModel)
	assert.Equal(t, "openai-official", eps[0].Provider)

	// 测试不存在的模型
	_, err = assembling.List(context.Background(), "gpt-5")
	assert.NoError(t, err)
}

func TestAssemblingDiscovery_Watch(t *testing.T) {
	inner := &mockDiscoveryForTest{
		endpoints: map[string][]*Endpoint{
			"gpt-4": {
				{
					ID:               "inst-1",
					URL:              "http://127.0.0.1:8000",
					Provider:         "openai-official",
					ProviderProtocol: "openai",
					APIKey:           "sk-test-key",
					Model:            "gpt-4",
					UpstreamModel:    "gpt-4-actual",
					Healthy:          true,
					Weight:           100,
				},
			},
		},
	}

	registry := NewProviderRegistry(nil)
	assembling := NewAssemblingDiscovery(inner, registry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := assembling.Watch(ctx, "gpt-4")
	assert.NoError(t, err)
	assert.NotNil(t, ch)

	// 接收推送一次
	select {
	case eps := <-ch:
		assert.Len(t, eps, 1)
		assert.Equal(t, "inst-1", eps[0].ID)
		assert.Equal(t, "gpt-4", eps[0].Model)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for watch channel push")
	}

	// 取消 context，验证 channel 关闭且不挂起
	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel should be closed after ctx cancellation")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestAssemblingDiscovery_AutoAssembly(t *testing.T) {
	mockProto := ProviderType("mock-openai")
	RegisterProviderFactory(mockProto, func(name, baseURL, apiKey string, models []string) Provider {
		return &stubProvider{name: name}
	})

	inner := &mockDiscoveryForTest{
		endpoints: map[string][]*Endpoint{
			"glm-5": {
				{
					ID:               "inst-dynamic",
					URL:              "http://zhipu-api.com",
					Provider:         "zhipu-dynamic",
					ProviderProtocol: string(mockProto),
					APIKey:           "key-xyz",
					Model:            "glm-5",
					UpstreamModel:    "glm-5",
					Healthy:          true,
					Weight:           100,
				},
			},
		},
	}

	registry := NewProviderRegistry(nil)
	assembling := NewAssemblingDiscovery(inner, registry)

	// 第一次调用，触发动态 Provider 装配并缓存
	eps, err := assembling.List(context.Background(), "glm-5")
	assert.NoError(t, err)
	assert.Len(t, eps, 1)
	assert.Equal(t, "inst-dynamic", eps[0].ID)
	assert.NotNil(t, eps[0].ProviderImpl)
	assert.Equal(t, "zhipu-dynamic", eps[0].ProviderImpl.Name())

	// 校验已被正确缓存（key = providerName + "|" + protocol + "|" + apiBase + "|" + apiKey）
	registry.implsMu.RLock()
	cachedImpl, ok := registry.impls["zhipu-dynamic|mock-openai|http://zhipu-api.com|key-xyz"]
	registry.implsMu.RUnlock()
	assert.True(t, ok)
	assert.Equal(t, eps[0].ProviderImpl, cachedImpl)

	// 第二次调用，应该直接复用缓存的 Provider 实例
	eps2, err := assembling.List(context.Background(), "glm-5")
	assert.NoError(t, err)
	assert.Equal(t, eps[0].ProviderImpl, eps2[0].ProviderImpl)
}
