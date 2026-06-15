package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// MockDynamicEndpointProvider mock DynamicEndpointProvider
type mockDynamicEndpointProvider struct {
	endpoints map[string][]DynamicEndpoint
}

func (m *mockDynamicEndpointProvider) GetEndpoints(ctx context.Context, model string) []DynamicEndpoint {
	return m.endpoints[model]
}

func TestStaticDiscovery_UpdateHealthAll(t *testing.T) {
	sd := NewStaticDiscovery()
	sd.RegisterService("gpt-4", []*Endpoint{
		{ID: "oai-1", Provider: "gpt-4", Healthy: true},
		{ID: "oai-2", Provider: "gpt-4", Healthy: true},
	})
	sd.RegisterService("claude-3", []*Endpoint{
		{ID: "ant-1", Provider: "claude-3", Healthy: true},
	})

	// 标记 gpt-4 为不健康 (注：在 engine 中，model 传参为 provider 名字，在此即 "gpt-4")
	sd.UpdateHealthAll("gpt-4", HealthStatusUnhealthy)

	instances, _ := sd.List(context.Background(), "gpt-4")
	for _, inst := range instances {
		assert.False(t, inst.Healthy, "expected %s to be unhealthy", inst.ID)
	}

	// claude-3 不受影响
	antInstances, _ := sd.List(context.Background(), "claude-3")
	assert.True(t, antInstances[0].Healthy, "expected claude-3 to remain healthy")

	// 恢复 gpt-4 为健康
	sd.UpdateHealthAll("gpt-4", HealthStatusHealthy)

	instances, _ = sd.List(context.Background(), "gpt-4")
	for _, inst := range instances {
		assert.True(t, inst.Healthy, "expected %s to be healthy", inst.ID)
	}
}

func TestStaticDiscovery_UpdateHealthAll_NonExistent(t *testing.T) {
	sd := NewStaticDiscovery()
	// 不存在的服务不应 panic
	sd.UpdateHealthAll("nonexistent", HealthStatusUnhealthy)
}

func TestDynamicDiscovery_ListAndWatch(t *testing.T) {
	dp := &mockDynamicEndpointProvider{
		endpoints: map[string][]DynamicEndpoint{
			"glm-5": {
				{
					ProviderName:     "zhipu-dynamic",
					ProviderProtocol: "openai",
					URL:              "http://zhipu-api.com",
					APIKey:           "key-xyz",
					RealModel:        "glm-5",
					Weight:           100,
					RequestTypes:     []string{"chat_completion"},
				},
			},
		},
	}

	disc := NewDynamicDiscovery()
	disc.SetDynamicProvider(dp)

	eps, err := disc.List(context.Background(), "glm-5")
	assert.NoError(t, err)
	assert.Len(t, eps, 1)
	assert.Equal(t, "zhipu-dynamic-glm-5-0", eps[0].ID)
	assert.Equal(t, "zhipu-dynamic", eps[0].Provider)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := disc.Watch(ctx, "glm-5")
	assert.NoError(t, err)

	select {
	case endpoints := <-ch:
		assert.Len(t, endpoints, 1)
		assert.Equal(t, "zhipu-dynamic", endpoints[0].Provider)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for watch")
	}

	cancel()
	select {
	case _, ok := <-ch:
		assert.False(t, ok)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for close")
	}
}

func TestCompositeDiscovery_PriorityFallback(t *testing.T) {
	// 1. Static Setup
	staticDisc := NewStaticDiscovery()
	staticDisc.RegisterService("gpt-4", []*Endpoint{
		{ID: "static-inst"},
	})

	// 2. Dynamic Setup
	dp := &mockDynamicEndpointProvider{
		endpoints: map[string][]DynamicEndpoint{
			"gpt-4": {
				{
					ProviderName:     "dynamic-p",
					ProviderProtocol: "openai",
					URL:              "http://test.com",
					RequestTypes:     []string{"chat_completion"},
				},
			},
		},
	}
	dynamicDisc := NewDynamicDiscovery()
	dynamicDisc.SetDynamicProvider(dp)

	// 组合起来
	composite := NewCompositeDiscovery([]Discovery{dynamicDisc, staticDisc})

	// Case 1: 动态和静态都有配置。根据优先级退避，必须直接返回动态端点，跳过静态！
	eps, err := composite.List(context.Background(), "gpt-4")
	assert.NoError(t, err)
	assert.Len(t, eps, 1)
	assert.Equal(t, "dynamic-p", eps[0].Provider)

	// Case 2: 动态没有配置，静态有。退避返回静态端点！
	dp.endpoints = nil // 清除动态配置
	eps2, err := composite.List(context.Background(), "gpt-4")
	assert.NoError(t, err)
	assert.Len(t, eps2, 1)
	assert.Equal(t, "static-inst", eps2[0].ID)

	// Case 3: 都没有配置。报错！
	_, err = composite.List(context.Background(), "unknown-model")
	assert.Error(t, err)
}
