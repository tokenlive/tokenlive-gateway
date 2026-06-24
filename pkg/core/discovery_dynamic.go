package core

import (
	"context"
	"fmt"
)

// DynamicEndpoint 动态端点描述结构
type DynamicEndpoint struct {
	ID                 string
	Code               string
	ProviderName       string
	ProviderProtocol   string
	URL                string
	APIKey             string
	RealModel          string
	Weight             int
	Priority           int
	Headers            map[string]string
	Metadata           map[string]string
	RequestTypes       []string
	InputPrice         *float64
	OutputPrice        *float64
	CachedPrice        *float64
	CacheCreationPrice *float64
}

// DynamicEndpointProvider 动态端点提供者接口
type DynamicEndpointProvider interface {
	GetEndpoints(ctx context.Context, model string) []DynamicEndpoint
}

// DynamicDiscovery 负责从动态数据接口获取端点信息并封装为 Endpoint 实例
type DynamicDiscovery struct {
	provider DynamicEndpointProvider
}

// NewDynamicDiscovery 创建 DynamicDiscovery
func NewDynamicDiscovery() *DynamicDiscovery {
	return &DynamicDiscovery{}
}

// SetDynamicProvider 设置动态端点提供器
func (d *DynamicDiscovery) SetDynamicProvider(provider DynamicEndpointProvider) {
	d.provider = provider
}

// List 实现 core.Discovery 接口
func (d *DynamicDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	if d.provider == nil {
		return nil, fmt.Errorf("dynamic provider not configured")
	}

	endpoints := d.provider.GetEndpoints(ctx, model)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no dynamic provider configured for model: %s", model)
	}

	result := make([]*Endpoint, 0, len(endpoints))
	for i, de := range endpoints {
		if len(de.RequestTypes) == 0 {
			return nil, fmt.Errorf("dynamic endpoint for model %s (provider %s) has no request_types configured", model, de.ProviderName)
		}
		var apis []RequestType
		for _, capStr := range de.RequestTypes {
			apis = append(apis, RequestType(capStr))
		}

		epID := de.ID
		if epID == "" {
			epID = fmt.Sprintf("%s-%s-%d", de.ProviderName, model, i)
		}
		ep := &Endpoint{
			ID:                 epID,
			Code:               de.Code,
			URL:                de.URL,
			Provider:           de.ProviderName,
			ProviderProtocol:   de.ProviderProtocol,
			APIKey:             de.APIKey,
			Model:              model,
			UpstreamModel:      de.RealModel,
			Weight:             de.Weight,
			Priority:           de.Priority,
			Headers:            de.Headers,
			Metadata:           de.Metadata,
			InputPrice:         de.InputPrice,
			OutputPrice:        de.OutputPrice,
			CachedPrice:        de.CachedPrice,
			CacheCreationPrice: de.CacheCreationPrice,
			Healthy:            true,
			RequestTypes:       apis,
		}
		result = append(result, ep)
	}

	return result, nil
}

// Watch 实现 core.Discovery 接口
func (d *DynamicDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	endpoints, err := d.List(ctx, model)
	if err != nil {
		return nil, err
	}

	ch := make(chan []*Endpoint, 1)
	ch <- endpoints

	go func() {
		<-ctx.Done()
		close(ch)
	}()

	return ch, nil
}

// Close 实现 core.Discovery 接口
func (d *DynamicDiscovery) Close() error {
	return nil
}
