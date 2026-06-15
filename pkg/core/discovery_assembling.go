package core

import (
	"context"
)

// AssemblingDiscovery 装饰器，自动为底层的 Endpoint 装配运行时 Provider 实例
type AssemblingDiscovery struct {
	inner    Discovery
	registry *ProviderRegistry
}

// NewAssemblingDiscovery 创建 AssemblingDiscovery
func NewAssemblingDiscovery(inner Discovery, registry *ProviderRegistry) *AssemblingDiscovery {
	return &AssemblingDiscovery{
		inner:    inner,
		registry: registry,
	}
}

// List 获取端点列表并装配 ProviderImpl
func (ad *AssemblingDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	endpoints, err := ad.inner.List(ctx, model)
	if err != nil {
		return nil, err
	}

	for _, ep := range endpoints {
		if ep.ProviderImpl == nil {
			ep.ProviderImpl = ad.registry.GetOrCreateProvider(ep)
		}
	}
	return endpoints, nil
}

// Watch 监听端点列表变化并装配 ProviderImpl
func (ad *AssemblingDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	ch, err := ad.inner.Watch(ctx, model)
	if err != nil {
		return nil, err
	}

	out := make(chan []*Endpoint, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case endpoints, ok := <-ch:
				if !ok {
					return
				}
				for _, ep := range endpoints {
					if ep.ProviderImpl == nil {
						ep.ProviderImpl = ad.registry.GetOrCreateProvider(ep)
					}
				}
				select {
				case out <- endpoints:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

// Close 关闭服务发现
func (ad *AssemblingDiscovery) Close() error {
	return ad.inner.Close()
}
