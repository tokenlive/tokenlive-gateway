package core

import (
	"context"
)

// AssemblingDiscovery is a decorator that automatically assembles runtime Provider instances onto endpoints.
type AssemblingDiscovery struct {
	inner    Discovery
	registry *ProviderRegistry
}

// NewAssemblingDiscovery creates an AssemblingDiscovery.
func NewAssemblingDiscovery(inner Discovery, registry *ProviderRegistry) *AssemblingDiscovery {
	return &AssemblingDiscovery{
		inner:    inner,
		registry: registry,
	}
}

// List retrieves endpoints and assembles ProviderImpl.
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

// Watch watches endpoint list changes and assembles ProviderImpl.
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

// Close closes the discovery service.
func (ad *AssemblingDiscovery) Close() error {
	return ad.inner.Close()
}
