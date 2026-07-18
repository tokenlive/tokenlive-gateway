package core

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// CompositeDiscovery is a priority-ordered fallback discovery.
type CompositeDiscovery struct {
	discoveries []Discovery
}

// NewCompositeDiscovery creates a CompositeDiscovery.
func NewCompositeDiscovery(discoveries []Discovery) *CompositeDiscovery {
	return &CompositeDiscovery{
		discoveries: discoveries,
	}
}

// List queries discovery instances in priority order, returning the first successful non-empty result.
func (c *CompositeDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	var lastErr error
	for _, d := range c.discoveries {
		if d == nil {
			continue
		}
		endpoints, err := d.List(ctx, model)
		if err == nil && len(endpoints) > 0 {
			return endpoints, nil
		}
		if err != nil {
			if zl := ctx.Value("zapLogger"); zl != nil {
				if logger, ok := zl.(*zap.Logger); ok {
					logger.Warn("discovery failed in composite chain",
						zap.String("model", model),
						zap.String("discovery_impl", fmt.Sprintf("%T", d)),
						zap.Error(err),
					)
				}
			}
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no endpoints found for model: %s", model)
}

// Watch watches the first discovery that yields instances or a non-empty result.
func (c *CompositeDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	var lastErr error
	for _, d := range c.discoveries {
		if d == nil {
			continue
		}
		endpoints, err := d.List(ctx, model)
		if err == nil && len(endpoints) > 0 {
			return d.Watch(ctx, model)
		}
		if err != nil {
			lastErr = err
		}
	}

	for _, d := range c.discoveries {
		if d != nil {
			return d.Watch(ctx, model)
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no discovery client available for model: %s", model)
}

// Close closes all discovery instances.
func (c *CompositeDiscovery) Close() error {
	var firstErr error
	for _, d := range c.discoveries {
		if d == nil {
			continue
		}
		if err := d.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
