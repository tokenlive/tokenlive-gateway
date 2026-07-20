package core

import (
	"context"
)

// Discovery is the gateway-level service discovery interface.
// Provides model-scoped endpoint retrieval and dynamic watching.
type Discovery interface {
	// List returns all endpoints supporting the given model.
	List(ctx context.Context, model string) ([]*Endpoint, error)
	// Watch watches endpoint changes for the given model.
	Watch(ctx context.Context, model string) (<-chan []*Endpoint, error)
	// Close closes the discovery service.
	Close() error
}

// ProviderConfig describes an LLM provider and its supported models.
type ProviderConfig struct {
	Name         string        // provider name; also used as serviceName for the underlying ServiceDiscovery
	Type         string        // provider type, e.g. "openai", "anthropic"
	Models       []string      // list of supported models
	RequestTypes []RequestType // supported request types
}
