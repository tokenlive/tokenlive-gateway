package llm

import (
	"fmt"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// ProviderConfig holds provider creation parameters.
type ProviderConfig struct {
	Name    string
	BaseURL string
	APIKey  string
	Models  []string
}

// NewProvider creates a core.Provider by type name using the registered factory.
func NewProvider(providerType string, cfg ProviderConfig) (core.Provider, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("provider %q: base_url is required", cfg.Name)
	}

	pt := core.ProviderType(providerType)
	factory, ok := core.GetProviderFactory(pt)
	if !ok {
		return nil, fmt.Errorf("unsupported provider type: %s", providerType)
	}

	return factory(cfg.Name, cfg.BaseURL, cfg.APIKey, cfg.Models), nil
}
