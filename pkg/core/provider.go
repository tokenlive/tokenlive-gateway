package core

import "context"

// ProviderType is the provider type.
type ProviderType string

const (
	ProviderOpenAI    ProviderType = "openai"
	ProviderAnthropic ProviderType = "anthropic"
	ProviderGemini    ProviderType = "gemini"
	ProviderJoyCode   ProviderType = "joycode"
)

// ProtocolFamily is the endpoint protocol family for RequestType matching.
type ProtocolFamily string

const (
	ProtocolOpenAI    ProtocolFamily = "openai"
	ProtocolAnthropic ProtocolFamily = "anthropic"
	ProtocolGemini    ProtocolFamily = "gemini"
	ProtocolJoyCode   ProtocolFamily = "joycode"
)

// Provider is the capability-based protocol adapter.
type Provider interface {
	Name() string
	Type() ProviderType
	RequestTypes() []RequestType
	Invoke(gctx *GatewayContext) error
	HealthCheck(ctx context.Context) error
	ValidateConfig() error
}
