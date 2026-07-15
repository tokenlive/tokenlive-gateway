package core

import "context"

// ProviderType Provider 类型
type ProviderType string

const (
	ProviderOpenAI    ProviderType = "openai"
	ProviderAnthropic ProviderType = "anthropic"
	ProviderGemini    ProviderType = "gemini"
	ProviderJoyCode   ProviderType = "joycode"
)

// ProtocolFamily 端点协议簇,用于路由阶段匹配 RequestType
type ProtocolFamily string

const (
	ProtocolOpenAI    ProtocolFamily = "openai"
	ProtocolAnthropic ProtocolFamily = "anthropic"
	ProtocolGemini    ProtocolFamily = "gemini"
	ProtocolJoyCode   ProtocolFamily = "joycode"
)

// Provider 协议适配层（Capability-based）
type Provider interface {
	Name() string
	Type() ProviderType
	RequestTypes() []RequestType
	Invoke(gctx *GatewayContext) error
	HealthCheck(ctx context.Context) error
	ValidateConfig() error
}
