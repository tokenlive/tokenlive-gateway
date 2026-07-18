package core

import (
	"fmt"
	"time"
)

// RequestType is a request-type enum.
type RequestType string

const (
	RequestTypeChatCompletion        RequestType = "chat_completion"
	RequestTypeEmbedding             RequestType = "embedding"
	RequestTypeImageGeneration       RequestType = "image_generation"
	RequestTypeResponses             RequestType = "responses"
	RequestTypeMessages              RequestType = "messages"
	RequestTypeGeminiGenerateContent RequestType = "gemini_generate_content"
)

// Endpoint is the gateway-layer endpoint view.
type Endpoint struct {
	ID               string
	Code             string
	URL              string
	Provider         string
	Model            string
	UpstreamModel    string // Upstream model name; empty falls back to Model
	Metadata         map[string]string
	Weight           int
	Priority         int // Priority (lower is higher)
	RequestTypes     []RequestType
	Healthy          bool
	ProviderImpl     Provider          // Provider implementation; set by Discovery or Engine
	Headers          map[string]string // Custom headers
	APIKey           string            // Auth credential
	AuthType         string            // Auth type: api_key, oauth_token
	ProviderProtocol string            // Protocol, e.g. "openai", "anthropic"

	// Per-endpoint rates; nil inherits Model Policy.Billing
	InputPrice         *float64
	OutputPrice        *float64
	CachedPrice        *float64
	CacheCreationPrice *float64
}

// SupportsRequestType reports whether the endpoint supports the request type.
// RequestTypes are declared constraints; ProviderProtocol is adapter capability.
// Both must match to avoid misrouting to unsupported protocol invokers.
func (ep *Endpoint) SupportsRequestType(rt RequestType) bool {
	if ep == nil {
		return false
	}
	if !ep.protocolSupportsRequestType(rt) {
		return false
	}

	for _, c := range ep.RequestTypes {
		if c == rt {
			return true
		}
	}

	if ep.declaresRequestType(RequestTypeChatCompletion) {
		switch rt {
		case RequestTypeMessages, RequestTypeResponses:
			return true
		}
	}

	return false
}

func (ep *Endpoint) declaresRequestType(rt RequestType) bool {
	for _, c := range ep.RequestTypes {
		if c == rt {
			return true
		}
	}
	return false
}

func (ep *Endpoint) protocolSupportsRequestType(rt RequestType) bool {
	switch ep.Protocol() {
	case ProtocolOpenAI:
		switch rt {
		case RequestTypeChatCompletion, RequestTypeEmbedding, RequestTypeResponses, RequestTypeMessages:
			return true
		}
	case ProtocolAnthropic:
		return rt == RequestTypeMessages
	case ProtocolGemini:
		return rt == RequestTypeGeminiGenerateContent
	case ProtocolJoyCode:
		switch rt {
		case RequestTypeChatCompletion, RequestTypeResponses, RequestTypeMessages:
			return true
		}
	case "":
		return true
	}
	return false
}

// CostPerToken returns per-token cost from metadata.
func (ep *Endpoint) CostPerToken() float64 {
	if v, ok := ep.Metadata["cost_per_token"]; ok {
		var f float64
		_, _ = fmt.Sscanf(v, "%f", &f)
		return f
	}
	return 0
}

// EffectiveModel returns the model name sent upstream.
func (ep *Endpoint) EffectiveModel() string {
	if ep.UpstreamModel != "" {
		return ep.UpstreamModel
	}
	return ep.Model
}

// Protocol returns the protocol family from ProviderProtocol.
func (ep *Endpoint) Protocol() ProtocolFamily {
	return ProtocolFamily(ep.ProviderProtocol)
}

// AttemptRecord records one invoke attempt.
type AttemptRecord struct {
	Model        string
	EndpointID   string
	EndpointCode string
	Provider     string
	Latency      time.Duration
	StatusCode   int
	ContentType  string
	Body         []byte
	Error        string
	Success      bool
	Timestamp    time.Time
}

// CircuitState is circuit-breaker state.
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}
