package core

import (
	"fmt"
	"time"
)

// RequestType 请求类型枚举
type RequestType string

const (
	RequestTypeChatCompletion  RequestType = "chat_completion"
	RequestTypeEmbedding       RequestType = "embedding"
	RequestTypeImageGeneration RequestType = "image_generation"
	RequestTypeResponses       RequestType = "responses"
	RequestTypeMessages        RequestType = "messages"
)

// Endpoint Gateway 层的端点视图
type Endpoint struct {
	ID               string
	URL              string
	Provider         string
	Model            string
	UpstreamModel    string // 实际发给上游的模型名，为空时回退到 Model
	Metadata         map[string]string
	Weight           int
	Priority         int // 优先级（越小越优先）
	RequestTypes     []RequestType
	Healthy          bool
	ProviderImpl     Provider          // 关联 of Provider implementation, filled by Discovery or Engine
	Headers          map[string]string // 自定义 Header
	APIKey           string            // 认证凭证
	ProviderProtocol string            // 协议类型，如 "openai", "anthropic"

	// 新增 Endpoint 费率单价（为 nil 时继承 Model 的 Policy.Billing 费率）
	InputPrice         *float64
	OutputPrice        *float64
	CachedPrice        *float64
	CacheCreationPrice *float64
}

// SupportsRequestType 检查端点是否支持指定请求类型
func (ep *Endpoint) SupportsRequestType(rt RequestType) bool {
	for _, c := range ep.RequestTypes {
		if c == rt {
			return true
		}
	}

	// 隐式能力推导：
	// 1. 如果请求类型是 RequestTypeMessages (例如 Anthropic /v1/messages) 且当前端点显式支持 RequestTypeChatCompletion，
	//    由于我们在适配层实现了自动翻译，因此该端点隐式支持 messages 请求，免去模型配置 messages 能力 of 负担。
	if rt == RequestTypeMessages {
		for _, c := range ep.RequestTypes {
			if c == RequestTypeChatCompletion {
				return true
			}
		}
	}
	// 2. 如果请求类型是 RequestTypeResponses (例如 OpenAI /v1/responses) 且当前端点显式支持 RequestTypeChatCompletion，
	//    由于我们支持降级翻译，该端点隐式支持 responses 请求。
	if rt == RequestTypeResponses {
		for _, c := range ep.RequestTypes {
			if c == RequestTypeChatCompletion {
				return true
			}
		}
	}
	return false
}

// CostPerToken 从 metadata 获取每 token 成本
func (ep *Endpoint) CostPerToken() float64 {
	if v, ok := ep.Metadata["cost_per_token"]; ok {
		var f float64
		_, _ = fmt.Sscanf(v, "%f", &f)
		return f
	}
	return 0
}

// EffectiveModel 返回实际发给上游的模型名
func (ep *Endpoint) EffectiveModel() string {
	if ep.UpstreamModel != "" {
		return ep.UpstreamModel
	}
	return ep.Model
}

// Protocol 返回类型化的协议簇(从 ProviderProtocol 字段读取)
func (ep *Endpoint) Protocol() ProtocolFamily {
	return ProtocolFamily(ep.ProviderProtocol)
}

// AttemptRecord 单次尝试记录
type AttemptRecord struct {
	Model      string
	EndpointID string
	Provider   string
	Latency    time.Duration
	StatusCode int
	Error      string
	Success    bool
	Timestamp  time.Time
}

// CircuitState 熔断器状态
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "Closed"
	case CircuitOpen:
		return "Open"
	case CircuitHalfOpen:
		return "Half-Open"
	default:
		return "Unknown"
	}
}
