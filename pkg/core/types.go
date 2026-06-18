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

// SupportsRequestType 检查端点是否支持指定请求类型。
// RequestTypes 表达模型/端点声明约束，ProviderProtocol 表达实际适配器能力；
// 二者必须同时满足，避免把 OpenAI Chat 能力误判给没有对应 invoker 的协议。
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
	case ProtocolJoyCode:
		switch rt {
		case RequestTypeChatCompletion, RequestTypeResponses:
			return true
		}
	case "":
		return true
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
