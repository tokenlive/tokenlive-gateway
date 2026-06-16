package policy

import (
	"encoding/json"
	"math"
	"math/rand"
	"strconv"
	"time"
)

// InvocationPolicy 调用与重试/降级策略
type InvocationPolicy struct {
	Type           string          `yaml:"type" json:"type"` // e.g. "failover"
	RetryPolicy    *RetryPolicy    `yaml:"retry_policy" json:"retry_policy"`
	FallbackPolicy *FallbackPolicy `yaml:"fallback_policy" json:"fallback_policy"`
}

// UnmarshalJSON 兼容 retryPolicy/fallbackPolicy 小驼峰字段。
func (i *InvocationPolicy) UnmarshalJSON(data []byte) error {
	type Alias InvocationPolicy
	aux := &struct {
		RetryPolicyCamel    *RetryPolicy    `json:"retryPolicy"`
		FallbackPolicyCamel *FallbackPolicy `json:"fallbackPolicy"`
		*Alias
	}{
		Alias: (*Alias)(i),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if aux.RetryPolicyCamel != nil {
		i.RetryPolicy = aux.RetryPolicyCamel
	}
	if aux.FallbackPolicyCamel != nil {
		i.FallbackPolicy = aux.FallbackPolicyCamel
	}
	return nil
}

// FallbackPolicy 降级子配置
type FallbackPolicy struct {
	Targets []string `yaml:"targets" json:"targets"` // 降级目标模型链条，如 ["gpt-4:free", "gpt-3.5-turbo"]
}

// RetryPolicy 重试子配置
type RetryPolicy struct {
	Retry          int                `yaml:"retry" json:"retry"`                         // 重试次数
	BackoffType    string             `yaml:"backoff_type" json:"backoff_type"`           // 退避类型 (e.g. "fixed", "exponential")
	BaseMs         int                `yaml:"base_ms" json:"base_ms"`                     // 退避间隔 (毫秒)
	ErrorCodes     []string           `yaml:"error_codes" json:"error_codes"`             // 需要重试的错误码/状态码列表
	ErrorMessages  []string           `yaml:"error_messages" json:"error_messages"`       // 需要重试的错误消息列表
	CodePolicy     *ErrorParserPolicy `yaml:"code_policy" json:"code_policy"`             // 错误码解析策略
	MessagePolicy  *ErrorParserPolicy `yaml:"message_policy" json:"message_policy"`       // 错误消息解析策略
	ConnectTimeout int                `yaml:"connect_timeout" json:"connect_timeout"`     // 建立连接超时 (毫秒)
	TtftTimeout    int                `yaml:"ttft_timeout" json:"ttft_timeout"`           // 首字超时 (毫秒)
	TotalTimeout   int                `yaml:"total_timeout" json:"total_timeout"`         // 请求总超时 (毫秒)
	IdleTimeout    int                `yaml:"idle_timeout" json:"idle_timeout"`           // 读空闲超时 (毫秒)
	Version        int64              `yaml:"version,omitempty" json:"version,omitempty"` // 版本标识 (保留)
}

// UnmarshalJSON 自定义反序列化，兼容 error_codes 数组中包含整型或字符型的情况，以及驼峰格式字段
func (r *RetryPolicy) UnmarshalJSON(data []byte) error {
	type Alias RetryPolicy
	aux := &struct {
		ErrorCodesCamel     []json.RawMessage  `json:"errorCodes"`
		ErrorCodesSnake     []json.RawMessage  `json:"error_codes"`
		ErrorMessagesCamel  []string           `json:"errorMessages"`
		CodePolicyCamel     *ErrorParserPolicy `json:"codePolicy"`
		MessagePolicyCamel  *ErrorParserPolicy `json:"messagePolicy"`
		ConnectTimeoutCamel int                `json:"connectTimeout"`
		TtftTimeoutCamel    int                `json:"ttftTimeout"`
		TotalTimeoutCamel   int                `json:"totalTimeout"`
		IdleTimeoutCamel    int                `json:"idleTimeout"`
		BackoffTypeCamel    string             `json:"backoffType"`
		BaseMsCamel         int                `json:"baseMs"`
		*Alias
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// 1. 处理 ErrorCodes (支持 errorCodes 和 error_codes，且兼容整型或字符型)
	var rawCodes []json.RawMessage
	if len(aux.ErrorCodesCamel) > 0 {
		rawCodes = aux.ErrorCodesCamel
	} else if len(aux.ErrorCodesSnake) > 0 {
		rawCodes = aux.ErrorCodesSnake
	}
	if len(rawCodes) > 0 {
		r.ErrorCodes = make([]string, len(rawCodes))
		for i, raw := range rawCodes {
			var s string
			if err := json.Unmarshal(raw, &s); err == nil {
				r.ErrorCodes[i] = s
			} else {
				var val float64
				if err := json.Unmarshal(raw, &val); err == nil {
					r.ErrorCodes[i] = strconv.FormatFloat(val, 'f', -1, 64)
				} else {
					r.ErrorCodes[i] = string(raw)
				}
			}
		}
	}

	// 2. 处理 ErrorMessages
	if len(aux.ErrorMessagesCamel) > 0 {
		r.ErrorMessages = aux.ErrorMessagesCamel
	}

	// 3. 处理 CodePolicy 和 MessagePolicy
	if aux.CodePolicyCamel != nil {
		r.CodePolicy = aux.CodePolicyCamel
	}
	if aux.MessagePolicyCamel != nil {
		r.MessagePolicy = aux.MessagePolicyCamel
	}

	// 4. 处理超时和退避相关字段
	if aux.ConnectTimeoutCamel > 0 {
		r.ConnectTimeout = aux.ConnectTimeoutCamel
	}
	if aux.TtftTimeoutCamel > 0 {
		r.TtftTimeout = aux.TtftTimeoutCamel
	}
	if aux.TotalTimeoutCamel > 0 {
		r.TotalTimeout = aux.TotalTimeoutCamel
	}
	if aux.IdleTimeoutCamel > 0 {
		r.IdleTimeout = aux.IdleTimeoutCamel
	}
	if aux.BackoffTypeCamel != "" {
		r.BackoffType = aux.BackoffTypeCamel
	}
	if aux.BaseMsCamel > 0 {
		r.BaseMs = aux.BaseMsCamel
	}

	// 5. 兼容秒与毫秒的单位换算：如果请求总超时或读空闲超时被当成秒配置（小于1000且大于0），自动转换为毫秒存储
	if r.TotalTimeout > 0 && r.TotalTimeout < 1000 {
		r.TotalTimeout = r.TotalTimeout * 1000
	}
	if r.IdleTimeout > 0 && r.IdleTimeout < 1000 {
		r.IdleTimeout = r.IdleTimeout * 1000
	}

	return nil
}

// GetErrorCodes 获取重试策略的错误码
func (r *RetryPolicy) GetErrorCodes() []string { return r.ErrorCodes }

// GetErrorMessages 获取重试策略的错误消息
func (r *RetryPolicy) GetErrorMessages() []string { return r.ErrorMessages }

// GetCodePolicy 获取重试策略的错误码解析策略
func (r *RetryPolicy) GetCodePolicy() *ErrorParserPolicy { return r.CodePolicy }

// GetMessagePolicy 获取重试策略的错误消息解析策略
func (r *RetryPolicy) GetMessagePolicy() *ErrorParserPolicy { return r.MessagePolicy }

// CalcBackoff 计算策略配置的退避时间
func (r *RetryPolicy) CalcBackoff(attempt int) time.Duration {
	if r.BackoffType == "fixed" || r.BackoffType == "" {
		return time.Duration(r.BaseMs) * time.Millisecond
	}
	base := float64(r.BaseMs)
	max := base * 100 // 默认上限为 100 倍 BaseMs
	delay := base * math.Pow(2, float64(attempt))
	if delay > max {
		delay = max
	}
	// 加上 jitter 随机抖动
	jitter := delay * 0.2 * (rand.Float64()*2 - 1)
	delay += jitter
	return time.Duration(delay) * time.Millisecond
}
