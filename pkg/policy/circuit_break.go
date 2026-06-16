package policy

import (
	"encoding/json"
	"strconv"
)

// CircuitBreakPolicy 熔断隔离策略
type CircuitBreakPolicy struct {
	Name                        string             `yaml:"name" json:"name"`
	Level                       string             `yaml:"level" json:"level"`                             // e.g. "SERVICE", "INSTANCE"
	SlidingWindowType           string             `yaml:"sliding_window_type" json:"sliding_window_type"` // "time", "count"
	SlidingWindowSize           int                `yaml:"sliding_window_size" json:"sliding_window_size"`
	MinCallsThreshold           int                `yaml:"min_calls_threshold" json:"min_calls_threshold"`
	CodePolicy                  *ErrorParserPolicy `yaml:"code_policy" json:"code_policy"`
	ErrorCodes                  []string           `yaml:"error_codes" json:"error_codes"`
	ErrorMessages               []string           `yaml:"error_messages" json:"error_messages"`
	MessagePolicy               *ErrorParserPolicy `yaml:"message_policy" json:"message_policy"`
	FailureRateThreshold        float64            `yaml:"failure_rate_threshold" json:"failure_rate_threshold"`
	SlowCallRateThreshold       float64            `yaml:"slow_call_rate_threshold" json:"slow_call_rate_threshold"`
	SlowCallDurationThreshold   int                `yaml:"slow_call_duration_threshold" json:"slow_call_duration_threshold"` // 毫秒
	WaitDurationInOpenState     int                `yaml:"wait_duration_in_open_state" json:"wait_duration_in_open_state"`   // 毫秒
	AllowedCallsInHalfOpenState int                `yaml:"allowed_calls_in_half_open_state" json:"allowed_calls_in_half_open_state"`
	ForceOpen                   int                `yaml:"force_open" json:"force_open"`
	OutlierMaxPercent           int                `yaml:"outlier_max_percent" json:"outlier_max_percent"`
	DegradeConfig               *DegradeConfig     `yaml:"degrade_config" json:"degrade_config"`
	Version                     int64              `yaml:"version" json:"version"`
	SlowCallMetric              string             `yaml:"slow_call_metric" json:"slow_call_metric"` // e.g. "TTFT"
}

// DegradeConfig 熔断降级返回配置
type DegradeConfig struct {
	ResponseCode int               `yaml:"response_code" json:"response_code"`
	Attributes   map[string]string `yaml:"attributes" json:"attributes"`
	ResponseBody string            `yaml:"response_body" json:"response_body"`
}

// UnmarshalJSON 兼容 Redis/Admin 侧历史小驼峰熔断策略字段。
func (c *CircuitBreakPolicy) UnmarshalJSON(data []byte) error {
	type Alias CircuitBreakPolicy
	aux := &struct {
		SlidingWindowTypeCamel           string             `json:"slidingWindowType"`
		SlidingWindowSizeCamel           int                `json:"slidingWindowSize"`
		MinCallsThresholdCamel           int                `json:"minCallsThreshold"`
		CodePolicyCamel                  *ErrorParserPolicy `json:"codePolicy"`
		ErrorCodesCamel                  []json.RawMessage  `json:"errorCodes"`
		ErrorCodesSnake                  []json.RawMessage  `json:"error_codes"`
		ErrorMessagesCamel               []string           `json:"errorMessages"`
		MessagePolicyCamel               *ErrorParserPolicy `json:"messagePolicy"`
		FailureRateThresholdCamel        *float64           `json:"failureRateThreshold"`
		SlowCallRateThresholdCamel       *float64           `json:"slowCallRateThreshold"`
		SlowCallDurationThresholdCamel   int                `json:"slowCallDurationThreshold"`
		WaitDurationInOpenStateCamel     int                `json:"waitDurationInOpenState"`
		AllowedCallsInHalfOpenStateCamel int                `json:"allowedCallsInHalfOpenState"`
		ForceOpenCamel                   json.RawMessage    `json:"forceOpen"`
		OutlierMaxPercentCamel           int                `json:"outlierMaxPercent"`
		DegradeConfigCamel               *DegradeConfig     `json:"degradeConfig"`
		SlowCallMetricCamel              string             `json:"slowCallMetric"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	if aux.SlidingWindowTypeCamel != "" {
		c.SlidingWindowType = aux.SlidingWindowTypeCamel
	}
	if aux.SlidingWindowSizeCamel > 0 {
		c.SlidingWindowSize = aux.SlidingWindowSizeCamel
	}
	if aux.MinCallsThresholdCamel > 0 {
		c.MinCallsThreshold = aux.MinCallsThresholdCamel
	}
	if aux.CodePolicyCamel != nil {
		c.CodePolicy = aux.CodePolicyCamel
	}
	rawCodes := aux.ErrorCodesSnake
	if len(aux.ErrorCodesCamel) > 0 {
		rawCodes = aux.ErrorCodesCamel
	}
	if len(rawCodes) > 0 {
		c.ErrorCodes = decodeErrorCodes(rawCodes)
	}
	if len(aux.ErrorMessagesCamel) > 0 {
		c.ErrorMessages = aux.ErrorMessagesCamel
	}
	if aux.MessagePolicyCamel != nil {
		c.MessagePolicy = aux.MessagePolicyCamel
	}
	if aux.FailureRateThresholdCamel != nil {
		c.FailureRateThreshold = *aux.FailureRateThresholdCamel
	}
	if aux.SlowCallRateThresholdCamel != nil {
		c.SlowCallRateThreshold = *aux.SlowCallRateThresholdCamel
	}
	if aux.SlowCallDurationThresholdCamel > 0 {
		c.SlowCallDurationThreshold = aux.SlowCallDurationThresholdCamel
	}
	if aux.WaitDurationInOpenStateCamel > 0 {
		c.WaitDurationInOpenState = aux.WaitDurationInOpenStateCamel
	}
	if aux.AllowedCallsInHalfOpenStateCamel > 0 {
		c.AllowedCallsInHalfOpenState = aux.AllowedCallsInHalfOpenStateCamel
	}
	if len(aux.ForceOpenCamel) > 0 {
		c.ForceOpen = decodeForceOpen(aux.ForceOpenCamel)
	}
	if aux.OutlierMaxPercentCamel > 0 {
		c.OutlierMaxPercent = aux.OutlierMaxPercentCamel
	}
	if aux.DegradeConfigCamel != nil {
		c.DegradeConfig = aux.DegradeConfigCamel
	}
	if aux.SlowCallMetricCamel != "" {
		c.SlowCallMetric = aux.SlowCallMetricCamel
	}
	return nil
}

// UnmarshalJSON 兼容 responseCode/responseBody 小驼峰字段。
func (d *DegradeConfig) UnmarshalJSON(data []byte) error {
	type Alias DegradeConfig
	aux := &struct {
		ResponseCodeCamel int    `json:"responseCode"`
		ResponseBodyCamel string `json:"responseBody"`
		*Alias
	}{
		Alias: (*Alias)(d),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	if aux.ResponseCodeCamel > 0 {
		d.ResponseCode = aux.ResponseCodeCamel
	}
	if aux.ResponseBodyCamel != "" {
		d.ResponseBody = aux.ResponseBodyCamel
	}
	return nil
}

func decodeErrorCodes(rawCodes []json.RawMessage) []string {
	codes := make([]string, 0, len(rawCodes))
	for _, raw := range rawCodes {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			codes = append(codes, s)
			continue
		}
		var val float64
		if err := json.Unmarshal(raw, &val); err == nil {
			codes = append(codes, strconv.FormatFloat(val, 'f', -1, 64))
			continue
		}
		codes = append(codes, string(raw))
	}
	return codes
}

func decodeForceOpen(raw json.RawMessage) int {
	var i int
	if err := json.Unmarshal(raw, &i); err == nil {
		return i
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil && b {
		return 1
	}
	return 0
}

// GetErrorCodes 获取熔断策略的错误码
func (c *CircuitBreakPolicy) GetErrorCodes() []string { return c.ErrorCodes }

// GetErrorMessages 获取熔断策略的错误消息
func (c *CircuitBreakPolicy) GetErrorMessages() []string { return c.ErrorMessages }

// GetCodePolicy 获取熔断策略的错误码解析策略
func (c *CircuitBreakPolicy) GetCodePolicy() *ErrorParserPolicy { return c.CodePolicy }

// GetMessagePolicy 获取熔断策略的错误消息解析策略
func (c *CircuitBreakPolicy) GetMessagePolicy() *ErrorParserPolicy { return c.MessagePolicy }
