package policy

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

// GetErrorCodes 获取熔断策略的错误码
func (c *CircuitBreakPolicy) GetErrorCodes() []string { return c.ErrorCodes }

// GetErrorMessages 获取熔断策略的错误消息
func (c *CircuitBreakPolicy) GetErrorMessages() []string { return c.ErrorMessages }

// GetCodePolicy 获取熔断策略的错误码解析策略
func (c *CircuitBreakPolicy) GetCodePolicy() *ErrorParserPolicy { return c.CodePolicy }

// GetMessagePolicy 获取熔断策略的错误消息解析策略
func (c *CircuitBreakPolicy) GetMessagePolicy() *ErrorParserPolicy { return c.MessagePolicy }
