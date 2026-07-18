package policy

import (
	"encoding/json"
	"math"
	"math/rand"
	"strconv"
	"time"
)

// InvocationPolicy configures invocation retry and fallback behavior.
type InvocationPolicy struct {
	ID             string          `yaml:"id" json:"id"`
	Name           string          `yaml:"name" json:"name"`
	Type           string          `yaml:"type" json:"type"` // e.g. "failover"
	RetryPolicy    *RetryPolicy    `yaml:"retry_policy" json:"retry_policy"`
	FallbackPolicy *FallbackPolicy `yaml:"fallback_policy" json:"fallback_policy"`
}

// UnmarshalJSON accepts camelCase retryPolicy/fallbackPolicy fields.
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

// FallbackPolicy is the fallback model chain.
type FallbackPolicy struct {
	Targets []string `yaml:"targets" json:"targets"` // e.g. ["gpt-4:free", "gpt-3.5-turbo"]
}

// RetryPolicy is the retry sub-config.
type RetryPolicy struct {
	Retry                 int                `yaml:"retry" json:"retry"`
	BackoffType           string             `yaml:"backoff_type" json:"backoff_type"` // e.g. "fixed", "exponential"
	BaseMs                int                `yaml:"base_ms" json:"base_ms"`
	ErrorCodes            []string           `yaml:"error_codes" json:"error_codes"`
	ErrorMessages         []string           `yaml:"error_messages" json:"error_messages"`
	CodePolicy            *ErrorParserPolicy `yaml:"code_policy" json:"code_policy"`
	MessagePolicy         *ErrorParserPolicy `yaml:"message_policy" json:"message_policy"`
	ConnectTimeout        int                `yaml:"connect_timeout" json:"connect_timeout"` // ms
	TtftTimeout           int                `yaml:"ttft_timeout" json:"ttft_timeout"`       // ms
	TotalTimeout          int                `yaml:"total_timeout" json:"total_timeout"`     // ms
	IdleTimeout           int                `yaml:"idle_timeout" json:"idle_timeout"`       // ms
	ExcludeFailedEndpoint *bool              `yaml:"exclude_failed_endpoint" json:"exclude_failed_endpoint"`
	Version               int64              `yaml:"version,omitempty" json:"version,omitempty"`
}

// UnmarshalJSON accepts mixed int/string error_codes and camelCase fields.
func (r *RetryPolicy) UnmarshalJSON(data []byte) error {
	type Alias RetryPolicy
	aux := &struct {
		ErrorCodesCamel            []json.RawMessage  `json:"errorCodes"`
		ErrorCodesSnake            []json.RawMessage  `json:"error_codes"`
		ErrorMessagesCamel         []string           `json:"errorMessages"`
		CodePolicyCamel            *ErrorParserPolicy `json:"codePolicy"`
		MessagePolicyCamel         *ErrorParserPolicy `json:"messagePolicy"`
		ConnectTimeoutCamel        int                `json:"connectTimeout"`
		TtftTimeoutCamel           int                `json:"ttftTimeout"`
		TotalTimeoutCamel          int                `json:"totalTimeout"`
		IdleTimeoutCamel           int                `json:"idleTimeout"`
		BackoffTypeCamel           string             `json:"backoffType"`
		BaseMsCamel                int                `json:"baseMs"`
		ExcludeFailedEndpointCamel *bool              `json:"excludeFailedEndpoint"`
		*Alias
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

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

	if len(aux.ErrorMessagesCamel) > 0 {
		r.ErrorMessages = aux.ErrorMessagesCamel
	}

	if aux.CodePolicyCamel != nil {
		r.CodePolicy = aux.CodePolicyCamel
	}
	if aux.MessagePolicyCamel != nil {
		r.MessagePolicy = aux.MessagePolicyCamel
	}

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

	// Values in (0, 1000) are treated as seconds and converted to ms.
	if r.TotalTimeout > 0 && r.TotalTimeout < 1000 {
		r.TotalTimeout = r.TotalTimeout * 1000
	}
	if r.IdleTimeout > 0 && r.IdleTimeout < 1000 {
		r.IdleTimeout = r.IdleTimeout * 1000
	}

	if aux.ExcludeFailedEndpointCamel != nil {
		r.ExcludeFailedEndpoint = aux.ExcludeFailedEndpointCamel
	}

	return nil
}

// IsExcludeFailedEndpoint reports whether failed endpoints are excluded on retry (default true).
func (r *RetryPolicy) IsExcludeFailedEndpoint() bool {
	if r.ExcludeFailedEndpoint == nil {
		return true
	}
	return *r.ExcludeFailedEndpoint
}

// GetErrorCodes implements ErrorPolicy.
func (r *RetryPolicy) GetErrorCodes() []string { return r.ErrorCodes }

// GetErrorMessages implements ErrorPolicy.
func (r *RetryPolicy) GetErrorMessages() []string { return r.ErrorMessages }

// GetCodePolicy implements ErrorPolicy.
func (r *RetryPolicy) GetCodePolicy() *ErrorParserPolicy { return r.CodePolicy }

// GetMessagePolicy implements ErrorPolicy.
func (r *RetryPolicy) GetMessagePolicy() *ErrorParserPolicy { return r.MessagePolicy }

// CalcBackoff returns the backoff duration for the given attempt.
func (r *RetryPolicy) CalcBackoff(attempt int) time.Duration {
	if r.BackoffType == "fixed" || r.BackoffType == "" {
		return time.Duration(r.BaseMs) * time.Millisecond
	}
	base := float64(r.BaseMs)
	max := base * 100 // cap at 100x BaseMs
	delay := base * math.Pow(2, float64(attempt))
	if delay > max {
		delay = max
	}
	jitter := delay * 0.2 * (rand.Float64()*2 - 1)
	delay += jitter
	return time.Duration(delay) * time.Millisecond
}
