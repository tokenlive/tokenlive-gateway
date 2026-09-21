package core

import (
	"fmt"
	"strings"
)

// SmartRoutingConfig is a versioned routing snapshot. Callers own returned clones.
type SmartRoutingConfig struct {
	Version              int64               `json:"version" yaml:"version" mapstructure:"version"`
	JudgeModel           string              `json:"judge_model" yaml:"judge_model" mapstructure:"judge_model"`
	JudgeTimeoutMs       int                 `json:"judge_timeout_ms" yaml:"judge_timeout_ms" mapstructure:"judge_timeout_ms"`
	JudgeMaxInputBytes   int                 `json:"judge_max_input_bytes" yaml:"judge_max_input_bytes" mapstructure:"judge_max_input_bytes"`
	JudgeMaxOutputTokens int                 `json:"judge_max_output_tokens" yaml:"judge_max_output_tokens" mapstructure:"judge_max_output_tokens"`
	Ranges               []SmartRoutingRange `json:"ranges" yaml:"ranges" mapstructure:"ranges"`
}

// SmartRoutingRange includes Min and excludes Max, except the final Max=100.
type SmartRoutingRange struct {
	Min   int    `json:"min" yaml:"min" mapstructure:"min"`
	Max   int    `json:"max" yaml:"max" mapstructure:"max"`
	Model string `json:"model" yaml:"model" mapstructure:"model"`
}

// ApplyDefaults fills omitted technical limits on an owned configuration.
func (c *SmartRoutingConfig) ApplyDefaults() {
	if c.JudgeTimeoutMs == 0 {
		c.JudgeTimeoutMs = 5000
	}
	if c.JudgeMaxInputBytes == 0 {
		c.JudgeMaxInputBytes = 65536
	}
	if c.JudgeMaxOutputTokens == 0 {
		c.JudgeMaxOutputTokens = 256
	}
}

// Clone isolates a request's range list from subsequent configuration refreshes.
func (c *SmartRoutingConfig) Clone() *SmartRoutingConfig {
	if c == nil {
		return nil
	}
	result := *c
	result.Ranges = append([]SmartRoutingRange(nil), c.Ranges...)
	return &result
}

// Validate checks the pure document; dependency types are checked by the source.
func (c *SmartRoutingConfig) Validate(modelCode string) error {
	if c == nil {
		return fmt.Errorf("smart_routing is required")
	}
	if strings.TrimSpace(modelCode) == "" || c.Version < 1 {
		return fmt.Errorf("model code and positive smart_routing version are required")
	}
	if strings.TrimSpace(c.JudgeModel) == "" || strings.EqualFold(c.JudgeModel, modelCode) {
		return fmt.Errorf("judge_model must name a different ordinary model")
	}
	if c.JudgeTimeoutMs < 0 || c.JudgeMaxInputBytes < 0 || c.JudgeMaxOutputTokens < 0 {
		return fmt.Errorf("judge limits must be positive or omitted for defaults")
	}
	if len(c.Ranges) < 2 {
		return fmt.Errorf("at least two ranges are required")
	}
	distinct := make(map[string]bool)
	end := 0
	for i, r := range c.Ranges {
		if r.Min != end || r.Min < 0 || r.Max > 100 || r.Max <= r.Min {
			return fmt.Errorf("range[%d] must form an ascending contiguous partition of 0..100", i)
		}
		if strings.TrimSpace(r.Model) == "" || strings.EqualFold(r.Model, modelCode) {
			return fmt.Errorf("range[%d] must name a different ordinary model", i)
		}
		distinct[strings.ToLower(r.Model)] = true
		end = r.Max
	}
	if end != 100 || len(distinct) < 2 {
		return fmt.Errorf("ranges must cover 0..100 with at least two distinct targets")
	}
	return nil
}
