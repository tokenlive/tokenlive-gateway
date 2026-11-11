package core

import (
	"fmt"
	"github.com/tokenlive/tokenlive-gateway/pkg/compositerouting"
	"strings"
)

// SmartRoutingConfig is a versioned routing snapshot. Callers own returned clones.
type SmartRoutingConfig struct {
	SchemaVersion        int                 `json:"schema_version,omitempty" yaml:"schema_version" mapstructure:"schema_version"`
	Mode                 string              `json:"mode,omitempty" yaml:"mode" mapstructure:"mode"`
	PrimaryModel         string              `json:"primary_model,omitempty" yaml:"primary_model" mapstructure:"primary_model"`
	DefaultModel         string              `json:"default_model,omitempty" yaml:"default_model" mapstructure:"default_model"`
	Rules                []SmartRoutingRule  `json:"rules,omitempty" yaml:"rules" mapstructure:"rules"`
	Version              int64               `json:"version" yaml:"version" mapstructure:"version"`
	JudgeModel           string              `json:"judge_model" yaml:"judge_model" mapstructure:"judge_model"`
	JudgeTimeoutMs       int                 `json:"judge_timeout_ms" yaml:"judge_timeout_ms" mapstructure:"judge_timeout_ms"`
	JudgeMaxInputBytes   int                 `json:"judge_max_input_bytes" yaml:"judge_max_input_bytes" mapstructure:"judge_max_input_bytes"`
	JudgeMaxOutputTokens int                 `json:"judge_max_output_tokens" yaml:"judge_max_output_tokens" mapstructure:"judge_max_output_tokens"`
	Ranges               []SmartRoutingRange `json:"ranges" yaml:"ranges" mapstructure:"ranges"`
}

type SmartRoutingRule struct {
	ID           string                       `json:"id" yaml:"id" mapstructure:"id"`
	Name         string                       `json:"name" yaml:"name" mapstructure:"name"`
	Order        int                          `json:"order" yaml:"order" mapstructure:"order"`
	Enabled      bool                         `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	RelationType string                       `json:"relation_type" yaml:"relation_type" mapstructure:"relation_type"`
	Conditions   []compositerouting.Condition `json:"conditions" yaml:"conditions" mapstructure:"conditions"`
	TargetModel  string                       `json:"target_model" yaml:"target_model" mapstructure:"target_model"`
}

func (c *SmartRoutingConfig) RoutingMode() string {
	if c == nil || c.Mode == "" {
		return compositerouting.Difficulty
	}
	return c.Mode
}
func (c *SmartRoutingConfig) PlanConfig() compositerouting.Config {
	result := compositerouting.Config{Mode: c.RoutingMode(), Primary: c.PrimaryModel, Default: c.DefaultModel}
	for _, r := range c.Rules {
		result.Rules = append(result.Rules, compositerouting.Rule{ID: r.ID, Name: r.Name, Order: r.Order, Enabled: r.Enabled, RelationType: r.RelationType, Conditions: r.Conditions, Target: r.TargetModel})
	}
	return result
}
func (c *SmartRoutingConfig) ReferenceCodes() []string {
	var codes []string
	add := func(code string) {
		if code != "" {
			codes = append(codes, code)
		}
	}
	add(c.JudgeModel)
	add(c.PrimaryModel)
	add(c.DefaultModel)
	for _, r := range c.Rules {
		add(r.TargetModel)
	}
	for _, r := range c.Ranges {
		add(r.Model)
	}
	return codes
}

// SmartRoutingRange includes Min and excludes Max, except the final Max=100.
type SmartRoutingRange struct {
	Min   int    `json:"min" yaml:"min" mapstructure:"min"`
	Max   int    `json:"max" yaml:"max" mapstructure:"max"`
	Model string `json:"model" yaml:"model" mapstructure:"model"`
}

// ApplyDefaults fills omitted technical limits on an owned configuration.
func (c *SmartRoutingConfig) ApplyDefaults() {
	if c.RoutingMode() != compositerouting.Difficulty {
		return
	}
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
	result.Rules = append([]SmartRoutingRule(nil), c.Rules...)
	for i := range result.Rules {
		result.Rules[i].Conditions = append([]compositerouting.Condition(nil), c.Rules[i].Conditions...)
		for j := range result.Rules[i].Conditions {
			result.Rules[i].Conditions[j].Values = append([]string(nil), c.Rules[i].Conditions[j].Values...)
		}
	}
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
	if c.RoutingMode() != compositerouting.Difficulty {
		if c.SchemaVersion != 2 || c.JudgeModel != "" || len(c.Ranges) > 0 || c.JudgeTimeoutMs != 0 || c.JudgeMaxInputBytes != 0 || c.JudgeMaxOutputTokens != 0 {
			return fmt.Errorf("invalid format 2 configuration")
		}
		if err := c.PlanConfig().Validate(); err != nil {
			return err
		}
		for _, code := range c.ReferenceCodes() {
			if strings.EqualFold(code, modelCode) {
				return fmt.Errorf("self reference is forbidden")
			}
		}
		return nil
	}
	if c.SchemaVersion != 0 && c.SchemaVersion != 1 || c.PrimaryModel != "" || c.DefaultModel != "" || len(c.Rules) > 0 {
		return fmt.Errorf("invalid legacy format")
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
