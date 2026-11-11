// Package compositerouting contains deterministic model selection, without I/O,
// authentication or quota side effects. Admin and Gateway must use this planner.
package compositerouting

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

const (
	Priority   = "priority"
	Rules      = "rules"
	Difficulty = "difficulty"
)

// Condition deliberately supports only structured business metadata.
type Condition struct {
	Type   string   `json:"type" yaml:"type" mapstructure:"type"`
	Key    string   `json:"key" yaml:"key" mapstructure:"key"`
	OpType string   `json:"op_type" yaml:"op_type" mapstructure:"op_type"`
	Values []string `json:"values" yaml:"values" mapstructure:"values"`
}

type Rule struct {
	ID           string      `json:"id" yaml:"id" mapstructure:"id"`
	Name         string      `json:"name" yaml:"name" mapstructure:"name"`
	Order        int         `json:"order" yaml:"order" mapstructure:"order"`
	Enabled      bool        `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	RelationType string      `json:"relation_type" yaml:"relation_type" mapstructure:"relation_type"`
	Conditions   []Condition `json:"conditions" yaml:"conditions" mapstructure:"conditions"`
	Target       string      `json:"target" yaml:"target" mapstructure:"target"`
}

type Config struct {
	Mode    string
	Primary string
	Default string
	Rules   []Rule
}

type Input struct {
	Tags    map[string]string `json:"tags" yaml:"tags" mapstructure:"tags"`
	Headers map[string]string `json:"headers" yaml:"headers" mapstructure:"headers"`
}

type Plan struct {
	Mode          string `json:"mode" yaml:"mode" mapstructure:"mode"`
	MatchedRuleID string `json:"matched_rule_id,omitempty" yaml:"matched_rule_id,omitempty" mapstructure:"matched_rule_id,omitempty"`
	Target        string `json:"target" yaml:"target" mapstructure:"target"`
	Reason        string `json:"reason" yaml:"reason" mapstructure:"reason"`
}

// AllowedHeader excludes credentials and identity headers by construction.
func AllowedHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "X-Tokenlive-Task", "X-Tokenlive-Tier":
		return true
	}
	return false
}

func (c Config) Validate() error {
	switch c.Mode {
	case Priority:
		if strings.TrimSpace(c.Primary) == "" || c.Default != "" || len(c.Rules) != 0 {
			return fmt.Errorf("主备模式需要主模型，且不能混入规则模式字段")
		}
	case Rules:
		if strings.TrimSpace(c.Default) == "" || c.Primary != "" || len(c.Rules) > 100 {
			return fmt.Errorf("规则模式需要默认模型，不能配置主模型，规则最多 100 条")
		}
		ids, orders := map[string]bool{}, map[int]bool{}
		for _, r := range c.Rules {
			if strings.TrimSpace(r.ID) == "" || ids[r.ID] || r.Order < 0 || orders[r.Order] || strings.TrimSpace(r.Target) == "" {
				return fmt.Errorf("规则 ID、目标模型必填，ID 和非负排序值必须唯一")
			}
			ids[r.ID], orders[r.Order] = true, true
			if r.RelationType != "AND" && r.RelationType != "OR" || len(r.Conditions) == 0 || len(r.Conditions) > 20 {
				return fmt.Errorf("规则需要 AND/OR 与 1–20 个有效条件；未命中使用默认模型")
			}
			for _, condition := range r.Conditions {
				if err := condition.Validate(); err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("不支持的确定性选模模式 %q", c.Mode)
	}
	return nil
}

func (c Condition) Validate() error {
	if strings.TrimSpace(c.Key) == "" || len(c.Key) > 128 || len(c.Values) == 0 || len(c.Values) > 50 {
		return fmt.Errorf("条件需要有效字段名与 1–50 个匹配值")
	}
	if c.Type != "TAG" && (c.Type != "HEADER" || !AllowedHeader(c.Key)) {
		return fmt.Errorf("条件只支持标签和 X-TokenLive-Task / X-TokenLive-Tier 业务 Header")
	}
	switch c.OpType {
	case "EQUAL", "NOT_EQUAL", "PREFIX":
		if len(c.Values) != 1 {
			return fmt.Errorf("该操作符只支持一个匹配值")
		}
	case "IN", "NOT_IN":
	default:
		return fmt.Errorf("不支持的条件操作符 %q", c.OpType)
	}
	for _, v := range c.Values {
		if strings.TrimSpace(v) == "" || len(v) > 1024 {
			return fmt.Errorf("匹配值不能为空且不能超过 1024 字节")
		}
	}
	return nil
}

// Select returns only the initial logical target. It never guesses availability,
// grants access or runs fallback; those belong to the authenticated executor.
func Select(c Config, in Input) (Plan, error) {
	if err := c.Validate(); err != nil {
		return Plan{}, err
	}
	p := Plan{Mode: c.Mode, Target: c.Primary, Reason: "primary"}
	if c.Mode == Priority {
		return p, nil
	}
	rules := append([]Rule(nil), c.Rules...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].Order < rules[j].Order })
	headers := make(map[string]string, len(in.Headers))
	for k, v := range in.Headers {
		key := http.CanonicalHeaderKey(k)
		if _, exists := headers[key]; exists {
			return Plan{}, fmt.Errorf("Header 名称不能大小写重复")
		}
		headers[key] = v
	}
	for _, r := range rules {
		if !r.Enabled {
			continue
		}
		matched := r.RelationType == "AND"
		for _, cond := range r.Conditions {
			value, exists := in.Tags[cond.Key]
			if cond.Type == "HEADER" {
				value, exists = headers[http.CanonicalHeaderKey(cond.Key)]
			}
			m := exists && matches(value, cond)
			if r.RelationType == "AND" {
				matched = matched && m
			} else {
				matched = matched || m
			}
		}
		if matched {
			return Plan{Mode: c.Mode, MatchedRuleID: r.ID, Target: r.Target, Reason: "rule_matched"}, nil
		}
	}
	return Plan{Mode: c.Mode, Target: c.Default, Reason: "default"}, nil
}

func matches(value string, c Condition) bool {
	if c.OpType == "PREFIX" {
		return strings.HasPrefix(value, c.Values[0])
	}
	found := false
	for _, v := range c.Values {
		found = found || value == v
	}
	if c.OpType == "NOT_EQUAL" || c.OpType == "NOT_IN" {
		return !found
	}
	return found
}
