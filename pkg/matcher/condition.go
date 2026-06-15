package matcher

import (
	"encoding/json"

	"gopkg.in/yaml.v3"
)

// TagCondition 匹配条件
type TagCondition struct {
	Type   string   `yaml:"type" json:"type"`       // e.g. "query", "header"
	OpType string   `yaml:"op_type" json:"op_type"` // e.g. "EQUAL", "IN"
	Key    string   `yaml:"key" json:"key"`
	Values []string `yaml:"values" json:"values"`
}

// UnmarshalJSON 自定义反序列化，兼容 opType 字段
func (c *TagCondition) UnmarshalJSON(data []byte) error {
	type Alias TagCondition
	aux := &struct {
		OpTypeCamel string `json:"opType"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.OpTypeCamel != "" {
		c.OpType = aux.OpTypeCamel
	}
	return nil
}

// UnmarshalYAML 自定义反序列化，兼容 opType 字段
func (c *TagCondition) UnmarshalYAML(value *yaml.Node) error {
	type Alias TagCondition
	aux := &struct {
		OpTypeCamel string `yaml:"opType"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := value.Decode(&aux); err != nil {
		return err
	}
	if aux.OpTypeCamel != "" {
		c.OpType = aux.OpTypeCamel
	}
	return nil
}

// MatchValues 比对提取的值是否满足 Condition 定义的操作符和通配符规则
func (c *TagCondition) MatchValues(actual []string) bool {
	if len(actual) == 0 {
		if c.OpType == "NOT_EQUAL" || c.OpType == "NOT_IN" {
			return true
		}
		return false
	}

	switch c.OpType {
	case "EQUAL", "IN":
		for _, a := range actual {
			for _, v := range c.Values {
				if MatchWildcard(v, a) {
					return true
				}
			}
		}
		return false
	case "NOT_EQUAL", "NOT_IN":
		for _, a := range actual {
			for _, v := range c.Values {
				if MatchWildcard(v, a) {
					return false
				}
			}
		}
		return true
	default:
		for _, a := range actual {
			for _, v := range c.Values {
				if MatchWildcard(v, a) {
					return true
				}
			}
		}
		return false
	}
}
