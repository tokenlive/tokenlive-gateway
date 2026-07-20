package matcher

import (
	"encoding/json"

	"gopkg.in/yaml.v3"
)

// TagCondition is a match condition.
type TagCondition struct {
	Type   string   `yaml:"type" json:"type"`       // e.g. "query", "header"
	OpType string   `yaml:"op_type" json:"op_type"` // e.g. "EQUAL", "IN"
	Key    string   `yaml:"key" json:"key"`
	Values []string `yaml:"values" json:"values"`
}

// UnmarshalJSON accepts opType alias.
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

// UnmarshalYAML accepts opType alias.
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

// MatchValues checks extracted values against operator and wildcards.
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
