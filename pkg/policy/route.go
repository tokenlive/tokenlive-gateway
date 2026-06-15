package policy

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
)

// RoutePolicy 路由策略
type RoutePolicy struct {
	Name     string     `yaml:"name" json:"name"`
	Version  int64      `yaml:"version" json:"version"`
	Order    int        `yaml:"order" json:"order"`
	TagRules []*TagRule `yaml:"details" json:"details"`
}

// TagRule 路由标签匹配规则
type TagRule struct {
	Order        int                     `yaml:"order" json:"order"`
	RelationType string                  `yaml:"relation_type" json:"relation_type"` // "AND", "OR"
	Conditions   []*matcher.TagCondition `yaml:"conditions" json:"conditions"`
	Destinations []*Destination          `yaml:"destinations" json:"destinations"`
}

// Destination 路由目标
type Destination struct {
	Weight       int                     `yaml:"weight" json:"weight"`
	RelationType string                  `yaml:"relation_type" json:"relation_type"` // "AND", "OR"
	Conditions   []*matcher.TagCondition `yaml:"conditions" json:"conditions"`
}
