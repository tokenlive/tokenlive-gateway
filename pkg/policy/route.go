package policy

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
)

// RoutePolicy configures request routing.
type RoutePolicy struct {
	ID       string     `yaml:"id" json:"id"`
	Name     string     `yaml:"name" json:"name"`
	Version  int64      `yaml:"version" json:"version"`
	Order    int        `yaml:"order" json:"order"`
	TagRules []*TagRule `yaml:"details" json:"details"`
}

// TagRule is a tag-match rule with destinations.
type TagRule struct {
	Order        int                     `yaml:"order" json:"order"`
	RelationType string                  `yaml:"relation_type" json:"relation_type"` // "AND", "OR"
	Conditions   []*matcher.TagCondition `yaml:"conditions" json:"conditions"`
	Destinations []*Destination          `yaml:"destinations" json:"destinations"`
}

// Destination is a weighted routing target.
type Destination struct {
	Weight       int                     `yaml:"weight" json:"weight"`
	RelationType string                  `yaml:"relation_type" json:"relation_type"` // "AND", "OR"
	Conditions   []*matcher.TagCondition `yaml:"conditions" json:"conditions"`
}
