package policy

import "github.com/tokenlive/tokenlive-gateway/pkg/matcher"

// TaggingPolicy injects tags into GatewayContext.Tags when Conditions match.
type TaggingPolicy struct {
	ID         string                  `yaml:"id" json:"id"`
	Name       string                  `yaml:"name" json:"name"`
	Version    int64                   `yaml:"version" json:"version"`
	Order      int                     `yaml:"order" json:"order"`
	Relation   string                  `yaml:"relation" json:"relation"` // "AND" (default), "OR"
	Conditions []*matcher.TagCondition `yaml:"conditions" json:"conditions"`
	Actions    []TaggingAction         `yaml:"actions" json:"actions"`
}

// TaggingAction applies a tag or header mutation.
type TaggingAction struct {
	Type  string `yaml:"type" json:"type"`   // e.g. TAG, REQ_HEADER, RSP_HEADER
	Key   string `yaml:"key" json:"key"`
	Value string `yaml:"value" json:"value"` // supports ${header.xxx}, ${query.xxx}, ${system.xxx}, ${tag.xxx}
}
