package policy

import "github.com/tokenlive/tokenlive-gateway/pkg/matcher"

// TaggingPolicy 染色打标策略：当 Conditions 命中时，执行 TaggingActions 将标签注入 GatewayContext.Tags
type TaggingPolicy struct {
	ID         string                  `yaml:"id" json:"id"`
	Name       string                  `yaml:"name" json:"name"`
	Version    int64                   `yaml:"version" json:"version"`
	Order      int                     `yaml:"order" json:"order"`
	Relation   string                  `yaml:"relation" json:"relation"` // "AND"（默认）, "OR"
	Conditions []*matcher.TagCondition `yaml:"conditions" json:"conditions"`
	Actions    []TaggingAction         `yaml:"actions" json:"actions"`
}

// TaggingAction 染色打标动作
type TaggingAction struct {
	Key   string `yaml:"key" json:"key"`     // 标签名
	Value string `yaml:"value" json:"value"` // 标签值，支持变量插值：${header.xxx}, ${query.xxx}, ${system.xxx}, ${tag.xxx}
}
