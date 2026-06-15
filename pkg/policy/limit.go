package policy

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
)

// LimitPolicy 限流策略
type LimitPolicy struct {
	ID             string                  `yaml:"id" json:"id"`
	Name           string                  `yaml:"name" json:"name"`
	Version        int64                   `yaml:"version" json:"version"`
	Type           string                  `yaml:"type" json:"type"` // e.g. "request", "token", "cost"
	SlidingWindows []*SlidingWindow        `yaml:"sliding_windows" json:"sliding_windows"`
	MaxWaitMs      int                     `yaml:"max_wait_ms" json:"max_wait_ms"`
	RelationType   string                  `yaml:"relation_type" json:"relation_type"` // "AND", "OR"
	Conditions     []*matcher.TagCondition `yaml:"conditions" json:"conditions"`
	Estimator      *EstimatorConfig        `yaml:"estimator" json:"estimator"`
}

// EstimatorConfig 估算器配置
type EstimatorConfig struct {
	Type  string  `yaml:"type" json:"type"`   // e.g. "length_ratio", "tiktoken"
	Ratio float64 `yaml:"ratio" json:"ratio"` // 字符换算 token 比例
}

// SlidingWindow 滑动窗口限制
type SlidingWindow struct {
	Threshold      int64    `yaml:"threshold" json:"threshold"`
	TimeWindowInMs int64    `yaml:"time_window_in_ms" json:"time_window_in_ms"`
	BurstRatio     *float64 `yaml:"burst_ratio,omitempty" json:"burst_ratio,omitempty"` // 令牌桶专属：爆发系数
}
