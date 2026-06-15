package policy

// LoadBalancePolicy 负载均衡策略
type LoadBalancePolicy struct {
	Type    string                 `yaml:"type" json:"type"` // e.g. "ROUND_ROBIN", "WEIGHTED", "STICKY"
	Version int64                  `yaml:"version" json:"version"`
	Params  map[string]interface{} `yaml:"params" json:"params"` // 额外参数，用于动态权重等
}
