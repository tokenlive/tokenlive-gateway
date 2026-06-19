package policy

import "encoding/json"

// LoadBalancePolicy 负载均衡策略
type LoadBalancePolicy struct {
	ID      string                 `yaml:"id" json:"id"`
	Name    string                 `yaml:"name" json:"name"`
	Type    string                 `yaml:"type" json:"type"` // e.g. "ROUND_ROBIN", "WEIGHTED", "STICKY"
	Version int64                  `yaml:"version" json:"version"`
	Params  map[string]interface{} `yaml:"params" json:"params"` // 额外参数，用于动态权重等
}

// UnmarshalJSON 自定义反序列化，兼容 Params 是 JSON 字符串或普通 JSON 对象的两种情况
func (l *LoadBalancePolicy) UnmarshalJSON(data []byte) error {
	type Alias LoadBalancePolicy
	aux := &struct {
		Params json.RawMessage `json:"params"`
		*Alias
	}{
		Alias: (*Alias)(l),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if len(aux.Params) > 0 {
		// 1. 尝试直接解析为 map[string]interface{}
		var m map[string]interface{}
		if err := json.Unmarshal(aux.Params, &m); err == nil {
			l.Params = m
		} else {
			// 2. 如果直接解析失败，说明它可能是一个被双引号包裹的 JSON 字符串，先解析出 string 结构
			var s string
			if err := json.Unmarshal(aux.Params, &s); err == nil && s != "" {
				// 再将字符串内容解析为 map
				var m2 map[string]interface{}
				if err := json.Unmarshal([]byte(s), &m2); err == nil {
					l.Params = m2
				}
			}
		}
	}
	return nil
}
