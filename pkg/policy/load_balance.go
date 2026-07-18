package policy

import "encoding/json"

// LoadBalancePolicy configures load balancing.
type LoadBalancePolicy struct {
	ID      string                 `yaml:"id" json:"id"`
	Name    string                 `yaml:"name" json:"name"`
	Type    string                 `yaml:"type" json:"type"` // e.g. "ROUND_ROBIN", "WEIGHTED", "STICKY"
	Version int64                  `yaml:"version" json:"version"`
	Params  map[string]interface{} `yaml:"params" json:"params"` // e.g. dynamic weights
}

// UnmarshalJSON accepts Params as a JSON object or a JSON-encoded string.
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
		var m map[string]interface{}
		if err := json.Unmarshal(aux.Params, &m); err == nil {
			l.Params = m
		} else {
			// Params may be a double-encoded JSON string
			var s string
			if err := json.Unmarshal(aux.Params, &s); err == nil && s != "" {
				var m2 map[string]interface{}
				if err := json.Unmarshal([]byte(s), &m2); err == nil {
					l.Params = m2
				}
			}
		}
	}
	return nil
}
