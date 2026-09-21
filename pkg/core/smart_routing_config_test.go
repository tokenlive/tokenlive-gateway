package core

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSmartRoutingConfigValidation(t *testing.T) {
	valid := SmartRoutingConfig{
		Version: 1, JudgeModel: "judge", JudgeTimeoutMs: 5000,
		JudgeMaxInputBytes: 65536, JudgeMaxOutputTokens: 256,
		Ranges: []SmartRoutingRange{{Min: 0, Max: 40, Model: "small"}, {Min: 40, Max: 100, Model: "large"}},
	}
	require.NoError(t, valid.Validate("smart"))
	tests := []struct {
		name   string
		change func(*SmartRoutingConfig)
	}{
		{"zero version", func(c *SmartRoutingConfig) { c.Version = 0 }},
		{"self judge", func(c *SmartRoutingConfig) { c.JudgeModel = "smart" }},
		{"empty judge", func(c *SmartRoutingConfig) { c.JudgeModel = "" }},
		{"gap", func(c *SmartRoutingConfig) { c.Ranges[1].Min = 41 }},
		{"overlap", func(c *SmartRoutingConfig) { c.Ranges[1].Min = 39 }},
		{"missing zero", func(c *SmartRoutingConfig) { c.Ranges[0].Min = 1 }},
		{"missing hundred", func(c *SmartRoutingConfig) { c.Ranges[1].Max = 99 }},
		{"empty interval", func(c *SmartRoutingConfig) { c.Ranges[0].Max = 0 }},
		{"self child", func(c *SmartRoutingConfig) { c.Ranges[0].Model = "smart" }},
		{"one distinct child", func(c *SmartRoutingConfig) { c.Ranges[1].Model = "small" }},
		{"empty child", func(c *SmartRoutingConfig) { c.Ranges[1].Model = "" }},
		{"negative timeout", func(c *SmartRoutingConfig) { c.JudgeTimeoutMs = -1 }},
		{"negative input bound", func(c *SmartRoutingConfig) { c.JudgeMaxInputBytes = -1 }},
		{"negative output bound", func(c *SmartRoutingConfig) { c.JudgeMaxOutputTokens = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid.Clone()
			tt.change(cfg)
			require.Error(t, cfg.Validate("smart"))
		})
	}
}

func TestSmartRoutingConfigCloneDefaultsAndIsolation(t *testing.T) {
	cfg := &SmartRoutingConfig{Version: 1, JudgeModel: "judge",
		Ranges: []SmartRoutingRange{{Min: 0, Max: 50, Model: "small"}, {Min: 50, Max: 100, Model: "large"}}}
	snapshot := cfg.Clone()
	snapshot.ApplyDefaults()
	require.Equal(t, 5000, snapshot.JudgeTimeoutMs)
	require.Equal(t, 65536, snapshot.JudgeMaxInputBytes)
	require.Equal(t, 256, snapshot.JudgeMaxOutputTokens)
	cfg.Ranges[0].Model = "mutated"
	require.Equal(t, "small", snapshot.Ranges[0].Model)
	require.Zero(t, cfg.JudgeTimeoutMs)
	require.NoError(t, snapshot.Validate("smart"))
	var fractional SmartRoutingConfig
	require.Error(t, json.Unmarshal([]byte(`{"ranges":[{"min":0.5,"max":100,"model":"small"}]}`), &fractional))
}
