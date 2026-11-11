package core

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/compositerouting"
)

func TestCompositeConfigFormatsAndDeepClone(t *testing.T) {
	c := &SmartRoutingConfig{SchemaVersion: 2, Version: 1, Mode: compositerouting.Rules, DefaultModel: "a", Rules: []SmartRoutingRule{{ID: "rule", Order: 10, Enabled: true, RelationType: "AND", TargetModel: "b", Conditions: []compositerouting.Condition{{Type: "TAG", Key: "task", OpType: "EQUAL", Values: []string{"code"}}}}}}
	c.ApplyDefaults()
	if err := c.Validate("smart"); err != nil {
		t.Fatal(err)
	}
	if c.JudgeTimeoutMs != 0 {
		t.Fatal("judge defaults applied to a deterministic router")
	}
	clone := c.Clone()
	clone.Rules[0].Conditions[0].Values[0] = "chat"
	if c.Rules[0].Conditions[0].Values[0] != "code" {
		t.Fatal("shared mutable condition values")
	}
	for _, invalid := range []*SmartRoutingConfig{
		{SchemaVersion: 3, Version: 1, Mode: compositerouting.Priority, PrimaryModel: "a"},
		{SchemaVersion: 2, Version: 1, Mode: compositerouting.Priority, PrimaryModel: "smart"},
		{SchemaVersion: 2, Version: 1, Mode: compositerouting.Priority, PrimaryModel: "a", JudgeModel: "b"},
	} {
		if err := invalid.Validate("smart"); err == nil {
			t.Fatalf("accepted invalid config: %+v", invalid)
		}
	}
}
