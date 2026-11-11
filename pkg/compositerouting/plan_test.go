package compositerouting

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectionOrderDefaultAndMissingMetadata(t *testing.T) {
	c := Config{Mode: Rules, Default: "default", Rules: []Rule{
		{ID: "later", Order: 20, Enabled: true, RelationType: "AND", Target: "later", Conditions: []Condition{{Type: "TAG", Key: "task", OpType: "IN", Values: []string{"code", "chat"}}}},
		{ID: "first", Order: 10, Enabled: true, RelationType: "AND", Target: "code", Conditions: []Condition{{Type: "TAG", Key: "task", OpType: "EQUAL", Values: []string{"code"}}}},
	}}
	p, err := Select(c, Input{Tags: map[string]string{"task": "code"}})
	require.NoError(t, err)
	require.Equal(t, "first", p.MatchedRuleID)
	require.Equal(t, "later", c.Rules[0].ID, "selection must not reorder a shared configuration")
	c.Rules[1].Enabled = false
	p, err = Select(c, Input{Tags: map[string]string{"task": "code"}})
	require.NoError(t, err)
	require.Equal(t, "later", p.Target)
	p, err = Select(c, Input{})
	require.NoError(t, err)
	require.Equal(t, "default", p.Target)
	c.Rules[0].Conditions[0].OpType = "NOT_IN"
	p, err = Select(c, Input{})
	require.NoError(t, err)
	require.Equal(t, "default", p.Target, "missing metadata must not satisfy a negative match")
}

func TestHeaderWhitelistAndCaseAmbiguity(t *testing.T) {
	c := Config{Mode: Rules, Default: "default", Rules: []Rule{{ID: "header", Order: 1, Enabled: true, RelationType: "OR", Target: "code", Conditions: []Condition{{Type: "HEADER", Key: "X-TokenLive-Task", OpType: "PREFIX", Values: []string{"code"}}}}}}
	p, err := Select(c, Input{Headers: map[string]string{"x-tokenlive-task": "code-review"}})
	require.NoError(t, err)
	require.Equal(t, "code", p.Target)
	_, err = Select(c, Input{Headers: map[string]string{"x-tokenlive-task": "code", "X-TokenLive-Task": "chat"}})
	require.Error(t, err)
	for _, header := range []string{"Authorization", "Cookie", "X-API-Key", "X-User-ID", "X-Tenant"} {
		c.Rules[0].Conditions[0].Key = header
		require.Error(t, c.Validate(), header)
	}
}

func TestRejectAmbiguousAndMixedConfiguration(t *testing.T) {
	for _, c := range []Config{
		{Mode: Priority},
		{Mode: Priority, Primary: "a", Default: "b"},
		{Mode: "other", Primary: "a"},
		{Mode: Rules, Default: "a", Rules: []Rule{{ID: "empty", Target: "b", RelationType: "AND"}}},
	} {
		require.Error(t, c.Validate())
	}
	p, err := Select(Config{Mode: Priority, Primary: "only"}, Input{})
	require.NoError(t, err)
	require.Equal(t, "only", p.Target)
}
