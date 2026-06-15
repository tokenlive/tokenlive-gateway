package matcher

import (
	"context"
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

type mockRequestContext struct {
	headers map[string][]string
	queries map[string][]string
	cookies map[string]string
	systems map[string]string
	tags    map[string]string
}

func (m *mockRequestContext) GetHeader(key string) []string {
	return m.headers[key]
}

func (m *mockRequestContext) GetQuery(key string) []string {
	return m.queries[key]
}

func (m *mockRequestContext) GetCookie(key string) string {
	return m.cookies[key]
}

func (m *mockRequestContext) GetSystemValue(key string) string {
	return m.systems[key]
}

func (m *mockRequestContext) GetTagValue(key string) string {
	if m.tags == nil {
		return ""
	}
	return m.tags[key]
}

func TestTagMatchers(t *testing.T) {
	reqCtx := &mockRequestContext{
		headers: map[string][]string{
			"x-tenant-id": {"tenant-100"},
		},
		queries: map[string][]string{
			"env": {"production"},
		},
		cookies: map[string]string{
			"session_id": "sess_xyz",
		},
		systems: map[string]string{
			"user":  "u_test",
			"model": "gpt-4",
		},
	}

	headerMatcher := DefaultTagMatcherFactory.Get("header")
	queryMatcher := DefaultTagMatcherFactory.Get("query")
	cookieMatcher := DefaultTagMatcherFactory.Get("cookie")
	systemMatcher := DefaultTagMatcherFactory.Get("system")

	if headerMatcher == nil || queryMatcher == nil || cookieMatcher == nil || systemMatcher == nil {
		t.Fatal("failed to get matchers from factory")
	}

	ctx := context.Background()

	t.Run("Header Matcher", func(t *testing.T) {
		cond := &TagCondition{Type: "header", OpType: "EQUAL", Key: "x-tenant-id", Values: []string{"tenant-100"}}
		if !headerMatcher.Match(ctx, cond, reqCtx) {
			t.Error("expected header matcher to pass")
		}

		cond2 := &TagCondition{Type: "header", OpType: "NOT_EQUAL", Key: "x-tenant-id", Values: []string{"tenant-200"}}
		if !headerMatcher.Match(ctx, cond2, reqCtx) {
			t.Error("expected header NOT_EQUAL matcher to pass")
		}
	})

	t.Run("Query Matcher", func(t *testing.T) {
		cond := &TagCondition{Type: "query", OpType: "IN", Key: "env", Values: []string{"production", "staging"}}
		if !queryMatcher.Match(ctx, cond, reqCtx) {
			t.Error("expected query matcher to pass")
		}
	})

	t.Run("Cookie Matcher", func(t *testing.T) {
		cond := &TagCondition{Type: "cookie", OpType: "EQUAL", Key: "session_id", Values: []string{"sess_xyz"}}
		if !cookieMatcher.Match(ctx, cond, reqCtx) {
			t.Error("expected cookie matcher to pass")
		}
	})

	t.Run("System Matcher", func(t *testing.T) {
		condUser := &TagCondition{Type: "system", OpType: "EQUAL", Key: "user", Values: []string{"u_test"}}
		if !systemMatcher.Match(ctx, condUser, reqCtx) {
			t.Error("expected system user matcher to pass")
		}

		condModel := &TagCondition{Type: "system", OpType: "EQUAL", Key: "model", Values: []string{"gpt-*"}}
		if !systemMatcher.Match(ctx, condModel, reqCtx) {
			t.Error("expected system model wildcard matcher to pass")
		}
	})
}

func TestMatchWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		val     string
		want    bool
	}{
		{"", "anything", true},
		{"*", "anything", true},
		{"exact", "exact", true},
		{"exact", "other", false},
		{"gpt-*", "gpt-4", true},
		{"gpt-*", "claude", false},
		{"*-turbo", "gpt-3.5-turbo", true},
		{"*-turbo", "gpt-4", false},
	}

	for _, tt := range tests {
		got := MatchWildcard(tt.pattern, tt.val)
		if got != tt.want {
			t.Errorf("MatchWildcard(%q, %q) = %v, want %v", tt.pattern, tt.val, got, tt.want)
		}
	}
}

func TestTagCondition_Unmarshal(t *testing.T) {
	importJSON := []byte(`{"type":"header","opType":"EQUAL","key":"x-tenant-id","values":["tenant-100"]}`)
	var cond1 TagCondition
	if err := json.Unmarshal(importJSON, &cond1); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	if cond1.OpType != "EQUAL" {
		t.Errorf("expected OpType 'EQUAL', got '%s'", cond1.OpType)
	}

	importYAML := `
type: header
opType: NOT_EQUAL
key: x-tenant-id
values:
  - tenant-200
`
	var cond2 TagCondition
	if err := yaml.Unmarshal([]byte(importYAML), &cond2); err != nil {
		t.Fatalf("failed to unmarshal YAML: %v", err)
	}
	if cond2.OpType != "NOT_EQUAL" {
		t.Errorf("expected OpType 'NOT_EQUAL', got '%s'", cond2.OpType)
	}
}
