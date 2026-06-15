package tagging

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func newTestGatewayContext(headers map[string]string, model, userID string) *core.GatewayContext {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, r)
	gctx.Model = model
	gctx.UserID = userID
	return gctx
}

func TestTaggingEngine_BasicTagging(t *testing.T) {
	engine := NewTaggingEngine()
	ctx := context.Background()

	t.Run("静态值打标", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:     "cost-tier",
				Order:    1,
				Relation: "OR",
				Conditions: []*matcher.TagCondition{
					{Type: "system", OpType: "IN", Key: "model", Values: []string{"gpt-4", "claude-opus"}},
				},
				Actions: []policy.TaggingAction{
					{Key: "cost_tier", Value: "high"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["cost_tier"] != "high" {
			t.Errorf("expected cost_tier=high, got %q", gctx.Tags["cost_tier"])
		}
	})

	t.Run("变量插值打标", func(t *testing.T) {
		gctx := newTestGatewayContext(map[string]string{"X-Tenant-ID": "tenant-abc"}, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:  "tenant-extract",
				Order: 1,
				Conditions: []*matcher.TagCondition{
					{Type: "header", OpType: "EQUAL", Key: "X-Tenant-ID", Values: []string{"*"}},
				},
				Actions: []policy.TaggingAction{
					{Key: "tenant", Value: "${header.X-Tenant-ID}"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["tenant"] != "tenant-abc" {
			t.Errorf("expected tenant=tenant-abc, got %q", gctx.Tags["tenant"])
		}
	})

	t.Run("条件不匹配时不打标", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-3.5-turbo", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:  "cost-tier",
				Order: 1,
				Conditions: []*matcher.TagCondition{
					{Type: "system", OpType: "IN", Key: "model", Values: []string{"gpt-4", "claude-opus"}},
				},
				Actions: []policy.TaggingAction{
					{Key: "cost_tier", Value: "high"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if _, exists := gctx.Tags["cost_tier"]; exists {
			t.Error("expected cost_tier tag not to be set")
		}
	})

	t.Run("空规则列表不打标", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		engine.Process(ctx, gctx, nil)

		if len(gctx.Tags) != 0 {
			t.Errorf("expected no tags, got %d", len(gctx.Tags))
		}
	})

	t.Run("空条件默认命中", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:  "always",
				Order: 1,
				Actions: []policy.TaggingAction{
					{Key: "env", Value: "production"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["env"] != "production" {
			t.Errorf("expected env=production, got %q", gctx.Tags["env"])
		}
	})
}

func TestTaggingEngine_AND_OR(t *testing.T) {
	engine := NewTaggingEngine()
	ctx := context.Background()

	t.Run("AND 关系：全部满足才打标", func(t *testing.T) {
		gctx := newTestGatewayContext(map[string]string{"X-Tenant-ID": "t1"}, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:     "and-rule",
				Order:    1,
				Relation: "AND",
				Conditions: []*matcher.TagCondition{
					{Type: "system", OpType: "EQUAL", Key: "model", Values: []string{"gpt-4"}},
					{Type: "header", OpType: "EQUAL", Key: "X-Tenant-ID", Values: []string{"t1"}},
				},
				Actions: []policy.TaggingAction{
					{Key: "vip", Value: "true"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["vip"] != "true" {
			t.Errorf("expected vip=true, got %q", gctx.Tags["vip"])
		}
	})

	t.Run("AND 关系：部分满足不打标", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:     "and-rule",
				Order:    1,
				Relation: "AND",
				Conditions: []*matcher.TagCondition{
					{Type: "system", OpType: "EQUAL", Key: "model", Values: []string{"gpt-4"}},
					{Type: "header", OpType: "EQUAL", Key: "X-Tenant-ID", Values: []string{"t1"}},
				},
				Actions: []policy.TaggingAction{
					{Key: "vip", Value: "true"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if _, exists := gctx.Tags["vip"]; exists {
			t.Error("expected vip tag not to be set")
		}
	})

	t.Run("OR 关系：任一满足即打标", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-3.5-turbo", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:     "or-rule",
				Order:    1,
				Relation: "OR",
				Conditions: []*matcher.TagCondition{
					{Type: "system", OpType: "IN", Key: "model", Values: []string{"gpt-4"}},
					{Type: "system", OpType: "IN", Key: "model", Values: []string{"gpt-3.5-turbo"}},
				},
				Actions: []policy.TaggingAction{
					{Key: "provider", Value: "openai"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["provider"] != "openai" {
			t.Errorf("expected provider=openai, got %q", gctx.Tags["provider"])
		}
	})
}

func TestTaggingEngine_Order(t *testing.T) {
	engine := NewTaggingEngine()
	ctx := context.Background()

	t.Run("规则按 Order 顺序执行，后打的标覆盖先打的", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:    "second",
				Order:   2,
				Actions: []policy.TaggingAction{{Key: "tier", Value: "premium"}},
			},
			{
				Name:    "first",
				Order:   1,
				Actions: []policy.TaggingAction{{Key: "tier", Value: "basic"}},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["tier"] != "premium" {
			t.Errorf("expected tier=premium (order 2 should override order 1), got %q", gctx.Tags["tier"])
		}
	})

	t.Run("链式打标：后续规则可读取前面打的标签", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:    "step1",
				Order:   1,
				Actions: []policy.TaggingAction{{Key: "cost_tier", Value: "high"}},
			},
			{
				Name:  "step2",
				Order: 2,
				Conditions: []*matcher.TagCondition{
					{Type: "tag", OpType: "EQUAL", Key: "cost_tier", Values: []string{"high"}},
				},
				Actions: []policy.TaggingAction{{Key: "priority", Value: "p0"}},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["cost_tier"] != "high" {
			t.Errorf("expected cost_tier=high, got %q", gctx.Tags["cost_tier"])
		}
		if gctx.Tags["priority"] != "p0" {
			t.Errorf("expected priority=p0, got %q", gctx.Tags["priority"])
		}
	})
}

func TestTaggingEngine_Interpolation(t *testing.T) {
	engine := NewTaggingEngine()
	ctx := context.Background()

	t.Run("system 变量插值", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user123")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:  "system-vars",
				Order: 1,
				Actions: []policy.TaggingAction{
					{Key: "model_tag", Value: "model=${system.model}"},
					{Key: "user_tag", Value: "uid=${system.user}"},
				},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["model_tag"] != "model=gpt-4" {
			t.Errorf("expected model_tag=model=gpt-4, got %q", gctx.Tags["model_tag"])
		}
		if gctx.Tags["user_tag"] != "uid=user123" {
			t.Errorf("expected user_tag=uid=user123, got %q", gctx.Tags["user_tag"])
		}
	})

	t.Run("tag 变量插值（读已打的标签）", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:    "step1",
				Order:   1,
				Actions: []policy.TaggingAction{{Key: "tier", Value: "gold"}},
			},
			{
				Name:    "step2",
				Order:   2,
				Actions: []policy.TaggingAction{{Key: "label", Value: "level=${tag.tier}"}},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["label"] != "level=gold" {
			t.Errorf("expected label=level=gold, got %q", gctx.Tags["label"])
		}
	})

	t.Run("纯静态值无正则开销", func(t *testing.T) {
		gctx := newTestGatewayContext(nil, "gpt-4", "user1")
		defer core.ReleaseContext(gctx)

		rules := []*policy.TaggingPolicy{
			{
				Name:    "static",
				Order:   1,
				Actions: []policy.TaggingAction{{Key: "env", Value: "production"}},
			},
		}

		engine.Process(ctx, gctx, rules)

		if gctx.Tags["env"] != "production" {
			t.Errorf("expected env=production, got %q", gctx.Tags["env"])
		}
	})
}

func TestTaggingEngine_NoPanicOnNilTags(t *testing.T) {
	engine := NewTaggingEngine()
	ctx := context.Background()

	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, r)
	gctx.Model = "gpt-4"
	gctx.Tags = nil // 模拟未初始化的 Tags
	defer core.ReleaseContext(gctx)

	rules := []*policy.TaggingPolicy{
		{
			Name:    "test",
			Order:   1,
			Actions: []policy.TaggingAction{{Key: "k", Value: "v"}},
		},
	}

	// 不应 panic
	engine.Process(ctx, gctx, rules)
}
