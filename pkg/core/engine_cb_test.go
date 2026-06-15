package core_test

import (
	"context"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/routers"

	"go.uber.org/zap"
)

func TestCircuitBreakerRouter_FiltersOpenEndpoints(t *testing.T) {
	cbManager := core.NewCircuitBreakerManager()

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}

	// 将 openai:gpt-4 服务级熔断设为 Open（记录 >=5 次失败）
	for i := 0; i < 5; i++ {
		cbManager.RecordRaw("openai:gpt-4", false, 0, 0, 0, 0)
	}

	router := routers.NewCircuitBreakerRouter(cbManager, false, zap.NewNop())
	gctx := &core.GatewayContext{Ctx: context.Background()}

	result := router.Route(gctx, []*core.Endpoint{ep1, ep2})

	// 两个 endpoint 共享同一个 serviceKey，服务级熔断后全部被过滤
	if len(result) != 0 {
		t.Errorf("expected 0 endpoints when service circuit is open, got %d", len(result))
	}
}

func TestCircuitBreakerRouter_FiltersInstanceOpen(t *testing.T) {
	cbManager := core.NewCircuitBreakerManager()

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}

	// 只将 ep-1 的实例级熔断设为 Open
	for i := 0; i < 5; i++ {
		cbManager.RecordRaw("ep-1", false, 0, 0, 0, 0)
	}

	router := routers.NewCircuitBreakerRouter(cbManager, false, zap.NewNop())
	gctx := &core.GatewayContext{Ctx: context.Background()}

	result := router.Route(gctx, []*core.Endpoint{ep1, ep2})

	// ep-1 被过滤，ep-2 保留
	if len(result) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(result))
	}
	if result[0].ID != "ep-2" {
		t.Errorf("expected ep-2 to survive, got %s", result[0].ID)
	}
}

func TestCircuitBreakerRouter_PassesClosedEndpoints(t *testing.T) {
	cbManager := core.NewCircuitBreakerManager()

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}

	// 记录少量失败，不足以触发熔断
	cbManager.RecordRaw("openai:gpt-4", false, 0, 0, 0, 0)
	cbManager.RecordRaw("ep-1", false, 0, 0, 0, 0)

	router := routers.NewCircuitBreakerRouter(cbManager, false, zap.NewNop())
	gctx := &core.GatewayContext{Ctx: context.Background()}

	result := router.Route(gctx, []*core.Endpoint{ep1, ep2})

	if len(result) != 2 {
		t.Errorf("expected 2 endpoints when circuits are closed, got %d", len(result))
	}
}

func TestCircuitBreakerRouter_AutoResetOnVersionChange(t *testing.T) {
	cbManager := core.NewCircuitBreakerManager()

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}

	// 设为 Open
	for i := 0; i < 5; i++ {
		cbManager.RecordRaw("openai:gpt-4", false, 0, 0, 0, 0)
	}

	router := routers.NewCircuitBreakerRouter(cbManager, false, zap.NewNop())
	gctx := &core.GatewayContext{Ctx: context.Background()}

	// 模拟加载 Version = 1 的策略，它依然保留 Open 状态
	gctx.Policy = &policy.Policy{
		CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
			{
				Name:    "cb-test",
				Level:   "SERVICE",
				Version: 1,
			},
		},
	}

	// 第一次调用：由于没有更改版本，依然被熔断拦截（返回 0 个端点）
	res1 := router.Route(gctx, []*core.Endpoint{ep1, ep2})
	if len(res1) != 0 {
		t.Errorf("expected 0 endpoints before version change, got %d", len(res1))
	}

	// 模拟在运行期把该策略的 Version 递增为 2
	gctx.Policy.CircuitBreakPolicies[0].Version = 2

	// 第二次调用：检测到版本从 1 变为 2，自动重置熔断器，不再拦截
	res2 := router.Route(gctx, []*core.Endpoint{ep1, ep2})
	if len(res2) != 2 {
		t.Errorf("expected 2 endpoints after version change due to auto-reset, got %d", len(res2))
	}

	// 检查底层熔断器状态，确认已被成功复位回 Closed
	if cbManager.GetState("openai:gpt-4") != core.CircuitClosed {
		t.Error("expected state of service circuit breaker to be reset to Closed")
	}
}
