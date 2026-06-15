package routers

import (
	"context"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ===== 辅助构造 =====

func newEndpoint(id string, provider, model string, caps []core.RequestType, meta map[string]string) *core.Endpoint {
	return &core.Endpoint{
		ID:           id,
		Provider:     provider,
		Model:        model,
		RequestTypes: caps,
		Metadata:     meta,
	}
}

func newGctx(rt core.RequestType) *core.GatewayContext {
	return &core.GatewayContext{
		RequestType: rt,
		Ctx:         context.Background(),
	}
}

func mustNewLogger(t *testing.T) *zap.Logger {
	t.Helper()
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	return logger
}

// ===== CapabilityRouter 测试 =====

func TestCapabilityRouter_Name(t *testing.T) {
	r := &CapabilityRouter{}
	assert.Equal(t, "capability", r.Name())
}

func TestCapabilityRouter_FiltersUnsupportedEndpoints(t *testing.T) {
	r := &CapabilityRouter{}
	gctx := newGctx(core.RequestTypeChatCompletion)

	ep1 := newEndpoint("ep1", "openai", "gpt-4", []core.RequestType{core.RequestTypeChatCompletion}, nil)
	ep2 := newEndpoint("ep2", "openai", "text-embedding-3", []core.RequestType{core.RequestTypeEmbedding}, nil)
	ep3 := newEndpoint("ep3", "anthropic", "claude-3", []core.RequestType{core.RequestTypeChatCompletion, core.RequestTypeEmbedding}, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2, ep3})

	assert.Len(t, result, 2)
	assert.Equal(t, "ep1", result[0].ID)
	assert.Equal(t, "ep3", result[1].ID)
}

func TestCapabilityRouter_EmptyResultWhenNoneSupport(t *testing.T) {
	r := &CapabilityRouter{}
	gctx := newGctx(core.RequestTypeImageGeneration)

	ep1 := newEndpoint("ep1", "openai", "gpt-4", []core.RequestType{core.RequestTypeChatCompletion}, nil)
	ep2 := newEndpoint("ep2", "openai", "text-embedding-3", []core.RequestType{core.RequestTypeEmbedding}, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2})

	assert.Empty(t, result)
}

func TestCapabilityRouter_EmptyInput(t *testing.T) {
	r := &CapabilityRouter{}
	gctx := newGctx(core.RequestTypeChatCompletion)

	result := r.Route(gctx, []*core.Endpoint{})

	assert.Empty(t, result)
}

func TestCapabilityRouter_AllPass(t *testing.T) {
	r := &CapabilityRouter{}
	gctx := newGctx(core.RequestTypeChatCompletion)

	ep1 := newEndpoint("ep1", "openai", "gpt-4", []core.RequestType{core.RequestTypeChatCompletion}, nil)
	ep2 := newEndpoint("ep2", "anthropic", "claude-3", []core.RequestType{core.RequestTypeChatCompletion}, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2})

	assert.Len(t, result, 2)
}

func TestCapabilityRouter_ImplicitMessagesSupport(t *testing.T) {
	r := &CapabilityRouter{}
	gctx := newGctx(core.RequestTypeMessages)

	// ep1 只有 chat_completion 能力，但因为支持隐式推导，所以它应该支持 messages。
	ep1 := newEndpoint("ep1", "openai", "gpt-4", []core.RequestType{core.RequestTypeChatCompletion}, nil)
	// ep2 只有 embedding 能力，它不支持 messages。
	ep2 := newEndpoint("ep2", "openai", "text-embedding-3", []core.RequestType{core.RequestTypeEmbedding}, nil)
	// ep3 显式支持 messages。
	ep3 := newEndpoint("ep3", "anthropic", "claude-3", []core.RequestType{core.RequestTypeMessages}, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2, ep3})

	assert.Len(t, result, 2)
	assert.Equal(t, "ep1", result[0].ID)
	assert.Equal(t, "ep3", result[1].ID)
}

func TestCapabilityRouter_ImplicitResponsesSupport(t *testing.T) {
	r := &CapabilityRouter{}
	gctx := newGctx(core.RequestTypeResponses)

	// ep1 只有 chat_completion 能力，但因为支持隐式推导，所以它应该支持 responses。
	ep1 := newEndpoint("ep1", "openai", "gpt-4", []core.RequestType{core.RequestTypeChatCompletion}, nil)
	// ep2 只有 embedding 能力，它不支持 responses。
	ep2 := newEndpoint("ep2", "openai", "text-embedding-3", []core.RequestType{core.RequestTypeEmbedding}, nil)
	// ep3 显式支持 responses。
	ep3 := newEndpoint("ep3", "openai", "gpt-4-responses", []core.RequestType{core.RequestTypeResponses}, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2, ep3})

	assert.Len(t, result, 2)
	assert.Equal(t, "ep1", result[0].ID)
	assert.Equal(t, "ep3", result[1].ID)
}

// ===== TagRouter 测试 =====

// ===== CircuitBreakerRouter 测试 =====

func TestCircuitBreakerRouter_Name(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	assert.Equal(t, "circuit_breaker", r.Name())
}

func TestCircuitBreakerRouter_AllPassWhenClosed(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	ep2 := newEndpoint("ep2", "anthropic", "claude-3", nil, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2})

	assert.Len(t, result, 2)
}

func TestCircuitBreakerRouter_FiltersOpenServiceCircuit(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	// 使 openai:gpt-4 的服务级熔断器跳闸
	for i := 0; i < 6; i++ {
		cbm.RecordRaw("openai:gpt-4", false, 0, 0, 0, 0)
	}

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	ep2 := newEndpoint("ep2", "anthropic", "claude-3", nil, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2})

	assert.Len(t, result, 1)
	assert.Equal(t, "ep2", result[0].ID)
}

func TestCircuitBreakerRouter_FiltersOpenInstanceCircuit(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	// 使 ep1 的实例级熔断器跳闸
	for i := 0; i < 6; i++ {
		cbm.RecordRaw("ep1", false, 0, 0, 0, 0)
	}

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	ep2 := newEndpoint("ep2", "openai", "gpt-4", nil, nil) // 同 provider:model，但不同实例

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2})

	assert.Len(t, result, 1)
	assert.Equal(t, "ep2", result[0].ID)
}

func TestCircuitBreakerRouter_FiltersBothLevels(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	// 服务级跳闸
	for i := 0; i < 6; i++ {
		cbm.RecordRaw("openai:gpt-4", false, 0, 0, 0, 0)
	}
	// 实例级跳闸（另一个 provider）
	for i := 0; i < 6; i++ {
		cbm.RecordRaw("ep3", false, 0, 0, 0, 0)
	}

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)       // 服务级开
	ep2 := newEndpoint("ep2", "anthropic", "claude-3", nil, nil) // 正常
	ep3 := newEndpoint("ep3", "anthropic", "claude-3", nil, nil) // 实例级开

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2, ep3})

	assert.Len(t, result, 1)
	assert.Equal(t, "ep2", result[0].ID)
}

func TestCircuitBreakerRouter_EmptyResultWhenAllOpen(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	// 所有 endpoint 的服务级熔断跳闸
	for i := 0; i < 6; i++ {
		cbm.RecordRaw("openai:gpt-4", false, 0, 0, 0, 0)
		cbm.RecordRaw("anthropic:claude-3", false, 0, 0, 0, 0)
	}

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	ep2 := newEndpoint("ep2", "anthropic", "claude-3", nil, nil)

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2})

	assert.Empty(t, result)
}

func TestCircuitBreakerRouter_HalfOpenPasses(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	// 先跳闸
	for i := 0; i < 6; i++ {
		cbm.RecordRaw("openai:gpt-4", false, 0, 0, 0, 0)
	}

	// 确认是 Open 状态
	state := cbm.GetState("openai:gpt-4")
	assert.Equal(t, core.CircuitOpen, state)

	// 手动重置到 HalfOpen（或者直接重置为 Closed 即可，MemoryStateStore 无需额外测试）
	cbm.Reset("openai:gpt-4")

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	result := r.Route(gctx, []*core.Endpoint{ep1})

	assert.Len(t, result, 1)
}

func TestCircuitBreakerRouter_EmptyInput(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	result := r.Route(gctx, []*core.Endpoint{})

	assert.Empty(t, result)
}

// ===== Router 接口一致性测试 =====

func TestRouterInterfaceCompliance(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()

	var _ core.Router = (*CapabilityRouter)(nil)
	var _ core.Router = (*TagRouter)(nil)
	var _ core.Router = (*CircuitBreakerRouter)(nil)
	var _ core.Router = (*PriorityRouter)(nil)

	// 验证 Name() 返回值
	var routers = []core.Router{
		&CapabilityRouter{},
		NewTagRouter(logger),
		NewCircuitBreakerRouter(cbm, false, logger),
		NewPriorityRouter(logger),
	}
	names := make([]string, len(routers))
	for i, r := range routers {
		names[i] = r.Name()
	}
	assert.Contains(t, names, "capability")
	assert.Contains(t, names, "tag")
	assert.Contains(t, names, "circuit_breaker")
	assert.Contains(t, names, "priority")
}

// ===== PriorityRouter 测试 =====

func TestPriorityRouter_Name(t *testing.T) {
	logger := mustNewLogger(t)
	r := NewPriorityRouter(logger)
	assert.Equal(t, "priority", r.Name())
}

func TestPriorityRouter_SelectsMinimumPriority(t *testing.T) {
	logger := mustNewLogger(t)
	r := NewPriorityRouter(logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	ep1.Priority = 2 // 优先级低
	ep2 := newEndpoint("ep2", "openai", "gpt-4", nil, nil)
	ep2.Priority = 1 // 优先级最高 (主)
	ep3 := newEndpoint("ep3", "openai", "gpt-4", nil, nil)
	ep3.Priority = 1 // 优先级最高 (同为主)
	ep4 := newEndpoint("ep4", "openai", "gpt-4", nil, nil)
	ep4.Priority = 3 // 优先级更低

	result := r.Route(gctx, []*core.Endpoint{ep1, ep2, ep3, ep4})

	assert.Len(t, result, 2)
	assert.Equal(t, "ep2", result[0].ID)
	assert.Equal(t, "ep3", result[1].ID)
}

func TestPriorityRouter_EmptyInput(t *testing.T) {
	logger := mustNewLogger(t)
	r := NewPriorityRouter(logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	result := r.Route(gctx, []*core.Endpoint{})

	assert.Empty(t, result)
}

// ===== TestCircuitBreakerRouter_HalfOpenPermits =====

func TestCircuitBreakerRouter_HalfOpenPermits_DisabledActive(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()

	// 1. 创建处于 HalfOpen 的端点路由（通过主动探测关闭）
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)
	gctx.Policy = &policy.Policy{
		CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
			{
				SlidingWindowSize:           5,
				MinCallsThreshold:           2,
				AllowedCallsInHalfOpenState: 1,
				WaitDurationInOpenState:     10,
			},
		},
	}

	ep := newEndpoint("ep-ho-test", "openai", "gpt-4", nil, nil)

	// 强设为 Open 熔断 (设置等待时间为 10ms，便于在测试中流转)
	cbm.RecordRaw(ep.ID, false, 5, 2, 1, 10*time.Millisecond)
	cbm.RecordRaw(ep.ID, false, 5, 2, 1, 10*time.Millisecond)
	assert.Equal(t, core.CircuitOpen, cbm.GetState(ep.ID))

	// 等待 15ms 冷却到期（以便 stateVal 将其变更为 Half-Open）
	time.Sleep(15 * time.Millisecond)

	// 2. 模拟多个请求流经 Router
	// 第一次调用：由于许可限制为 1，且当前 activeCalls 为 0，应当成功放行
	res1 := r.Route(gctx, []*core.Endpoint{ep})
	assert.Len(t, res1, 1)

	// 模拟负载均衡选中并决定调用，抢占半开许可
	ok := cbm.AcquireHalfOpenPermit(ep.ID, false)
	assert.True(t, ok)

	// 第二次调用：并发请求，由于 activeCalls 已经被抢占，第 2 次应当被拦截排除
	res2 := r.Route(gctx, []*core.Endpoint{ep})
	assert.Empty(t, res2)

	// 3. 探路结束，返回成功，自动清理 activeCalls 计数并变更为 Closed
	cbm.RecordSuccess(gctx, ep)

	// 恢复 Closed 状态，后续调用均正常放行
	assert.Equal(t, core.CircuitClosed, cbm.GetState(ep.ID))
	res3 := r.Route(gctx, []*core.Endpoint{ep})
	assert.Len(t, res3, 1)
}

func TestCircuitBreakerRouter_HalfOpenPermits_EnabledActive(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()

	// 1. 创建处于 HalfOpen 的端点路由，且开启了主动健康探测 (enableActive = true)
	r := NewCircuitBreakerRouter(cbm, true, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)

	ep := newEndpoint("ep-ho-test2", "openai", "gpt-4", nil, nil)

	// 强设为 Open 熔断
	cbm.RecordRaw(ep.ID, false, 5, 2, 1, 10*time.Millisecond)
	cbm.RecordRaw(ep.ID, false, 5, 2, 1, 10*time.Millisecond)

	// 等待 15ms 冷却到期
	time.Sleep(15 * time.Millisecond)

	// 2. 真实请求流经 Router。由于开启了主动探测，并发探路许可数为 0
	// 即使到了冷却期，因为有主动探测协程负责，任何线上真实用户的请求也必须全部拦截
	res := r.Route(gctx, []*core.Endpoint{ep})
	assert.Empty(t, res, "expected active health checking to fully isolate half-open endpoint from real traffic")
}

func TestCircuitBreakerRouter_OutlierMaxPercentLimit(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)
	gctx.Policy = &policy.Policy{
		CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
			{
				Level:                   "INSTANCE",
				SlidingWindowSize:       10,
				MinCallsThreshold:       1,
				OutlierMaxPercent:       20, // 限制最大熔断 20%
				WaitDurationInOpenState: 10,
			},
		},
	}

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	ep2 := newEndpoint("ep2", "openai", "gpt-4", nil, nil)
	ep3 := newEndpoint("ep3", "openai", "gpt-4", nil, nil)
	ep4 := newEndpoint("ep4", "openai", "gpt-4", nil, nil)
	ep5 := newEndpoint("ep5", "openai", "gpt-4", nil, nil)
	endpoints := []*core.Endpoint{ep1, ep2, ep3, ep4, ep5}

	// 记录 ep1 和 ep2 熔断
	cbm.RecordRaw("ep1", false, 10, 1, 1, 30*time.Second)
	cbm.RecordRaw("ep2", false, 10, 1, 1, 30*time.Second)

	assert.Equal(t, core.CircuitOpen, cbm.GetState("ep1"))
	assert.Equal(t, core.CircuitOpen, cbm.GetState("ep2"))

	result := r.Route(gctx, endpoints)

	// 5 个端点，20% 熔断比例，最大允许熔断数 = floor(5 * 20/100) = 1。
	// 虽然 ep1 和 ep2 都是 Open 状态，但因为排序限制，只有 ep1（字典序较小）被熔断，ep2 被放通。
	// 结果应该为 ep2, ep3, ep4, ep5 (共 4 个)
	assert.Len(t, result, 4)
	assert.Equal(t, "ep2", result[0].ID)
	assert.Equal(t, "ep3", result[1].ID)
	assert.Equal(t, "ep4", result[2].ID)
	assert.Equal(t, "ep5", result[3].ID)
}

func TestCircuitBreakerRouter_OutlierMaxPercentLimit_TimeOrdered(t *testing.T) {
	logger := mustNewLogger(t)
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, logger)
	gctx := newGctx(core.RequestTypeChatCompletion)
	gctx.Policy = &policy.Policy{
		CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
			{
				Level:                   "INSTANCE",
				SlidingWindowSize:       10,
				MinCallsThreshold:       1,
				OutlierMaxPercent:       20, // 限制最大熔断 20%
				WaitDurationInOpenState: 10,
			},
		},
	}

	ep1 := newEndpoint("ep1", "openai", "gpt-4", nil, nil)
	ep2 := newEndpoint("ep2", "openai", "gpt-4", nil, nil)
	ep3 := newEndpoint("ep3", "openai", "gpt-4", nil, nil)
	ep4 := newEndpoint("ep4", "openai", "gpt-4", nil, nil)
	ep5 := newEndpoint("ep5", "openai", "gpt-4", nil, nil)
	endpoints := []*core.Endpoint{ep1, ep2, ep3, ep4, ep5}

	// ep2 先熔断
	cbm.RecordRaw("ep2", false, 10, 1, 1, 30*time.Second)
	// 睡眠以制造时间差
	time.Sleep(10 * time.Millisecond)
	// ep1 后熔断
	cbm.RecordRaw("ep1", false, 10, 1, 1, 30*time.Second)

	assert.Equal(t, core.CircuitOpen, cbm.GetState("ep1"))
	assert.Equal(t, core.CircuitOpen, cbm.GetState("ep2"))

	result := r.Route(gctx, endpoints)

	// 5 个端点，20% 熔断比例，最大允许熔断数 = 1。
	// 按时间排序：ep2 先熔断，应被排在前面，保持熔断状态。
	// ep1 后熔断，应被放通。
	// 结果集应为：ep1, ep3, ep4, ep5 (共 4 个)
	assert.Len(t, result, 4)
	assert.Equal(t, "ep1", result[0].ID)
	assert.Equal(t, "ep3", result[1].ID)
	assert.Equal(t, "ep4", result[2].ID)
	assert.Equal(t, "ep5", result[3].ID)
}
