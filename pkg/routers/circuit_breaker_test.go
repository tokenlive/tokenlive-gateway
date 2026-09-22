package routers

import (
	"net/http"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// 策略被禁用后,路由不应再按旧的 Open/HalfOpen 状态过滤 endpoint,
// 且旧状态应被复位,避免僵尸熔断永久拦截流量。
func TestCircuitBreakerRouter_NoPolicy_ResetsAndPassesAll(t *testing.T) {
	cbm := core.NewCircuitBreakerManager()
	r := NewCircuitBreakerRouter(cbm, false, zap.NewNop())

	ep := &core.Endpoint{
		ID:       "ep-1",
		Code:     "grok-primary",
		Provider: "Grok1",
		Model:    "grok-4.6",
	}
	endpoints := []*core.Endpoint{ep}

	// 1. 策略存在时:打挂熔断器,进入 Open
	gctx := &core.GatewayContext{
		Policy: &policy.Policy{
			CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
				{
					ID:                          "cb-instance",
					Name:                        "instance breaker",
					Level:                       "INSTANCE",
					SlidingWindowType:           "count",
					SlidingWindowSize:           5,
					MinCallsThreshold:           1,
					FailureRateThreshold:        1,
					AllowedCallsInHalfOpenState: 1,
					WaitDurationInOpenState:     60000,
					ErrorCodes:                  []string{"400"},
				},
			},
		},
		UpstreamResponse: &http.Response{StatusCode: http.StatusBadRequest},
	}
	cbm.RecordFailure(gctx, ep, nil)
	if !cbm.IsInstanceOpen(ep.ID) {
		t.Fatalf("expected instance breaker to be OPEN after failure")
	}

	// 等待进入 HalfOpen
	time.Sleep(10 * time.Millisecond)
	// 手动推进状态: Open -> HalfOpen 需要等 recoveryTO,这里用短超时重新构造
	// 直接验证主路径: 策略消失后 Route 应放行全部 endpoint

	// 2. 策略消失后(禁用):Route 不再过滤,且状态被复位
	gctxNoPolicy := &core.GatewayContext{}
	result := r.Route(gctxNoPolicy, endpoints)
	if len(result) != 1 || result[0].ID != ep.ID {
		t.Fatalf("expected all endpoints to pass when no policy, got %d", len(result))
	}
	if state := cbm.GetState(ep.ID); state != core.CircuitClosed {
		t.Fatalf("expected breaker state to be reset to CLOSED, got %v", state)
	}

	// 3. 再次 Route(状态已清理)依然全量放行
	result = r.Route(gctxNoPolicy, endpoints)
	if len(result) != 1 {
		t.Fatalf("expected stable pass-through after reset, got %d", len(result))
	}
}
