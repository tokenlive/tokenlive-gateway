package core

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func TestCircuitBreakerEntry_TimeWindowSliding(t *testing.T) {
	e := &circuitBreakerEntry{
		state: CircuitClosed,
	}

	now := time.Now()

	// 1. 记录 2 次成功和 1 次失败，对齐在当前这秒
	old, newStatus := e.record(true, now, "time", 5, 3, 1, 10*time.Second, 0.0)
	if old != CircuitClosed || newStatus != CircuitClosed {
		t.Errorf("expected CLOSED -> CLOSED, got %v -> %v", old, newStatus)
	}
	e.record(true, now, "time", 5, 3, 1, 10*time.Second, 0.0)
	// 触发第 3 次记录（失败），达到了阈值（mc = 3）但由于总失败数是 1，小于 failThresh (3)，不应熔断
	e.record(false, now, "time", 5, 3, 1, 10*time.Second, 0.0)

	if e.state != CircuitClosed {
		t.Errorf("expected state to be CLOSED, got %v", e.state)
	}
	if len(e.buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(e.buckets))
	}
	if e.buckets[0].successes != 2 || e.buckets[0].failures != 1 {
		t.Errorf("unexpected bucket stats: successes=%d, failures=%d", e.buckets[0].successes, e.buckets[0].failures)
	}

	// 2. 在当前这秒再投递 2 次失败。此时这一秒的总失败数达到 3，应该触发熔断
	e.record(false, now, "time", 5, 3, 1, 10*time.Second, 0.0)
	_, finalStatus := e.record(false, now, "time", 5, 3, 1, 10*time.Second, 0.0)

	if finalStatus != CircuitOpen {
		t.Errorf("expected state to turn OPEN, got %v", finalStatus)
	}

	// 3. 重置，并测试 5 秒后过期淘汰
	e.reset()
	if e.state != CircuitClosed {
		t.Errorf("expected reset to CLOSED, got %v", e.state)
	}

	// 在 t=now 秒，记录 3 次失败 -> 触发熔断
	e.record(false, now, "time", 5, 3, 1, 10*time.Second, 0.0)
	e.record(false, now, "time", 5, 3, 1, 10*time.Second, 0.0)
	_, curStatus := e.record(false, now, "time", 5, 3, 1, 10*time.Second, 0.0)
	if curStatus != CircuitOpen {
		t.Errorf("expected OPEN, got %v", curStatus)
	}

	// 流逝 6 秒 (now + 6s)
	future := now.Add(6 * time.Second)
	e.computeState(future) // 触发过期
	if len(e.buckets) != 0 {
		t.Errorf("expected buckets to be expired and empty, got %d", len(e.buckets))
	}
}

func TestCircuitBreakerManager_RecordFailure_EventIncludesEndpointCode(t *testing.T) {
	cbm := NewCircuitBreakerManager()
	var got CBEvent
	cbm.SetEventHandler(func(evt CBEvent) {
		got = evt
	})

	gctx := &GatewayContext{
		Policy: &policy.Policy{
			CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
				{
					ID:                          "cb-instance",
					Name:                        "instance breaker",
					Level:                       "INSTANCE",
					SlidingWindowSize:           1,
					MinCallsThreshold:           1,
					FailureRateThreshold:        1,
					AllowedCallsInHalfOpenState: 1,
					WaitDurationInOpenState:     1000,
					ErrorCodes:                  []string{"500"},
				},
			},
		},
		UpstreamResponse: &http.Response{StatusCode: http.StatusInternalServerError},
	}
	ep := &Endpoint{
		ID:       "ep-1",
		Code:     "glm-primary",
		Provider: "glm-provider",
		Model:    "glm-5.2",
	}

	cbm.RecordFailure(gctx, ep, errors.New("upstream error: status 500"))

	if got.Key != "ep-1" {
		t.Fatalf("expected circuit break event for endpoint ep-1, got %q", got.Key)
	}
	if got.EndpointCode != "glm-primary" {
		t.Fatalf("expected endpoint code %q, got %q", "glm-primary", got.EndpointCode)
	}
}
