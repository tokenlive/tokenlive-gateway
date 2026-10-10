package core

import (
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

func prepareAttemptHalfOpen(t *testing.T, cbm *CircuitBreakerManager, keys ...string) {
	t.Helper()
	for _, key := range keys {
		cbm.RecordRaw(key, false, 10, 1, 2, time.Nanosecond)
		if cbm.GetState(key) != CircuitHalfOpen {
			t.Fatalf("%s 未进入半开", key)
		}
	}
}

func attemptTestContext(level string) *GatewayContext {
	return &GatewayContext{Policy: &policy.Policy{CircuitBreakPolicies: []*policy.CircuitBreakPolicy{{
		Level: level, Version: 1, SlidingWindowSize: 10, MinCallsThreshold: 1,
		AllowedCallsInHalfOpenState: 2, WaitDurationInOpenState: 60000,
		ErrorMessages: []string{"matched"},
	}}}}
}

func TestCircuitBreakerAttempt_DualLayerRollback(t *testing.T) {
	cbm := NewCircuitBreakerManager()
	ep := &Endpoint{ID: "ep", Provider: "provider", Model: "model"}
	prepareAttemptHalfOpen(t, cbm, "provider:model", "ep")
	if !cbm.AcquireHalfOpenPermit("ep", false) {
		t.Fatal("无法占用端点许可")
	}
	if lease, err := cbm.AcquireAttempt(nil, ep, false); err == nil || lease != nil {
		t.Fatal("端点许可冲突应拒绝双层获取")
	}
	if !cbm.AcquireHalfOpenPermit("provider:model", false) {
		t.Fatal("端点拒绝后服务许可未回滚")
	}
}

func TestCircuitBreakerAttempt_FinishAlwaysReturnsBothPermits(t *testing.T) {
	for _, tc := range []struct {
		name, level string
		failure     bool
	}{
		{"disabled success", "disabled", false}, {"disabled failure", "disabled", true},
		{"service success", "SERVICE", false}, {"service unmatched", "SERVICE", true},
		{"endpoint success", "ENDPOINT", false}, {"endpoint unmatched", "ENDPOINT", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cbm := NewCircuitBreakerManager()
			ep := &Endpoint{ID: "ep", Provider: "provider", Model: "model"}
			g := attemptTestContext(tc.level)
			if tc.level == "disabled" {
				g.Policy = nil
			}
			prepareAttemptHalfOpen(t, cbm, "provider:model", "ep")
			lease, err := cbm.AcquireAttempt(g, ep, false)
			if err != nil {
				t.Fatal(err)
			}
			if tc.failure {
				lease.RecordFailure(g, errors.New("other failure"))
			} else {
				lease.RecordSuccess(g)
			}
			if _, err := cbm.AcquireAttempt(g, ep, false); err != nil {
				t.Fatalf("结束后许可泄漏: %v", err)
			}
		})
	}
}

func TestCircuitBreakerAttempt_OldFinishCannotReleaseNewPermit(t *testing.T) {
	for _, change := range []string{"double finish", "reset", "policy version", "next half-open"} {
		t.Run(change, func(t *testing.T) {
			cbm := NewCircuitBreakerManager()
			ep := &Endpoint{ID: "ep", Provider: "provider", Model: "model"}
			g := attemptTestContext("")
			prepareAttemptHalfOpen(t, cbm, "provider:model", "ep")
			old, err := cbm.AcquireAttempt(g, ep, false)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "double finish":
				old.RecordSuccess(g)
			case "reset":
				cbm.Reset("provider:model")
				cbm.Reset("ep")
				prepareAttemptHalfOpen(t, cbm, "provider:model", "ep")
			case "policy version":
				g = attemptTestContext("")
				g.Policy.CircuitBreakPolicies[0].Version = 2
				cbm.CheckAndResetOnVersionChange("provider:model", 2)
				cbm.CheckAndResetOnVersionChange("ep", 2)
				prepareAttemptHalfOpen(t, cbm, "provider:model", "ep")
			case "next half-open":
				cbm.RecordRaw("provider:model", false, 10, 1, 2, time.Nanosecond)
				cbm.RecordRaw("ep", false, 10, 1, 2, time.Nanosecond)
				cbm.GetState("provider:model")
				cbm.GetState("ep")
			}
			current, err := cbm.AcquireAttempt(g, ep, false)
			if err != nil {
				t.Fatal(err)
			}
			old.RecordFailure(attemptTestContext(""), errors.New("matched"))
			old.Release()
			if cbm.GetState("ep") != CircuitHalfOpen || cbm.GetState("provider:model") != CircuitHalfOpen {
				t.Fatal("旧结束污染了当前健康状态")
			}
			if _, err := cbm.AcquireAttempt(g, ep, false); err == nil {
				t.Fatal("旧结束误归还了新许可")
			}
			current.Release()
			if _, err := cbm.AcquireAttempt(g, ep, false); err != nil {
				t.Fatalf("当前结束未归还许可: %v", err)
			}
		})
	}
}

func TestCircuitBreakerAttempt_NoRulesStillMatchesFailure(t *testing.T) {
	cbm := NewCircuitBreakerManager()
	ep := &Endpoint{ID: "ep", Provider: "provider", Model: "model"}
	g := attemptTestContext("")
	g.Policy.CircuitBreakPolicies[0].ErrorMessages = nil
	prepareAttemptHalfOpen(t, cbm, "provider:model", "ep")
	lease, err := cbm.AcquireAttempt(g, ep, false)
	if err != nil {
		t.Fatal(err)
	}
	lease.RecordFailure(g, errors.New("any failure"))
	if cbm.GetState("ep") != CircuitOpen || cbm.GetState("provider:model") != CircuitOpen {
		t.Fatal("无匹配规则应保留默认全部失败计数")
	}
}

func TestCircuitBreakerAttempt_MultiplePoliciesPreserveLegacyOrder(t *testing.T) {
	for _, tc := range []struct {
		name          string
		secondVersion int64
		secondMatches bool
		success       bool
		wantState     CircuitState
		wantEvents    []string
		wantResults   []bool
		wantVersion   int64
		wantPolicyID  string
	}{
		{"first matches different version", 2, false, false, CircuitOpen, []string{"A"}, nil, 1, "A"},
		{"both match same version", 1, true, false, CircuitOpen, []string{"A"}, []bool{false}, 1, "B"},
		{"both match different version", 2, true, false, CircuitOpen, []string{"A", "B"}, []bool{false}, 2, "B"},
		{"success same version", 1, false, true, CircuitClosed, nil, []bool{true, true}, 1, "B"},
		{"success different version", 2, false, true, CircuitClosed, nil, []bool{true}, 2, "B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 同一输入走 legacy 和 lease；快照明确约束策略顺序、记录数与事件元数据。
			for _, legacy := range []bool{true, false} {
				manager := NewCircuitBreakerManager()
				ep := &Endpoint{ID: "ep", Provider: "provider", Model: "model"}
				prepareAttemptHalfOpen(t, manager, "ep")
				g := attemptTestContext("ENDPOINT")
				a := g.Policy.CircuitBreakPolicies[0]
				a.ID, a.AllowedCallsInHalfOpenState = "A", 1
				b := *a
				b.ID, b.Version = "B", tc.secondVersion
				if !tc.secondMatches {
					b.ErrorMessages = []string{"other"}
				}
				g.Policy.CircuitBreakPolicies = append(g.Policy.CircuitBreakPolicies, &b)
				var events []string
				manager.SetEventHandler(func(evt CBEvent) { events = append(events, evt.PolicyID) })
				if legacy {
					if !manager.AcquireHalfOpenPermit("ep", false) {
						t.Fatal("无法获取 legacy 许可")
					}
					if tc.success {
						manager.RecordSuccess(g, ep)
					} else {
						manager.RecordFailure(g, ep, errors.New("matched"))
					}
				} else {
					lease, err := manager.AcquireAttempt(g, ep, false)
					if err != nil {
						t.Fatal(err)
					}
					if tc.success {
						lease.RecordSuccess(g)
					} else {
						lease.RecordFailure(g, errors.New("matched"))
					}
				}
				if manager.GetState("ep") != tc.wantState || !reflect.DeepEqual(events, tc.wantEvents) {
					t.Fatalf("legacy=%v: state=%v events=%v, want %v %v", legacy, manager.GetState("ep"), events, tc.wantState, tc.wantEvents)
				}
				entry := manager.getEntry("ep")
				entry.mu.Lock()
				results := append([]bool(nil), entry.results...)
				version, policyID := entry.policyVersion, entry.policyID
				entry.mu.Unlock()
				if !reflect.DeepEqual(results, tc.wantResults) || version != tc.wantVersion || policyID != tc.wantPolicyID {
					t.Fatalf("legacy=%v 快照 results=%v version=%d policy=%s, want=%v/%d/%s", legacy, results, version, policyID, tc.wantResults, tc.wantVersion, tc.wantPolicyID)
				}
			}
		})
	}
}

func TestCircuitBreakerAttempt_PolicyCallbackResetCannotAffectNewPermit(t *testing.T) {
	manager := NewCircuitBreakerManager()
	ep := &Endpoint{ID: "ep", Provider: "provider", Model: "model"}
	prepareAttemptHalfOpen(t, manager, "ep")
	g := attemptTestContext("ENDPOINT")
	g.Policy.CircuitBreakPolicies[0].ID = "A"
	b := *g.Policy.CircuitBreakPolicies[0]
	b.ID, b.Version = "B", 2
	g.Policy.CircuitBreakPolicies = append(g.Policy.CircuitBreakPolicies, &b)
	var current *CircuitBreakerAttempt
	manager.SetEventHandler(func(evt CBEvent) {
		if evt.PolicyID != "A" {
			t.Fatal("外部 reset 后旧调用仍记录后续策略")
		}
		manager.SetEventHandler(nil)
		manager.Reset("ep")
		prepareAttemptHalfOpen(t, manager, "ep")
		var err error
		current, err = manager.AcquireAttempt(nil, ep, false)
		if err != nil {
			t.Fatal(err)
		}
	})
	old, err := manager.AcquireAttempt(g, ep, false)
	if err != nil {
		t.Fatal(err)
	}
	old.RecordFailure(g, errors.New("matched"))
	if current == nil || manager.GetState("ep") != CircuitHalfOpen {
		t.Fatal("外部 reset 后的新半开状态被旧调用污染")
	}
	if _, err := manager.AcquireAttempt(nil, ep, false); err == nil {
		t.Fatal("旧结束归还了 callback 获取的新许可")
	}
	current.Release()
	if lease, err := manager.AcquireAttempt(nil, ep, false); err != nil {
		t.Fatal(err)
	} else {
		lease.Release()
	}
}

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

// 策略被禁用后,半开探测许可必须归还,否则熔断器永久卡在 HALF_OPEN,
// 后续请求全部被 AllowRequest 拒绝(线上已发生的僵尸熔断问题)。
func TestCircuitBreakerManager_PolicyRemoved_ReleasesHalfOpenPermit(t *testing.T) {
	cbm := NewCircuitBreakerManager()

	ep := &Endpoint{
		ID:       "ep-1",
		Code:     "grok-primary",
		Provider: "Grok1",
		Model:    "grok-4.6",
	}
	serviceKey := ep.Provider + ":" + ep.Model

	// 1. 用短恢复超时的策略把实例打到 Open
	gctx := &GatewayContext{
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
					WaitDurationInOpenState:     50, // 50ms 后转 HalfOpen
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

	// 2. 等待超过恢复超时,状态推进到 HalfOpen
	time.Sleep(80 * time.Millisecond)
	if state := cbm.GetState(ep.ID); state != CircuitHalfOpen {
		t.Fatalf("expected HALF_OPEN after wait_duration, got %v", state)
	}

	// 3. 半开许可被获取(模拟探测请求在途)
	if !cbm.AcquireHalfOpenPermit(ep.ID, false) {
		t.Fatalf("expected to acquire half-open permit")
	}

	// 4. 策略禁用(消失)后,成功响应的记录应归还许可而不是泄漏
	gctxNoPolicy := &GatewayContext{}
	cbm.RecordSuccess(gctxNoPolicy, ep)

	// 5. 许可已归还:允许再次获取,路由不会被永久拒绝
	if !cbm.AcquireHalfOpenPermit(ep.ID, false) {
		t.Fatalf("expected permit to be released after policy removal, permit leaked")
	}

	// 6. 服务级许可同样归还
	if !cbm.AcquireHalfOpenPermit(serviceKey, false) {
		t.Fatalf("expected service-level permit to be released after policy removal")
	}
}
