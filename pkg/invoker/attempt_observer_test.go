package invoker

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"go.uber.org/zap"
)

func prepareObserverHalfOpen(t *testing.T, manager *core.CircuitBreakerManager, ep *core.Endpoint) {
	t.Helper()
	for _, key := range []string{ep.Provider + ":" + ep.Model, ep.ID} {
		manager.RecordRaw(key, false, 10, 1, 2, time.Nanosecond)
		if manager.GetState(key) != core.CircuitHalfOpen {
			t.Fatalf("%s 未进入半开", key)
		}
	}
}

func observerPolicy(level string, messages ...string) *policy.Policy {
	return &policy.Policy{CircuitBreakPolicies: []*policy.CircuitBreakPolicy{{
		Level: level, SlidingWindowSize: 10, MinCallsThreshold: 1,
		AllowedCallsInHalfOpenState: 2, WaitDurationInOpenState: 60000,
		ErrorMessages: messages,
	}}}
}

func assertObserverPermitsReturned(t *testing.T, manager *core.CircuitBreakerManager, ep *core.Endpoint) {
	t.Helper()
	lease, err := manager.AcquireAttempt(nil, ep, false)
	if err != nil {
		t.Fatalf("调用结束后双层许可泄漏: %v", err)
	}
	lease.Release()
}

type observerFeedbackStore struct {
	*mockStateStore
	feedback func()
}

func (s *observerFeedbackStore) RecordLatency(ctx context.Context, epID string, latency time.Duration) error {
	s.feedback()
	return s.mockStateStore.RecordLatency(ctx, epID, latency)
}

func TestAttemptObserver_ClusterRecordsPhysicalAttemptBeforeFeedback(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success store", true: "failure event and store"}[failure], func(t *testing.T) {
			ep := &core.Endpoint{ID: "ep", Provider: "provider", Model: "model"}
			provider := &fixedErrorProvider{}
			if failure {
				provider.err = errors.New("matched")
			}
			manager := core.NewCircuitBreakerManager()
			g := newTestGatewayContext()
			defer core.ReleaseContext(g)
			g.Policy = observerPolicy("ENDPOINT", "matched")
			var feedbackStart time.Time
			var callbacks int
			feedback := func() {
				callbacks++
				if feedbackStart.IsZero() {
					feedbackStart = time.Now()
				}
				if g.AttemptCount != 1 || len(g.History) != 1 {
					t.Errorf("反馈开始时物理尝试尚未记录: count=%d history=%d", g.AttemptCount, len(g.History))
				}
				time.Sleep(10 * time.Millisecond)
			}
			manager.SetEventHandler(func(core.CBEvent) { feedback() })
			store := &observerFeedbackStore{mockStateStore: newMockStateStore(), feedback: feedback}
			ci := NewClusterInvoker(&mockDiscovery{endpoints: []*core.Endpoint{ep}}, nil,
				map[string]core.LoadBalancer{"round_robin": &mockLoadBalancer{provider: provider}},
				nil, manager, store, zap.NewNop(), nil)
			if err := ci.Invoke(g); (err != nil) != failure {
				t.Fatal(err)
			}
			wantCallbacks := 1
			if failure {
				wantCallbacks = 2
			}
			if callbacks != wantCallbacks || len(g.History) != 1 {
				t.Fatalf("反馈次数=%d history=%d", callbacks, len(g.History))
			}
			rec := g.History[0]
			if rec.Timestamp.After(feedbackStart) || rec.Latency > feedbackStart.Sub(g.UpstreamConnect) {
				t.Fatal("物理尝试时间/延迟包含了健康或 StateStore 反馈耗时")
			}
			if rec.Success == failure || failure && rec.Error != "matched" {
				t.Fatal("反馈前未记录正确的物理调用结果")
			}
		})
	}
}

func TestAttemptObserver_ClusterIgnoredErrorsReturnHalfOpenPermits(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"disconnect", core.ErrClientDisconnected},
		{"nonretryable", &nonRetryableTestError{message: "rejected"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := &core.Endpoint{ID: "ep", Provider: "provider", Model: "model"}
			provider := &fixedErrorProvider{name: "provider", err: tc.err}
			manager := core.NewCircuitBreakerManager()
			store := newMockStateStore()
			prepareObserverHalfOpen(t, manager, ep)
			ci := NewClusterInvoker(&mockDiscovery{endpoints: []*core.Endpoint{ep}}, nil,
				map[string]core.LoadBalancer{"round_robin": &mockLoadBalancer{provider: provider}},
				&policy.RetryPolicy{Retry: 2, BaseMs: 1}, manager, store, zap.NewNop(), nil)
			g := newTestGatewayContext()
			defer core.ReleaseContext(g)
			g.Policy = observerPolicy("")
			if err := ci.Invoke(g); !errors.Is(err, tc.err) {
				t.Fatalf("返回错误不匹配: %v", err)
			}
			if provider.callCount != 1 || g.AttemptCount != 1 {
				t.Fatal("忽略的错误仍被重试")
			}
			if manager.GetState(ep.ID) != core.CircuitHalfOpen || len(store.latency) != 0 || len(store.ttft) != 0 {
				t.Fatal("断连/不可重试错误不应计健康或惩罚")
			}
			assertObserverPermitsReturned(t, manager, ep)
		})
	}
}

func TestAttemptObserver_ClusterSingleLayerAndUnmatchedFinish(t *testing.T) {
	for _, level := range []string{"SERVICE", "ENDPOINT", "disabled"} {
		for _, failure := range []bool{false, true} {
			t.Run(level+map[bool]string{true: "/failure", false: "/success"}[failure], func(t *testing.T) {
				ep := &core.Endpoint{ID: "ep", Provider: "provider", Model: "model"}
				provider := &fixedErrorProvider{name: "provider"}
				if failure {
					provider.err = errors.New("other failure")
				}
				manager := core.NewCircuitBreakerManager()
				store := newMockStateStore()
				prepareObserverHalfOpen(t, manager, ep)
				ci := NewClusterInvoker(&mockDiscovery{endpoints: []*core.Endpoint{ep}}, nil,
					map[string]core.LoadBalancer{"round_robin": &mockLoadBalancer{provider: provider}},
					&policy.RetryPolicy{}, manager, store, zap.NewNop(), nil)
				g := newTestGatewayContext()
				defer core.ReleaseContext(g)
				if level != "disabled" {
					g.Policy = observerPolicy(level, "matched failure")
				}
				if err := ci.Invoke(g); (err != nil) != failure {
					t.Fatalf("调用结果错误: %v", err)
				}
				assertObserverPermitsReturned(t, manager, ep)
				if failure && store.latency[ep.ID] != 30*time.Second {
					t.Fatal("普通失败必须保留合成延迟惩罚")
				}
			})
		}
	}
}

type observerTimingProvider struct {
	mockProvider
}

func (p *observerTimingProvider) Invoke(g *core.GatewayContext) error {
	g.TTFT = 20 * time.Millisecond
	g.UpstreamConnect = time.Now().Add(-30 * time.Millisecond)
	return nil
}

func TestAttemptObserver_ClusterSlowSuccessStillReturnsSuccessAndLatency(t *testing.T) {
	for _, metric := range []string{"TTFT", "RTT"} {
		t.Run(metric, func(t *testing.T) {
			ep := &core.Endpoint{ID: "ep", Provider: "provider", Model: "model"}
			manager, store := core.NewCircuitBreakerManager(), newMockStateStore()
			ci := NewClusterInvoker(&mockDiscovery{endpoints: []*core.Endpoint{ep}}, nil,
				map[string]core.LoadBalancer{"round_robin": &mockLoadBalancer{provider: &observerTimingProvider{}}},
				nil, manager, store, zap.NewNop(), nil)
			g := newTestGatewayContext()
			defer core.ReleaseContext(g)
			g.Policy = observerPolicy("", "slow call")
			g.Policy.CircuitBreakPolicies[0].SlowCallMetric = metric
			g.Policy.CircuitBreakPolicies[0].SlowCallDurationThreshold = 1
			if err := ci.Invoke(g); err != nil {
				t.Fatal(err)
			}
			if manager.GetState(ep.ID) != core.CircuitOpen || manager.GetState("provider:model") != core.CircuitOpen {
				t.Fatal("慢成功调用应按匹配策略计失败")
			}
			if store.latency[ep.ID] < 30*time.Millisecond || store.ttft[ep.ID] != 20*time.Millisecond {
				t.Fatal("慢成功仍须记录 RTT 和正 TTFT，不写失败惩罚")
			}
		})
	}
}

func runObservedHedgeSub(hi *HedgingInvoker, g *core.GatewayContext, ep *core.Endpoint, ctx context.Context) *hedgingSession {
	ctx, cancel := context.WithCancel(ctx)
	session := &hedgingSession{mainWriter: httptest.NewRecorder(), winnerChan: make(chan string, 2),
		failuresChan: make(chan error, 2), completed: map[string]chan struct{}{ep.ID: make(chan struct{})}}
	hi.invokeSub(g, ep, ctx, cancel, session)
	cancel()
	return session
}

func TestAttemptObserver_HedgeErrorsKeepManagerMatching(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		matcher string
		want    core.CircuitState
	}{
		{"loser cancel matched", context.Canceled, "context canceled", core.CircuitOpen},
		{"loser cancel unmatched", context.Canceled, "other", core.CircuitHalfOpen},
		{"disconnect matched", core.ErrClientDisconnected, "client disconnected", core.CircuitOpen},
		{"nonretryable matched", &nonRetryableTestError{message: "rejected"}, "rejected", core.CircuitOpen},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := &core.Endpoint{ID: "ep", Provider: "provider", Model: "model", ProviderImpl: &fixedErrorProvider{err: tc.err}}
			manager, store := core.NewCircuitBreakerManager(), newMockStateStore()
			prepareObserverHalfOpen(t, manager, ep)
			hi := NewHedgingInvoker(nil, nil, nil, manager, store, zap.NewNop(), nil)
			g := newTestGatewayContext()
			defer core.ReleaseContext(g)
			g.Policy = observerPolicy("", tc.matcher)
			s := runObservedHedgeSub(hi, g, ep, context.Background())
			if len(s.failuresChan) != 1 || s.lastFailure == nil {
				t.Fatal("对冲失败编排丢失")
			}
			if manager.GetState(ep.ID) != tc.want || manager.GetState("provider:model") != tc.want {
				t.Fatal("对冲错误应全部交由 Manager 匹配，不能提前忽略")
			}
			if len(store.latency) != 0 || len(store.ttft) != 0 {
				t.Fatal("对冲失败不应写惩罚")
			}
			if tc.want == core.CircuitHalfOpen {
				assertObserverPermitsReturned(t, manager, ep)
			}
		})
	}
}

func TestAttemptObserver_HedgeSuccessOnlyRecordsRTT(t *testing.T) {
	ep := &core.Endpoint{ID: "ep", Provider: "provider", Model: "model", ProviderImpl: &observerTimingProvider{}}
	manager, store := core.NewCircuitBreakerManager(), newMockStateStore()
	prepareObserverHalfOpen(t, manager, ep)
	hi := NewHedgingInvoker(nil, nil, nil, manager, store, zap.NewNop(), nil)
	g := newTestGatewayContext()
	defer core.ReleaseContext(g)
	g.Policy = observerPolicy("", "slow call")
	g.Policy.CircuitBreakPolicies[0].SlowCallMetric = "TTFT"
	g.Policy.CircuitBreakPolicies[0].SlowCallDurationThreshold = 1
	s := runObservedHedgeSub(hi, g, ep, context.Background())
	if s.winnerID != ep.ID || manager.GetState(ep.ID) != core.CircuitHalfOpen {
		t.Fatal("对冲成功不应评估慢调用")
	}
	if store.latency[ep.ID] < 30*time.Millisecond || len(store.ttft) != 0 {
		t.Fatal("对冲成功只记录 RTT，不写 TTFT 序列")
	}
	assertObserverPermitsReturned(t, manager, ep)
}
