package invoker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// ===== Mocks for Hedging Test =====

type mockHedgingProvider struct {
	name         string
	delay        time.Duration
	err          error
	responseStr  string
	isStream     bool
	invokeCalled chan bool
}

func (mp *mockHedgingProvider) Name() string            { return mp.name }
func (mp *mockHedgingProvider) Type() core.ProviderType { return core.ProviderType("mock") }
func (mp *mockHedgingProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{core.RequestTypeChatCompletion}
}
func (mp *mockHedgingProvider) HealthCheck(ctx context.Context) error { return nil }
func (mp *mockHedgingProvider) ValidateConfig() error                 { return nil }

func (mp *mockHedgingProvider) Invoke(gctx *core.GatewayContext) error {
	if mp.invokeCalled != nil {
		select {
		case mp.invokeCalled <- true:
		default:
		}
	}

	select {
	case <-time.After(mp.delay):
	case <-gctx.Ctx.Done():
		return gctx.Ctx.Err()
	}

	if mp.err != nil {
		return mp.err
	}

	gctx.InputTokens = 10
	gctx.OutputTokens = 20
	gctx.Cost = 0.001

	if mp.isStream {
		gctx.TTFT = 10 * time.Millisecond
		flusher, _ := gctx.ResponseWriter.(http.Flusher)
		_, _ = gctx.ResponseWriter.Write([]byte("data: " + mp.responseStr + "\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
	} else {
		gctx.UpstreamBody = []byte(`{"response":"` + mp.responseStr + `"}`)
	}

	return nil
}

type mockHedgingDiscovery struct {
	endpoints []*core.Endpoint
}

func (md *mockHedgingDiscovery) List(ctx context.Context, model string) ([]*core.Endpoint, error) {
	return md.endpoints, nil
}
func (md *mockHedgingDiscovery) Close() error { return nil }

type mockHedgingStateStore struct {
	core.StateStore
}

func (ms *mockHedgingStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	return nil
}

type mockHedgingLoadBalancer struct{}

func (mlb *mockHedgingLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	return &testEpInvoker{ep: endpoints[0]}
}

type testEpInvoker struct {
	ep *core.Endpoint
}

func (ei *testEpInvoker) Invoke(gctx *core.GatewayContext) error {
	gctx.SelectedEndpoint = ei.ep
	return ei.ep.ProviderImpl.Invoke(gctx)
}
func (ei *testEpInvoker) Endpoint() *core.Endpoint {
	return ei.ep
}

type mockFallbackInvoker struct {
	called bool
}

func (mfi *mockFallbackInvoker) Invoke(gctx *core.GatewayContext) error {
	mfi.called = true
	gctx.UpstreamBody = []byte(`{"response":"fallback"}`)
	return nil
}
func (mfi *mockFallbackInvoker) Endpoint() *core.Endpoint { return nil }

// ===== Test Cases =====

// TestHedgingInvoker_FastWinA 验证首选端点 A 快于 B 时，A 直接胜出，B 甚至未被启动（延迟对冲）
func TestHedgingInvoker_FastWinA(t *testing.T) {
	calledChanB := make(chan bool, 1)

	providerA := &mockHedgingProvider{name: "prov-a", delay: 50 * time.Millisecond, responseStr: "A response", isStream: true}
	providerB := &mockHedgingProvider{name: "prov-b", delay: 100 * time.Millisecond, responseStr: "B response", isStream: true, invokeCalled: calledChanB}

	epA := &core.Endpoint{ID: "ep-a", ProviderImpl: providerA}
	epB := &core.Endpoint{ID: "ep-b", ProviderImpl: providerB}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{epA, epB}}
	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockHedgingLoadBalancer{},
	}

	logger, _ := zap.NewDevelopment()
	fallback := &mockFallbackInvoker{}

	// 设置对冲延迟为 150ms，由于 A 在 50ms 内就会返回，所以 B 不应该启动
	policyConfig := &policy.Policy{
		InvocationPolicy: &policy.InvocationPolicy{
			Type: "hedging",
			RetryPolicy: &policy.RetryPolicy{
				BaseMs: 150, // hedging_delay_ms
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	gctx.Policy = policyConfig
	gctx.IsStream = true
	gctx.Model = "gpt-4"

	hi := NewHedgingInvoker(discovery, []core.Router{}, lbs, core.NewCircuitBreakerManager(), newMockStateStore(), logger, fallback)

	err := hi.Invoke(gctx)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	// 确认 B 没被启动过
	select {
	case <-calledChanB:
		t.Fatal("B was invoked but it shouldn't have been due to delay hedging win on A")
	default:
	}

	// 确认结果来自 A
	if w.Body.String() != "data: A response\n\n" {
		t.Errorf("expected A response, got %q", w.Body.String())
	}
}

// TestHedgingInvoker_RaceWinner 验证当 A 的延迟大于 delay 时，双发对冲发生，竞速获胜者胜出，未胜出者被取消
func TestHedgingInvoker_RaceWinner(t *testing.T) {
	calledChanB := make(chan bool, 1)

	// A 延迟 200ms，B 延迟 50ms（B 会被延迟启动，但在启动后会迅速完成）
	providerA := &mockHedgingProvider{name: "prov-a", delay: 200 * time.Millisecond, responseStr: "A response", isStream: true}
	providerB := &mockHedgingProvider{name: "prov-b", delay: 20 * time.Millisecond, responseStr: "B response", isStream: true, invokeCalled: calledChanB}

	epA := &core.Endpoint{ID: "ep-a", ProviderImpl: providerA}
	epB := &core.Endpoint{ID: "ep-b", ProviderImpl: providerB}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{epA, epB}}
	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockHedgingLoadBalancer{},
	}

	logger, _ := zap.NewDevelopment()
	fallback := &mockFallbackInvoker{}

	// 对冲延迟 50ms。200ms(A) 相比于 50ms 对冲延迟窗太大，因此 50ms 后会触发对 B 的调用
	policyConfig := &policy.Policy{
		InvocationPolicy: &policy.InvocationPolicy{
			Type: "hedging",
			RetryPolicy: &policy.RetryPolicy{
				BaseMs: 50,
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	gctx.Policy = policyConfig
	gctx.IsStream = true
	gctx.Model = "gpt-4"

	hi := NewHedgingInvoker(discovery, []core.Router{}, lbs, core.NewCircuitBreakerManager(), newMockStateStore(), logger, fallback)

	err := hi.Invoke(gctx)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	// 确认 B 被调用了
	select {
	case called := <-calledChanB:
		if !called {
			t.Fatal("expected B to be invoked")
		}
	default:
		t.Fatal("expected B to be invoked but B channel was empty")
	}

	// 确认 B 拿到了竞速优胜（因为 A 延迟 200ms，B 在 50ms 启动后 20ms（共70ms时）就返回了）
	if w.Body.String() != "data: B response\n\n" {
		t.Errorf("expected B response, got %q", w.Body.String())
	}
}

// TestHedgingInvoker_EarlyFailureA 验证 A 在调用初期失败时，能够无延迟立即启动对 B 的对冲请求
func TestHedgingInvoker_EarlyFailureA(t *testing.T) {
	calledChanB := make(chan bool, 1)

	providerA := &mockHedgingProvider{name: "prov-a", delay: 10 * time.Millisecond, err: errors.New("provider A dead"), isStream: true}
	providerB := &mockHedgingProvider{name: "prov-b", delay: 10 * time.Millisecond, responseStr: "B response", isStream: true, invokeCalled: calledChanB}

	epA := &core.Endpoint{ID: "ep-a", ProviderImpl: providerA}
	epB := &core.Endpoint{ID: "ep-b", ProviderImpl: providerB}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{epA, epB}}
	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockHedgingLoadBalancer{},
	}

	logger, _ := zap.NewDevelopment()
	fallback := &mockFallbackInvoker{}

	// 设置一个较长的 500ms 对冲延迟。如果无错误拦截，B 必须要等到 500ms 后才启动。
	// 但 A 会在 10ms 时直接报错，应该立即激活 B。
	policyConfig := &policy.Policy{
		InvocationPolicy: &policy.InvocationPolicy{
			Type: "hedging",
			RetryPolicy: &policy.RetryPolicy{
				BaseMs: 500,
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	gctx.Policy = policyConfig
	gctx.IsStream = true
	gctx.Model = "gpt-4"

	hi := NewHedgingInvoker(discovery, []core.Router{}, lbs, core.NewCircuitBreakerManager(), newMockStateStore(), logger, fallback)

	startTime := time.Now()
	err := hi.Invoke(gctx)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	duration := time.Since(startTime)
	if duration > 100*time.Millisecond {
		t.Errorf("Expected early win in less than 100ms due to A early failure, but took %v", duration)
	}

	// 确认 B 被调用
	select {
	case <-calledChanB:
	default:
		t.Fatal("expected B to be invoked")
	}

	if w.Body.String() != "data: B response\n\n" {
		t.Errorf("expected B response, got %q", w.Body.String())
	}
}

// TestHedgingInvoker_Fallback 验证可用实例数小于 2 时，能够平滑退化到默认的单发串行 Invoker
func TestHedgingInvoker_Fallback(t *testing.T) {
	providerA := &mockHedgingProvider{name: "prov-a", delay: 10 * time.Millisecond, responseStr: "A response", isStream: false}
	epA := &core.Endpoint{ID: "ep-a", ProviderImpl: providerA}

	// 只有一个端点 epA
	discovery := &mockDiscovery{endpoints: []*core.Endpoint{epA}}
	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockHedgingLoadBalancer{},
	}

	logger, _ := zap.NewDevelopment()
	fallback := &mockFallbackInvoker{}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	gctx.Model = "gpt-4"

	hi := NewHedgingInvoker(discovery, []core.Router{}, lbs, core.NewCircuitBreakerManager(), newMockStateStore(), logger, fallback)

	err := hi.Invoke(gctx)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	// 确认 fallback 串行调用器被触发了
	if !fallback.called {
		t.Error("expected fallback invoker to be triggered due to insufficient endpoints")
	}

	if string(gctx.UpstreamBody) != `{"response":"fallback"}` {
		t.Errorf("expected fallback response, got %s", string(gctx.UpstreamBody))
	}
}
