package invoker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// mockStateStore 实现 StateStore 接口，用于测试
type mockStateStore struct {
	mu      sync.Mutex
	latency map[string]time.Duration
}

func newMockStateStore() *mockStateStore {
	return &mockStateStore{
		latency: make(map[string]time.Duration),
	}
}

func (m *mockStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latency[endpointID] = latency
	return nil
}

func (m *mockStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
	return "", nil
}

func (m *mockStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	return nil
}

func (m *mockStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lat, ok := m.latency[endpointID]; ok {
		return lat, nil
	}
	return 0, nil
}

func (m *mockStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	return 10000, nil
}

func (m *mockStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	return nil
}

func (m *mockStateStore) RateLimitTake(ctx context.Context, key string, tokens int64, limit int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	return true, capacity - tokens, nil
}

func (m *mockStateStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0, nil
}

func (m *mockStateStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func (m *mockStateStore) Close() error { return nil }

// mockDiscovery 实现 Discovery 接口
type mockDiscovery struct {
	endpoints []*core.Endpoint
	err       error
}

func (m *mockDiscovery) List(ctx context.Context, model string) ([]*core.Endpoint, error) {
	return m.endpoints, m.err
}

func (m *mockDiscovery) Watch(ctx context.Context, model string) (<-chan []*core.Endpoint, error) {
	ch := make(chan []*core.Endpoint)
	close(ch)
	return ch, nil
}

func (m *mockDiscovery) Close() error { return nil }

// mockLoadBalancer 实现 LoadBalancer 接口，始终选择第一个 endpoint
type mockLoadBalancer struct {
	provider  core.Provider
	callCount int
}

func (m *mockLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	m.callCount++
	if len(endpoints) == 0 {
		return nil
	}
	return NewProviderInvoker(m.provider, endpoints[0])
}

// passThroughRouter 实现 Router 接口，透传所有 endpoints
type passThroughRouter struct{}

func (r *passThroughRouter) Name() string { return "pass_through" }
func (r *passThroughRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	return endpoints
}

func newTestClusterInvoker(
	discovery core.Discovery,
	lb core.LoadBalancer,
	retry *policy.RetryPolicy,
) *ClusterInvoker {
	logger, _ := zap.NewDevelopment()
	stateStore := newMockStateStore()
	cbManager := core.NewCircuitBreakerManager()
	return NewClusterInvoker(
		discovery,
		[]core.Router{&passThroughRouter{}},
		map[string]core.LoadBalancer{"round_robin": lb},
		retry,
		cbManager,
		stateStore,
		logger,
		nil,
	)
}

func newTestGatewayContext() *core.GatewayContext {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx := core.AcquireContext(w, r)
	gctx.Model = "gpt-4"
	return gctx
}

func TestClusterInvoker_Success(t *testing.T) {
	ep := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	provider := &mockProvider{name: "openai"}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	retry := &policy.RetryPolicy{
		Retry:       2,
		BackoffType: "exponential_jitter",
		BaseMs:      10,
		ErrorCodes:  []string{"500", "502", "503"},
	}

	ci := newTestClusterInvoker(discovery, lb, retry)
	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gctx.AttemptCount != 1 {
		t.Errorf("expected 1 attempt, got %d", gctx.AttemptCount)
	}
	if gctx.SelectedEndpoint == nil || gctx.SelectedEndpoint.ID != "ep-1" {
		t.Errorf("expected selected endpoint ep-1, got %v", gctx.SelectedEndpoint)
	}
}

func TestClusterInvoker_Retry(t *testing.T) {
	// 两个 endpoint：第一个会失败并被排除，第二个会成功
	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}

	failingProvider := &countingProvider{
		name:      "openai",
		failCount: 1, // 前1次失败
	}

	// discovery 返回两个 endpoint
	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2}}
	lb := &mockLoadBalancer{provider: failingProvider}
	retry := &policy.RetryPolicy{
		Retry:       2,
		BackoffType: "exponential_jitter",
		BaseMs:      1,
		ErrorCodes:  []string{"500", "502", "503"},
	}

	ci := newTestClusterInvoker(discovery, lb, retry)
	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error after retry, got %v", err)
	}
	if gctx.AttemptCount != 2 {
		t.Errorf("expected 2 attempts, got %d", gctx.AttemptCount)
	}
}

func TestRetryPolicy_ShouldRetryWithReason(t *testing.T) {
	retry := &policy.RetryPolicy{
		Retry:         2,
		BackoffType:   "exponential_jitter",
		BaseMs:        1,
		ErrorCodes:    []string{"502", "503"},
		ErrorMessages: []string{"rate limit exceeded"},
	}

	// 1. 匹配状态码
	shouldRetry, reason := retry.MatchErrorWithReason(502, "application/json", "bad gateway", nil)
	if !shouldRetry || reason != "matched status code '502' in error codes list" {
		t.Errorf("expected matched status code 502, got shouldRetry=%v, reason='%s'", shouldRetry, reason)
	}

	// 2. 匹配消息模式
	shouldRetry, reason = retry.MatchErrorWithReason(500, "application/json", "request hit rate limit exceeded, block", nil)
	if !shouldRetry || reason != "matched error message pattern 'rate limit exceeded' (error: 'request hit rate limit exceeded, block')" {
		t.Errorf("expected matched pattern, got shouldRetry=%v, reason='%s'", shouldRetry, reason)
	}

	// 3. 不匹配任何规则
	shouldRetry, reason = retry.MatchErrorWithReason(500, "application/json", "other internal error", nil)
	if shouldRetry || reason == "" {
		t.Errorf("expected not matched, got shouldRetry=%v, reason='%s'", shouldRetry, reason)
	}
}

func TestClusterInvoker_NoEndpoints(t *testing.T) {
	discovery := &mockDiscovery{endpoints: []*core.Endpoint{}}
	lb := &mockLoadBalancer{provider: &mockProvider{name: "openai"}}
	retry := &policy.RetryPolicy{
		Retry:  1,
		BaseMs: 1,
	}

	ci := newTestClusterInvoker(discovery, lb, retry)
	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if !errors.Is(err, core.ErrNoAvailableEndpoint) {
		t.Fatalf("expected core.ErrNoAvailableEndpoint, got %v", err)
	}
}

func TestClusterInvoker_AllRetriesFail(t *testing.T) {
	// 三个 endpoint，全部失败后无可用 endpoint
	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}
	ep3 := &core.Endpoint{ID: "ep-3", Provider: "openai", Model: "gpt-4"}

	failingProvider := &countingProvider{
		name:      "openai",
		failCount: 10, // 始终失败
	}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2, ep3}}
	lb := &mockLoadBalancer{provider: failingProvider}
	retry := &policy.RetryPolicy{
		Retry:       2,
		BackoffType: "exponential_jitter",
		BaseMs:      1,
		ErrorCodes:  []string{"500", "502", "503"},
	}

	ci := newTestClusterInvoker(discovery, lb, retry)
	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if err == nil {
		t.Fatal("expected error when all retries fail, got nil")
	}
	// 3 endpoints available, 3 retries (attempt 0 + 2 retries) = 3 attempts
	if gctx.AttemptCount != 3 {
		t.Errorf("expected 3 attempts, got %d", gctx.AttemptCount)
	}
}

// countingProvider 可控制失败次数的 Provider
type countingProvider struct {
	name      string
	failCount int // 前 failCount 次调用返回错误
	callCount int32
}

func (p *countingProvider) Name() string            { return p.name }
func (p *countingProvider) Type() core.ProviderType { return core.ProviderOpenAI }
func (p *countingProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{core.RequestTypeChatCompletion}
}
func (p *countingProvider) Invoke(gctx *core.GatewayContext) error {
	count := int(atomic.AddInt32(&p.callCount, 1))
	if count <= p.failCount {
		// 设置 UpstreamResponse 以便 ShouldRetry 能匹配状态码
		gctx.UpstreamResponse = &http.Response{StatusCode: 500}
		return errors.New("upstream error: status 500")
	}
	return nil
}
func (p *countingProvider) HealthCheck(ctx context.Context) error { return nil }
func (p *countingProvider) ValidateConfig() error                 { return nil }

// 编译时检查
var _ core.Provider = (*countingProvider)(nil)

// ===== 从 engine_cb_test.go 迁移的集成装配测试 =====

type testCapabilityRouter struct{}

func (r *testCapabilityRouter) Name() string { return "capability" }
func (r *testCapabilityRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if ep.SupportsRequestType(gctx.RequestType) {
			result = append(result, ep)
		}
	}
	return result
}

type testCircuitBreakerRouter struct {
	cbManager *core.CircuitBreakerManager
	logger    *zap.Logger
}

func (r *testCircuitBreakerRouter) Name() string { return "circuit_breaker" }
func (r *testCircuitBreakerRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	var result []*core.Endpoint
	for _, ep := range endpoints {
		serviceKey := ep.Provider + ":" + ep.Model
		if r.cbManager.IsServiceOpen(serviceKey) {
			continue
		}
		if r.cbManager.IsInstanceOpen(ep.ID) {
			continue
		}
		result = append(result, ep)
	}
	return result
}

type testRoundRobin struct {
	counter uint64
}

func (lb *testRoundRobin) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	idx := lb.counter
	lb.counter++
	ep := endpoints[idx%uint64(len(endpoints))]
	return NewProviderInvoker(ep.ProviderImpl, ep)
}

func TestBuildClusterInvoker_IncludesCircuitBreakerRouter(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	discovery := &mockDiscovery{
		endpoints: []*core.Endpoint{
			{ID: "ep-1", Provider: "openai", Model: "gpt-4"},
			{ID: "ep-2", Provider: "openai", Model: "gpt-4"},
		},
	}

	engine := core.NewEngine(&core.EngineConfig{}, discovery, ss, nil, logger)

	// 注册默认工厂
	engine.RegisterRouterFactory("capability", func(cfg core.RouterConfig, ss core.StateStore, logger *zap.Logger) core.Router {
		return &testCapabilityRouter{}
	})
	engine.RegisterRouterFactory("circuit_breaker", func(cfg core.RouterConfig, ss core.StateStore, logger *zap.Logger) core.Router {
		return &testCircuitBreakerRouter{cbManager: engine.CircuitBreakerManager(), logger: logger}
	})
	engine.RegisterLoadBalancerFactory("round_robin", func(ss core.StateStore) core.LoadBalancer {
		return &testRoundRobin{}
	})

	invokerBuilder := NewBuilder()
	invokerInstance, err := invokerBuilder.BuildInvoker(&core.InvokerConfig{Type: "cluster"}, engine)
	if err != nil {
		t.Fatalf("build invoker failed: %v", err)
	}
	ci := invokerInstance.(*ClusterInvoker)

	// 验证 router chain 包含两个 router
	if len(ci.routerChain) != 2 {
		t.Fatalf("expected 2 routers in chain, got %d", len(ci.routerChain))
	}

	// 验证第一个是 CapabilityRouter
	if ci.routerChain[0].Name() != "capability" {
		t.Errorf("expected first router to be 'capability', got %q", ci.routerChain[0].Name())
	}

	// 验证第二个是 CircuitBreakerRouter
	if ci.routerChain[1].Name() != "circuit_breaker" {
		t.Errorf("expected second router to be 'circuit_breaker', got %q", ci.routerChain[1].Name())
	}
}

func TestBuildClusterInvoker_EndToEnd_CircuitBreakerFiltering(t *testing.T) {
	ss := newMockStateStore()

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4", RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2}}
	logger, _ := zap.NewDevelopment()

	engine := core.NewEngine(&core.EngineConfig{}, discovery, ss, nil, logger)

	// 将 ep-1 实例级熔断设为 Open
	for i := 0; i < 5; i++ {
		engine.CircuitBreakerManager().RecordRaw("ep-1", false, 0, 0, 0, 0)
	}

	engine.RegisterRouterFactory("capability", func(cfg core.RouterConfig, ss core.StateStore, logger *zap.Logger) core.Router {
		return &testCapabilityRouter{}
	})
	engine.RegisterRouterFactory("circuit_breaker", func(cfg core.RouterConfig, ss core.StateStore, logger *zap.Logger) core.Router {
		return &testCircuitBreakerRouter{cbManager: engine.CircuitBreakerManager(), logger: logger}
	})
	engine.RegisterLoadBalancerFactory("round_robin", func(ss core.StateStore) core.LoadBalancer {
		return &testRoundRobin{}
	})

	invokerBuilder := NewBuilder()
	invokerInstance, err := invokerBuilder.BuildInvoker(&core.InvokerConfig{Type: "cluster"}, engine)
	if err != nil {
		t.Fatalf("build invoker failed: %v", err)
	}
	ci := invokerInstance.(*ClusterInvoker)

	gctx := &core.GatewayContext{
		RequestType: core.RequestTypeChatCompletion,
		Model:       "gpt-4",
	}
	gctx.Ctx = context.Background()
	gctx.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gctx.ResponseWriter = httptest.NewRecorder()

	// Invoke 应该过滤掉 ep-1，只使用 ep-2
	err = ci.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if gctx.SelectedEndpoint == nil {
		t.Fatal("expected selected endpoint, got nil")
	}
	if gctx.SelectedEndpoint.ID != "ep-2" {
		t.Errorf("expected ep-2 to be selected (ep-1 is circuit-open), got %s", gctx.SelectedEndpoint.ID)
	}
}
