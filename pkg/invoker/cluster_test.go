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
	ttft    map[string]time.Duration
}

func newMockStateStore() *mockStateStore {
	return &mockStateStore{
		latency: make(map[string]time.Duration),
		ttft:    make(map[string]time.Duration),
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

func (m *mockStateStore) RecordTTFT(ctx context.Context, endpointID string, ttft time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ttft[endpointID] = ttft
	return nil
}

func (m *mockStateStore) GetAvgTTFT(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.ttft[endpointID]; ok {
		return v, nil
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
	ep1 := &core.Endpoint{ID: "ep-1", Code: "endpoint-code-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Code: "endpoint-code-2", Provider: "openai", Model: "gpt-4"}

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
	if len(gctx.History) != 2 {
		t.Fatalf("expected 2 history records, got %d", len(gctx.History))
	}
	firstAttempt := gctx.History[0]
	if firstAttempt.Success {
		t.Fatal("expected first attempt to be recorded as failed")
	}
	if firstAttempt.EndpointID != "ep-1" {
		t.Errorf("expected first attempt endpoint ep-1, got %q", firstAttempt.EndpointID)
	}
	if firstAttempt.EndpointCode != "endpoint-code-1" {
		t.Errorf("expected first attempt endpoint code endpoint-code-1, got %q", firstAttempt.EndpointCode)
	}
	if firstAttempt.StatusCode != 500 {
		t.Errorf("expected first attempt status 500, got %d", firstAttempt.StatusCode)
	}
	if firstAttempt.Error != "upstream error: status 500" {
		t.Errorf("expected first attempt error to be recorded, got %q", firstAttempt.Error)
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

func TestClusterInvoker_NoEndpointsWithZeroRetryReturnsFallbackSignal(t *testing.T) {
	discovery := &mockDiscovery{endpoints: []*core.Endpoint{}}
	lb := &mockLoadBalancer{provider: &mockProvider{name: "openai"}}
	retry := &policy.RetryPolicy{
		Retry:  0,
		BaseMs: 1,
	}

	ci := newTestClusterInvoker(discovery, lb, retry)
	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if !errors.Is(err, core.ErrNoAvailableEndpoint) {
		t.Fatalf("expected core.ErrNoAvailableEndpoint, got %v", err)
	}
	if gctx.AttemptCount != 0 {
		t.Fatalf("expected no physical attempts when no endpoints exist, got %d", gctx.AttemptCount)
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

func TestClusterInvoker_RetryLimitIsStrictEvenWhenMoreEndpointsExist(t *testing.T) {
	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}
	ep3 := &core.Endpoint{ID: "ep-3", Provider: "openai", Model: "gpt-4"}
	ep4 := &core.Endpoint{ID: "ep-4", Provider: "openai", Model: "gpt-4"}

	provider := &countingProvider{
		name:      "openai",
		failCount: 10,
	}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2, ep3, ep4}}
	lb := &mockLoadBalancer{provider: provider}
	retry := &policy.RetryPolicy{
		Retry:       2,
		BackoffType: "fixed",
		BaseMs:      1,
		ErrorCodes:  []string{"500"},
	}

	ci := newTestClusterInvoker(discovery, lb, retry)
	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if err == nil {
		t.Fatal("expected error after retry limit is reached")
	}
	if gctx.AttemptCount != 3 {
		t.Fatalf("expected retry=2 to allow exactly 3 attempts, got %d", gctx.AttemptCount)
	}
	for _, rec := range gctx.History {
		if rec.EndpointID == ep4.ID {
			t.Fatalf("expected ep-4 not to be attempted after retry limit, history=%+v", gctx.History)
		}
	}
}

func TestClusterInvoker_RoundRobinRetriesWalkRemainingEndpointsInOrder(t *testing.T) {
	provider := &countingProvider{
		name:      "openai",
		failCount: 10,
	}
	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}
	ep3 := &core.Endpoint{ID: "ep-3", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}
	ep4 := &core.Endpoint{ID: "ep-4", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2, ep3, ep4}}
	retry := &policy.RetryPolicy{
		Retry:       2,
		BackoffType: "fixed",
		BaseMs:      1,
		ErrorCodes:  []string{"500"},
	}
	ci := newTestClusterInvoker(discovery, &testRoundRobin{}, retry)

	gctx1 := newTestGatewayContext()
	defer core.ReleaseContext(gctx1)
	err := ci.Invoke(gctx1)
	if err == nil {
		t.Fatal("expected first request to fail")
	}
	assertAttemptEndpoints(t, gctx1.History, []string{"ep-1", "ep-2", "ep-3"})

	gctx2 := newTestGatewayContext()
	defer core.ReleaseContext(gctx2)
	err = ci.Invoke(gctx2)
	if err == nil {
		t.Fatal("expected second request to fail")
	}
	assertAttemptEndpoints(t, gctx2.History, []string{"ep-2", "ep-3", "ep-4"})
}

func TestClusterInvoker_ReturnsNoAvailableEndpointWhenEndpointsExhaustBeforeRetryLimit(t *testing.T) {
	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4"}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4"}

	provider := &countingProvider{
		name:      "openai",
		failCount: 10,
	}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2}}
	lb := &mockLoadBalancer{provider: provider}
	retry := &policy.RetryPolicy{
		Retry:       3,
		BackoffType: "fixed",
		BaseMs:      1,
		ErrorCodes:  []string{"500"},
	}

	ci := newTestClusterInvoker(discovery, lb, retry)
	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if !errors.Is(err, core.ErrNoAvailableEndpoint) {
		t.Fatalf("expected ErrNoAvailableEndpoint before retry limit, got %v", err)
	}
	if gctx.AttemptCount != 2 {
		t.Fatalf("expected only 2 physical attempts, got %d", gctx.AttemptCount)
	}
}

func assertAttemptEndpoints(t *testing.T, history []core.AttemptRecord, want []string) {
	t.Helper()
	if len(history) != len(want) {
		t.Fatalf("expected %d attempts, got %d: %+v", len(want), len(history), history)
	}
	for i, rec := range history {
		if rec.EndpointID != want[i] {
			t.Fatalf("attempt %d endpoint mismatch: got %q, want %q; history=%+v", i, rec.EndpointID, want[i], history)
		}
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

type testPriorityRouter struct{}

func (r *testPriorityRouter) Name() string { return "priority" }
func (r *testPriorityRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	if len(endpoints) == 0 {
		return endpoints
	}
	minPriority := endpoints[0].Priority
	for _, ep := range endpoints[1:] {
		if ep.Priority < minPriority {
			minPriority = ep.Priority
		}
	}
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if ep.Priority == minPriority {
			result = append(result, ep)
		}
	}
	return result
}

func TestClusterInvoker_PriorityFailover(t *testing.T) {
	provider := &countingProvider{
		name:      "openai",
		failCount: 1, // 第一次调用失败
	}

	// ep-1 优先级高 (1)，ep-2 优先级低 (2)
	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", Priority: 1, ProviderImpl: provider}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4", Priority: 2, ProviderImpl: provider}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2}}
	logger, _ := zap.NewDevelopment()
	stateStore := newMockStateStore()
	cbManager := core.NewCircuitBreakerManager()

	// 这里使用包含 testPriorityRouter 的路由链
	ci := NewClusterInvoker(
		discovery,
		[]core.Router{&testPriorityRouter{}},
		map[string]core.LoadBalancer{"round_robin": &testRoundRobin{}},
		&policy.RetryPolicy{
			Retry:       1,
			BackoffType: "fixed",
			BaseMs:      1,
			ErrorCodes:  []string{"500"},
		},
		cbManager,
		stateStore,
		logger,
		nil,
	)

	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected failover to succeed, got error: %v", err)
	}

	if gctx.AttemptCount != 2 {
		t.Errorf("expected 2 attempts (first failed, second succeeded), got %d", gctx.AttemptCount)
	}

	if gctx.SelectedEndpoint == nil {
		t.Fatal("expected selected endpoint, got nil")
	}
	if gctx.SelectedEndpoint.ID != "ep-2" {
		t.Errorf("expected failover to backup endpoint ep-2 (Priority 2), got %s", gctx.SelectedEndpoint.ID)
	}
}

func TestClusterInvoker_ExcludeFailedEndpointFalse_RetriesOnSameEndpoint(t *testing.T) {
	provider := &countingProvider{
		name:      "openai",
		failCount: 2, // 失败2次，需要第3次才成功
	}

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}
	ep2 := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1, ep2}}
	logger, _ := zap.NewDevelopment()
	stateStore := newMockStateStore()
	cbManager := core.NewCircuitBreakerManager()

	excludeFalse := false
	ci := NewClusterInvoker(
		discovery,
		[]core.Router{},
		map[string]core.LoadBalancer{"round_robin": &testRoundRobin{}},
		&policy.RetryPolicy{
			Retry:                 2,
			BackoffType:           "fixed",
			BaseMs:                1,
			ErrorCodes:            []string{"500"},
			ExcludeFailedEndpoint: &excludeFalse,
		},
		cbManager,
		stateStore,
		logger,
		nil,
	)

	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if err != nil {
		t.Fatalf("expected retry on same endpoint to succeed eventually, got error: %v", err)
	}

	if gctx.AttemptCount != 3 {
		t.Errorf("expected 3 attempts, got %d", gctx.AttemptCount)
	}

	// 因为 exclude_failed_endpoint 为 false，所以三次都是分配在 ep-1
	if gctx.SelectedEndpoint == nil || gctx.SelectedEndpoint.ID != "ep-1" {
		t.Errorf("expected selected endpoint to be ep-1, got %v", gctx.SelectedEndpoint)
	}
}

func TestClusterInvoker_ExcludeFailedEndpointFalse_AbortedWhenCircuitBreakerOpen(t *testing.T) {
	provider := &countingProvider{
		name:      "openai",
		failCount: 5, // 总是失败
	}

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}

	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1}}
	logger, _ := zap.NewDevelopment()
	stateStore := newMockStateStore()
	cbManager := core.NewCircuitBreakerManager()

	excludeFalse := false
	ci := NewClusterInvoker(
		discovery,
		[]core.Router{},
		map[string]core.LoadBalancer{"round_robin": &testRoundRobin{}},
		&policy.RetryPolicy{
			Retry:                 3,
			BackoffType:           "fixed",
			BaseMs:                1,
			ErrorCodes:            []string{"500"},
			ExcludeFailedEndpoint: &excludeFalse,
		},
		cbManager,
		stateStore,
		logger,
		nil,
	)

	// 先让 ep-1 连续失败 10 次触发熔断
	for i := 0; i < 10; i++ {
		cbManager.RecordRaw("ep-1", false, 0, 0, 0, 0)
	}

	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	err := ci.Invoke(gctx)
	if err == nil {
		t.Fatal("expected error due to circuit breaker Open, got nil")
	}

	// 应该在第 1 次 Attempt 真正请求前（由于已熔断）就直接被阻断返回错误，所以 AttemptCount 应该为 0
	if gctx.AttemptCount != 0 {
		t.Errorf("expected 0 attempts (aborted before execution), got %d", gctx.AttemptCount)
	}
}

func TestClusterInvoker_FatalErrAbortsRetry(t *testing.T) {
	provider := &countingProvider{
		name:      "openai",
		failCount: 5,
	}

	ep1 := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", ProviderImpl: provider}
	discovery := &mockDiscovery{endpoints: []*core.Endpoint{ep1}}
	logger, _ := zap.NewDevelopment()
	stateStore := newMockStateStore()
	cbManager := core.NewCircuitBreakerManager()

	ci := NewClusterInvoker(
		discovery,
		[]core.Router{},
		map[string]core.LoadBalancer{"round_robin": &testRoundRobin{}},
		&policy.RetryPolicy{
			Retry: 3,
			ErrorCodes: []string{"500"},
		},
		cbManager,
		stateStore,
		logger,
		nil,
	)

	gctx := newTestGatewayContext()
	defer core.ReleaseContext(gctx)

	// 强制注入 FatalErr
	gctx.FatalErr = core.ErrFatalNoAvailableEndpoint

	err := ci.Invoke(gctx)
	if err == nil || err.Error() != core.ErrFatalNoAvailableEndpoint.Error() {
		t.Fatalf("expected ErrFatalNoAvailableEndpoint, got %v", err)
	}

	// 应该在第 1 次 Attempt 执行前就直接因为 FatalErr 返回，所以 AttemptCount 为 0
	if gctx.AttemptCount != 0 {
		t.Errorf("expected 0 attempts, got %d", gctx.AttemptCount)
	}
}


