package core_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	_ "github.com/tokenlive/tokenlive-gateway/pkg/llm/providers" // 自动加载并注册真实 OpenAI 驱动以进行全链路仿真

	"go.uber.org/zap"
)

// ===== Chaos Http Mock Upstream Server =====

type ChaosHttpMockServer struct {
	server *httptest.Server
}

func NewChaosHttpMockServer() *ChaosHttpMockServer {
	s := &ChaosHttpMockServer{}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/return-502"):
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("bad gateway chaos"))

		case strings.Contains(path, "/delay-first-byte"):
			time.Sleep(1200 * time.Millisecond)
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
			return

		case strings.Contains(path, "/disconnect-post-ttft"):
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.WriteHeader(http.StatusOK)

			flusher, _ := w.(http.Flusher)
			content := "hello"
			if strings.Contains(path, "done-substring") {
				content = "model output containing response.done"
			}
			_, _ = w.Write([]byte(fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", content)))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)

			// 强行通过 Hijacker 关掉 TCP 连接，造成首包发送后中途断连
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
					return
				}
			}
			return

		case strings.Contains(path, "/timeout-pre-ttft"):
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}

		default:
			if r.Header.Get("Accept") == "text/event-stream" || strings.Contains(r.URL.RawQuery, "stream=true") {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))
			} else {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"success"}}],"usage":{"prompt_tokens":10,"completion_tokens":20}}`))
			}
		}
	}))
	return s
}

func (s *ChaosHttpMockServer) Close() {
	s.server.Close()
}

func (s *ChaosHttpMockServer) URL() string {
	return s.server.URL
}

// ===== Chaos Custom StateStore =====

type chaosStateStore struct {
	mu      sync.Mutex
	refunds map[string]int64
	incrs   map[string]int64
	latency map[string]time.Duration
}

func newChaosStateStore() *chaosStateStore {
	return &chaosStateStore{
		refunds: make(map[string]int64),
		incrs:   make(map[string]int64),
		latency: make(map[string]time.Duration),
	}
}

func (s *chaosStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incrs[key] += tokens
	return s.incrs[key], nil
}

func (s *chaosStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refunds[key] += tokens
	return nil
}

func (s *chaosStateStore) RateLimitTake(ctx context.Context, key string, tokens int64, limit int64, capacity int64, window time.Duration, now time.Time) (bool, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incrs[key] += tokens
	return true, capacity - s.incrs[key], nil
}

func (s *chaosStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latency[endpointID] = latency
	return nil
}

func (s *chaosStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
	return "", nil
}

func (s *chaosStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
	return nil
}

func (s *chaosStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
	return 0, nil
}

func (s *chaosStateStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0.0001, nil
}

func (s *chaosStateStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func (s *chaosStateStore) Close() error { return nil }

// ===== Mocks for Chaos Discovery =====

type chaosDiscovery struct {
	endpoints []*core.Endpoint
}

func (cd *chaosDiscovery) List(ctx context.Context, model string) ([]*core.Endpoint, error) {
	var matched []*core.Endpoint
	for _, ep := range cd.endpoints {
		if model == "" || ep.Model == model {
			matched = append(matched, ep)
		}
	}
	return matched, nil
}

func (cd *chaosDiscovery) Watch(ctx context.Context, model string) (<-chan []*core.Endpoint, error) {
	ch := make(chan []*core.Endpoint)
	close(ch)
	return ch, nil
}

func (cd *chaosDiscovery) Close() error { return nil }

// ===== Test Cases =====

// TestChaosHarness_FallbackOnPreByteFailure 验证首字节前故障重试与跨模型 Fallback 降级
func TestChaosHarness_FallbackOnPreByteFailure(t *testing.T) {
	mockServer := NewChaosHttpMockServer()
	defer mockServer.Close()

	// 注册 502 故障的 OpenAI 驱动与健康 OpenAI 驱动
	openAIProvider502, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "gpt-4-fail",
		BaseURL: mockServer.URL() + "/return-502",
		APIKey:  "test-key-openai",
		Models:  []string{"gpt-4"},
	})
	if err != nil {
		t.Fatalf("failed to create openai provider: %v", err)
	}

	openAIProviderHealthy, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "gpt-3.5-turbo-ok",
		BaseURL: mockServer.URL() + "/healthy",
		APIKey:  "test-key-openai",
		Models:  []string{"gpt-3.5-turbo"},
	})
	if err != nil {
		t.Fatalf("failed to create openai provider: %v", err)
	}

	// 主模型 gpt-4 的两个故障端点
	epA := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", URL: mockServer.URL() + "/models", Healthy: true, ProviderImpl: openAIProvider502, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
	epB := &core.Endpoint{ID: "ep-2", Provider: "openai", Model: "gpt-4", URL: mockServer.URL() + "/models", Healthy: true, ProviderImpl: openAIProvider502, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}

	// 降级模型 gpt-3.5-turbo 的健康端点
	epC := &core.Endpoint{ID: "ep-3", Provider: "openai", Model: "gpt-3.5-turbo", URL: mockServer.URL() + "/models", Healthy: true, ProviderImpl: openAIProviderHealthy, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}

	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{epA, epB, epC}}
	store := newChaosStateStore()
	logger, _ := zap.NewDevelopment()

	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockLoadBalancer{provider: openAIProvider502},
	}
	defaultRetry := &policy.RetryPolicy{
		Retry:       2,
		BackoffType: "exponential_jitter",
		BaseMs:      1,
		ErrorCodes:  []string{"502"},
	}
	cbManager := core.NewCircuitBreakerManager()

	ciGpt4 := invoker.NewClusterInvoker(discovery, []core.Router{}, lbs, defaultRetry, cbManager, store, logger, nil)
	ciGpt3 := invoker.NewClusterInvoker(discovery, []core.Router{}, lbs, defaultRetry, cbManager, store, logger, nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}`))
	w := httptest.NewRecorder()

	gctx := core.AcquireContext(w, req)
	gctx.Model = "gpt-4"
	gctx.RequestType = core.RequestTypeChatCompletion
	gctx.RawBody = []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}`)
	gctx.Policy = &policy.Policy{
		CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
			{
				Name:              "cb-test",
				Level:             "INSTANCE",
				SlidingWindowSize: 10,
				MinCallsThreshold: 1,
			},
		},
		InvocationPolicy: &policy.InvocationPolicy{
			Type:        "failover",
			RetryPolicy: defaultRetry,
			FallbackPolicy: &policy.FallbackPolicy{
				Targets: []string{"gpt-3.5-turbo"},
			},
		},
	}

	// 模拟 engine.go 动态降级请求管道
	var invokeErr error
	fallbackPolicy := gctx.Policy.InvocationPolicy.FallbackPolicy
	models := append([]string{gctx.Model}, fallbackPolicy.Targets...)
	for i, modelName := range models {
		if i > 0 {
			gctx.Model = modelName
			gctx.FallbackChain = append(gctx.FallbackChain, modelName)
		}
		var currentInvoker core.Invoker = ciGpt4
		if modelName == "gpt-3.5-turbo" {
			currentInvoker = ciGpt3
		}
		invokeErr = currentInvoker.Invoke(gctx)
		if invokeErr == nil {
			break
		}
		if gctx.TTFT > 0 {
			break
		}
		if i == len(models)-1 {
			break
		}
		// 客户端错误（400-499，排除429）不降级
		if gctx.UpstreamResponse != nil {
			code := gctx.UpstreamResponse.StatusCode
			if code >= 400 && code < 500 && code != 429 {
				break
			}
		}
	}
	err = invokeErr
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	// 主模型 gpt-4 尝试 2 次失败后，降级到 gpt-3.5-turbo 尝试了 1 次成功，总共 AttemptCount 为 3
	if gctx.AttemptCount != 3 {
		t.Errorf("expected attempt count 3, got %d", gctx.AttemptCount)
	}
	if len(gctx.FallbackChain) != 1 || gctx.FallbackChain[0] != "gpt-3.5-turbo" {
		t.Errorf("expected fallback chain ['gpt-3.5-turbo'], got %v", gctx.FallbackChain)
	}

	cb1 := cbManager.GetState("ep-1")
	cb2 := cbManager.GetState("ep-2")

	if cb1 != core.CircuitOpen {
		t.Error("expected ep-1 circuit breaker failure to be recorded and transitioned to Open")
	}
	if cb2 != core.CircuitOpen {
		t.Error("expected ep-2 circuit breaker failure to be recorded and transitioned to Open")
	}
}

// TestChaosHarness_TTFTSlowCallMelting 验证首字节延迟 (TTFT) 熔断指标的采集
func TestChaosHarness_TTFTSlowCallMelting(t *testing.T) {
	mockServer := NewChaosHttpMockServer()
	defer mockServer.Close()

	openAIProvider, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "gpt-4-slow",
		BaseURL: mockServer.URL() + "/delay-first-byte",
		APIKey:  "test-key-openai",
		Models:  []string{"gpt-4"},
	})
	if err != nil {
		t.Fatalf("failed to create openai provider: %v", err)
	}
	epA := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", URL: mockServer.URL() + "/models", Healthy: true, ProviderImpl: openAIProvider, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}

	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{epA}}
	store := newChaosStateStore()
	logger, _ := zap.NewDevelopment()

	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockLoadBalancer{provider: openAIProvider},
	}
	cbManager := core.NewCircuitBreakerManager()
	ci := invoker.NewClusterInvoker(discovery, []core.Router{}, lbs, nil, cbManager, store, logger, nil)

	testPolicy := &policy.Policy{
		CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
			{
				Name:                      "cb-ttft",
				Level:                     "endpoint",
				SlidingWindowType:         "count",
				SlidingWindowSize:         10,
				MinCallsThreshold:         1,
				SlowCallDurationThreshold: 800, // 800ms
				SlowCallRateThreshold:     50,
				SlowCallMetric:            "TTFT",
			},
		},
	}

	// 必须启用 stream 才有首字节 TTFT 概念
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-4","stream":true,"messages":[]}`))
	w := httptest.NewRecorder()

	gctx := core.AcquireContext(w, req)
	gctx.Model = "gpt-4"
	gctx.Policy = testPolicy
	gctx.RequestType = core.RequestTypeChatCompletion
	gctx.RawBody = []byte(`{"model":"gpt-4","stream":true,"messages":[]}`)
	gctx.IsStream = true

	err = ci.Invoke(gctx)
	if err != nil {
		t.Fatalf("Invoke failed: %v", err)
	}

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	if gctx.TTFT < 800*time.Millisecond {
		t.Errorf("expected TTFT >= 800ms, got %v", gctx.TTFT)
	}

	cb1 := cbManager.GetState("ep-1")
	if cb1 != core.CircuitOpen {
		t.Error("expected slow call TTFT > 800ms to be recorded as failure in CircuitBreakerManager and transitioned to Open")
	}
}

// TestChaosHarness_DisconnectPostTTFTAndRefund 验证流式首包后网络断连时：禁止重试且 OutboundPrecise 退款结算触发
func TestChaosHarness_DisconnectPostTTFTAndRefund(t *testing.T) {
	mockServer := NewChaosHttpMockServer()
	defer mockServer.Close()

	openAIProvider, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "gpt-4-stream-disconnect",
		BaseURL: mockServer.URL() + "/disconnect-post-ttft",
		APIKey:  "test-key-openai",
		Models:  []string{"gpt-4"},
	})
	if err != nil {
		t.Fatalf("failed to create openai provider: %v", err)
	}
	epA := &core.Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", URL: mockServer.URL() + "/models", Healthy: true, ProviderImpl: openAIProvider, RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}

	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{epA}}
	store := newChaosStateStore()
	logger, _ := zap.NewDevelopment()

	limitPolicy := &policy.LimitPolicy{
		Name:         "token-limit",
		Type:         "token",
		RelationType: "user",
		SlidingWindows: []*policy.SlidingWindow{
			{
				Threshold:      1000,
				TimeWindowInMs: 60000,
			},
		},
	}
	testPolicy := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{limitPolicy},
	}

	rateLimitFilter := inbound.NewRateLimitFilter(store)
	tokenSettlementFilter := outbound.NewTokenSettlementFilter(store, nil, nil)

	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockLoadBalancer{provider: openAIProvider},
	}
	cbManager := core.NewCircuitBreakerManager()
	ci := invoker.NewClusterInvoker(discovery, []core.Router{}, lbs, nil, cbManager, store, logger, nil)

	engineConfig := &core.EngineConfig{
		Pipelines: map[string]*core.PipelineConfig{
			"chat_completion": {
				Name:            "chat_completion",
				RequestTypes:    []core.RequestType{core.RequestTypeChatCompletion},
				InboundFilters:  []string{"rate_limit"},
				OutboundFilters: []string{"token_settlement"},
				Invoker:         core.InvokerConfig{Type: "failover"},
			},
		},
	}

	engine := core.NewEngine(engineConfig, discovery, store, &mockPolicyProvider{policy: testPolicy}, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{invoker: ci})
	engine.RegisterFilter("rate_limit", rateLimitFilter)
	engine.RegisterFilter("token_settlement", &testOutboundFilterWrapper{inner: tokenSettlementFilter})

	err = engine.Init()
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-4","stream":true,"messages":[]}`))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	capCtx := getCapturedContext()

	if capCtx.AttemptCount != 1 {
		t.Errorf("expected attempt count 1 (no retries after TTFT), got %d", capCtx.AttemptCount)
	}

	if capCtx.Err == nil {
		t.Error("expected connection error post TTFT, got nil")
	}

	body := w.Body.String()
	if !strings.Contains(body, `data: {"error": {"message":`) {
		t.Errorf("expected OpenAI error format in body, got %q", body)
	}
	if !strings.Contains(body, `"type": "upstream_error"`) {
		t.Errorf("expected upstream_error type inside error event, got %q", body)
	}

	store.mu.Lock()
	incrs := store.incrs
	store.mu.Unlock()

	expectedKey := ":gpt-4:token-limit:1m0s"
	incredTokens := incrs[expectedKey]
	// Body: `{"model":"gpt-4","stream":true,"messages":[]}`
	// length = 45 -> EstimateInputTokens = 45/4 = 11.
	// 已传输的字符是 "hello" (5个字符)，按默认 ratio 0.6 估算 completion tokens = 5 * 0.6 = 3.
	// 由于 input tokens 在 limit.go 中被锁定为 11（防中断退费漏洞），所以实际总消耗为 11 + 3 = 14.
	// 预估扣减 11，所以追加扣减了 3，总计 incred 应该是 14.
	if incredTokens != 14 {
		t.Errorf("expected 14 total incred tokens for key %s, got %d", expectedKey, incredTokens)
	}
}

// ===== Test helpers =====

type mockLoadBalancer struct {
	provider core.Provider
}

func (m *mockLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	prov := endpoints[0].ProviderImpl
	if prov == nil {
		prov = m.provider
	}
	return &testInvoker{provider: prov, endpoint: endpoints[0]}
}

type testInvoker struct {
	provider core.Provider
	endpoint *core.Endpoint
}

func (ti *testInvoker) Invoke(gctx *core.GatewayContext) error {
	gctx.SelectedEndpoint = ti.endpoint
	gctx.UpstreamConnect = time.Now()
	return ti.provider.Invoke(gctx)
}

func (ti *testInvoker) Endpoint() *core.Endpoint {
	return ti.endpoint
}

type mockPolicyProvider struct {
	policy *policy.Policy
}

func (m *mockPolicyProvider) GetPolicy(ctx context.Context, tenantCode, userID, model string) (*policy.Policy, error) {
	return m.policy, nil
}

type testInvokerBuilder struct {
	invoker core.Invoker
}

func (b *testInvokerBuilder) BuildInvoker(cfg *core.InvokerConfig, r core.InvokerDependencyResolver) (core.Invoker, error) {
	return b.invoker, nil
}

// 借用全局变量拦截 GatewayContext 指标，规避 ReleaseContext 重置问题
type capturedContext struct {
	AttemptCount int
	Err          error
}

var (
	captured   capturedContext
	capturedMu sync.Mutex
)

func getCapturedContext() capturedContext {
	capturedMu.Lock()
	defer capturedMu.Unlock()
	return captured
}

func setCapturedContext(gctx *core.GatewayContext) {
	capturedMu.Lock()
	defer capturedMu.Unlock()
	captured.AttemptCount = gctx.AttemptCount
	captured.Err = gctx.Err
}

// 自定义 OutboundFilter 用以包装 TokenSettlementFilter，并拦截 gctx 指标
type testOutboundFilterWrapper struct {
	inner core.OutboundFilter
}

func (w *testOutboundFilterWrapper) Name() string { return w.inner.Name() }
func (w *testOutboundFilterWrapper) Order() int   { return w.inner.Order() }
func (w *testOutboundFilterWrapper) Criticality() core.FilterCriticality {
	return w.inner.Criticality()
}
func (w *testOutboundFilterWrapper) OnResponse(gctx *core.GatewayContext) error {
	setCapturedContext(gctx)
	return w.inner.OnResponse(gctx)
}

// TestChaosHarness_ResponsesStreamDisconnect 验证在 Responses 协议流式首包后断连时：
// 网关向客户端写入规范的 response.done 错误事件和 [DONE] 帧
func TestChaosHarness_ResponsesStreamDisconnect(t *testing.T) {
	mockServer := NewChaosHttpMockServer()
	defer mockServer.Close()

	openAIProvider, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "gpt-4-responses-disconnect",
		BaseURL: mockServer.URL() + "/disconnect-post-ttft",
		APIKey:  "test-key-openai",
		Models:  []string{"gpt-4"},
	})
	if err != nil {
		t.Fatalf("failed to create openai provider: %v", err)
	}
	epA := &core.Endpoint{
		ID:           "ep-1",
		Provider:     "openai",
		Model:        "gpt-4",
		URL:          mockServer.URL() + "/models",
		Healthy:      true,
		ProviderImpl: openAIProvider,
		RequestTypes: []core.RequestType{core.RequestTypeResponses},
	}

	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{epA}}
	store := newChaosStateStore()
	logger, _ := zap.NewDevelopment()

	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockLoadBalancer{provider: openAIProvider},
	}
	cbManager := core.NewCircuitBreakerManager()
	ci := invoker.NewClusterInvoker(discovery, []core.Router{}, lbs, nil, cbManager, store, logger, nil)

	engineConfig := &core.EngineConfig{
		Pipelines: map[string]*core.PipelineConfig{
			"responses": {
				Name:            "responses",
				RequestTypes:    []core.RequestType{core.RequestTypeResponses},
				InboundFilters:  []string{},
				OutboundFilters: []string{"test_capture"},
				Invoker:         core.InvokerConfig{Type: "failover"},
			},
		},
	}

	engine := core.NewEngine(engineConfig, discovery, store, &mockPolicyProvider{policy: &policy.Policy{}}, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{invoker: ci})
	engine.RegisterFilter("test_capture", &testOutboundFilterWrapper{inner: &mockOutboundFilter{}})

	err = engine.Init()
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-4","stream":true}`))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	capCtx := getCapturedContext()
	if capCtx.Err == nil {
		t.Error("expected connection error post TTFT, got nil")
	}

	body := w.Body.String()
	if !strings.Contains(body, `event: response.done`) {
		t.Errorf("expected response.done event in body, got %q", body)
	}
	if !strings.Contains(body, `event: response.completed`) {
		t.Errorf("expected response.completed event in body, got %q", body)
	}
	if !strings.Contains(body, `"status":"failed"`) {
		t.Errorf("expected failed status in body, got %q", body)
	}
}

// TestChaosHarness_ResponsesStreamDisconnectWithDoneSubstring 验证当 Choices 包含 "response.done" 子串时断连
// 依然能精准判定未发真正完成事件，并写出兜底 response.done 事件
func TestChaosHarness_ResponsesStreamDisconnectWithDoneSubstring(t *testing.T) {
	mockServer := NewChaosHttpMockServer()
	defer mockServer.Close()

	openAIProvider, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "gpt-4-responses-disconnect-done-sub",
		BaseURL: mockServer.URL() + "/disconnect-post-ttft/done-substring",
		APIKey:  "test-key-openai",
		Models:  []string{"gpt-4"},
	})
	if err != nil {
		t.Fatalf("failed to create openai provider: %v", err)
	}
	epA := &core.Endpoint{
		ID:           "ep-1",
		Provider:     "openai",
		Model:        "gpt-4",
		URL:          mockServer.URL() + "/models",
		Healthy:      true,
		ProviderImpl: openAIProvider,
		RequestTypes: []core.RequestType{core.RequestTypeResponses},
	}

	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{epA}}
	store := newChaosStateStore()
	logger, _ := zap.NewDevelopment()

	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockLoadBalancer{provider: openAIProvider},
	}
	cbManager := core.NewCircuitBreakerManager()
	ci := invoker.NewClusterInvoker(discovery, []core.Router{}, lbs, nil, cbManager, store, logger, nil)

	engineConfig := &core.EngineConfig{
		Pipelines: map[string]*core.PipelineConfig{
			"responses": {
				Name:            "responses",
				RequestTypes:    []core.RequestType{core.RequestTypeResponses},
				InboundFilters:  []string{},
				OutboundFilters: []string{"test_capture"},
				Invoker:         core.InvokerConfig{Type: "failover"},
			},
		},
	}

	engine := core.NewEngine(engineConfig, discovery, store, &mockPolicyProvider{policy: &policy.Policy{}}, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{invoker: ci})
	engine.RegisterFilter("test_capture", &testOutboundFilterWrapper{inner: &mockOutboundFilter{}})

	err = engine.Init()
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-4","stream":true}`))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	capCtx := getCapturedContext()
	if capCtx.Err == nil {
		t.Error("expected connection error post TTFT, got nil")
	}

	body := w.Body.String()
	if !strings.Contains(body, `event: response.done`) {
		t.Errorf("expected response.done event in body, got %q", body)
	}
	if !strings.Contains(body, `event: response.completed`) {
		t.Errorf("expected response.completed event in body, got %q", body)
	}
	if !strings.Contains(body, `"status":"failed"`) {
		t.Errorf("expected failed status in body, got %q", body)
	}
	// 确保也含有了我们模拟的 done 子串 model output
	if !strings.Contains(body, "model output containing response.done") {
		t.Errorf("expected model output message, got %q", body)
	}
}

// TestChaosHarness_MessagesStreamDisconnect 验证在 Anthropic messages 协议流式首包后断连时：
// 网关向客户端写入规范的 Anthropic 风格的 event: error 错误事件
func TestChaosHarness_MessagesStreamDisconnect(t *testing.T) {
	mockServer := NewChaosHttpMockServer()
	defer mockServer.Close()

	openAIProvider, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "gpt-4-messages-disconnect",
		BaseURL: mockServer.URL() + "/disconnect-post-ttft",
		APIKey:  "test-key-openai",
		Models:  []string{"gpt-4"},
	})
	if err != nil {
		t.Fatalf("failed to create openai provider: %v", err)
	}
	epA := &core.Endpoint{
		ID:           "ep-1",
		Provider:     "openai",
		Model:        "gpt-4",
		URL:          mockServer.URL() + "/models",
		Healthy:      true,
		ProviderImpl: openAIProvider,
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion, core.RequestTypeMessages},
	}

	discovery := &chaosDiscovery{endpoints: []*core.Endpoint{epA}}
	store := newChaosStateStore()
	logger, _ := zap.NewDevelopment()

	lbs := map[string]core.LoadBalancer{
		"round_robin": &mockLoadBalancer{provider: openAIProvider},
	}
	cbManager := core.NewCircuitBreakerManager()
	ci := invoker.NewClusterInvoker(discovery, []core.Router{}, lbs, nil, cbManager, store, logger, nil)

	engineConfig := &core.EngineConfig{
		Pipelines: map[string]*core.PipelineConfig{
			"chat_completion": {
				Name:            "chat_completion",
				RequestTypes:    []core.RequestType{core.RequestTypeChatCompletion},
				InboundFilters:  []string{},
				OutboundFilters: []string{"test_capture"},
				Invoker:         core.InvokerConfig{Type: "failover"},
			},
			"messages": {
				Name:            "messages",
				RequestTypes:    []core.RequestType{core.RequestTypeMessages},
				InboundFilters:  []string{},
				OutboundFilters: []string{"test_capture"},
				Invoker:         core.InvokerConfig{Type: "failover"},
			},
		},
	}

	engine := core.NewEngine(engineConfig, discovery, store, &mockPolicyProvider{policy: &policy.Policy{}}, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{invoker: ci})
	engine.RegisterFilter("test_capture", &testOutboundFilterWrapper{inner: &mockOutboundFilter{}})

	err = engine.Init()
	if err != nil {
		t.Fatalf("engine init failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gpt-4","stream":true}`))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	capCtx := getCapturedContext()
	if capCtx.Err == nil {
		t.Error("expected connection error post TTFT, got nil")
	}

	body := w.Body.String()
	if !strings.Contains(body, `event: error`) {
		t.Errorf("expected event: error in body, got %q", body)
	}
	if !strings.Contains(body, `"type": "error"`) || !strings.Contains(body, `"error": {"type": "upstream_error"`) {
		t.Errorf("expected Anthropic error structure in body, got %q", body)
	}
}

type mockOutboundFilter struct{}

func (f *mockOutboundFilter) Name() string                               { return "test_capture" }
func (f *mockOutboundFilter) Order() int                                 { return 0 }
func (f *mockOutboundFilter) Criticality() core.FilterCriticality        { return core.Critical }
func (f *mockOutboundFilter) OnResponse(gctx *core.GatewayContext) error { return nil }
