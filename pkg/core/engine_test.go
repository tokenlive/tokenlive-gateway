package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"fmt"

	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// ===== 测试辅助 mock =====

type testInvokerBuilder struct{}

func (b *testInvokerBuilder) BuildInvoker(cfg *InvokerConfig, r InvokerDependencyResolver) (Invoker, error) {
	routers := r.ResolveRouters(cfg.Routers)
	lb := r.ResolveLoadBalancer("") // default round_robin
	return &testClusterInvoker{
		discovery:  r.Discovery(),
		routers:    routers,
		lb:         lb,
		stateStore: r.StateStore(),
	}, nil
}

type testClusterInvoker struct {
	discovery  Discovery
	routers    []Router
	lb         LoadBalancer
	stateStore StateStore
}

func (ci *testClusterInvoker) Invoke(gctx *GatewayContext) error {
	eps, err := ci.discovery.List(gctx.Ctx, gctx.Model)
	if err != nil {
		return err
	}
	for _, r := range ci.routers {
		eps = r.Route(gctx, eps)
	}
	if len(eps) == 0 {
		return fmt.Errorf("no available endpoint")
	}
	selected := ci.lb.Select(gctx, eps)
	if selected == nil {
		return fmt.Errorf("no available endpoint")
	}
	return selected.Invoke(gctx)
}

func (ci *testClusterInvoker) Endpoint() *Endpoint {
	return nil
}

// testInboundFilter 可控的 InboundFilter mock
type testInboundFilter struct {
	name     string
	order    int
	onReqErr error
	called   bool
}

func (f *testInboundFilter) Name() string { return f.name }
func (f *testInboundFilter) Order() int   { return f.order }
func (f *testInboundFilter) OnRequest(gctx *GatewayContext) error {
	f.called = true
	return f.onReqErr
}

// testOutboundFilter 可控的 OutboundFilter mock
type testOutboundFilter struct {
	name     string
	order    int
	onResErr error
	called   bool
}

func (f *testOutboundFilter) Name() string                   { return f.name }
func (f *testOutboundFilter) Order() int                     { return f.order }
func (f *testOutboundFilter) Criticality() FilterCriticality { return BestEffort }
func (f *testOutboundFilter) OnResponse(gctx *GatewayContext) error {
	f.called = true
	return f.onResErr
}

// testHTTPError 带 Code() 方法的错误，用于测试 getErrorCode 的 interface 路径
type testHTTPError struct {
	code    int
	message string
}

func (e *testHTTPError) Error() string { return e.message }
func (e *testHTTPError) Code() int     { return e.code }

// reflectHTTPError 模拟 filters.HTTPError：有 Code int 字段但无 Code() 方法
// 用于测试 getErrorCode 的 reflect 路径
type reflectHTTPError struct {
	Code    int
	Message string
}

func (e *reflectHTTPError) Error() string { return e.Message }

// newTestEngine 创建用于测试的 Engine，手动设置 pipelines
func newTestEngine(pipelines map[string]*Pipeline) *Engine {
	logger, _ := zap.NewDevelopment()
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		config:          &EngineConfig{},
		discovery:       &mockDiscovery{endpoints: []*Endpoint{{ID: "ep-1", Provider: "openai", Model: "gpt-4"}}},
		pipelines:       pipelines,
		stateStore:      newMockStateStore(),
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
		ctx:             ctx,
		cancel:          cancel,
	}
	registerTestDefaults(e)
	return e
}

// registerTestDefaults 注册测试用的默认 Router/LB 工厂
func registerTestDefaults(e *Engine) {
	e.RegisterRouterFactory("capability", func(cfg RouterConfig, _ StateStore, _ *zap.Logger) Router {
		return &testCapabilityRouter{}
	})
	e.RegisterRouterFactory("circuit_breaker", func(cfg RouterConfig, ss StateStore, logger *zap.Logger) Router {
		return &testCircuitBreakerRouter{stateStore: ss, logger: logger}
	})
	e.RegisterLoadBalancerFactory("round_robin", func(_ StateStore) LoadBalancer {
		return &testRoundRobin{}
	})
}

// ===== TestResolveRequestType =====

func TestResolveRequestType(t *testing.T) {
	tests := []struct {
		path string
		want RequestType
	}{
		{"/v1/chat/completions", RequestTypeChatCompletion},
		{"/v1/messages", RequestTypeMessages},
		{"/v1/embeddings", RequestTypeEmbedding},
		{"/v1/images/generations", RequestTypeImageGeneration},
		{"/v1/responses", RequestTypeResponses},
		{"/unknown/path", RequestTypeChatCompletion}, // default
		{"", RequestTypeChatCompletion},              // empty path -> default
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := resolveRequestType(tt.path)
			if got != tt.want {
				t.Errorf("resolveRequestType(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

// ===== TestExtractModel / TestExtractStream =====

func TestExtractModel(t *testing.T) {
	engine := &Engine{}

	tests := []struct {
		name string
		body string
		want string
	}{
		{"normal", `{"model":"gpt-4","messages":[]}`, "gpt-4"},
		{"empty model", `{"model":""}`, ""},
		{"no model field", `{"messages":[]}`, ""},
		{"invalid json", `{broken`, ""},
		{"empty body", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.extractModel([]byte(tt.body))
			if got != tt.want {
				t.Errorf("extractModel(%q) = %q, want %q", tt.body, got, tt.want)
			}
		})
	}
}

func TestExtractStream(t *testing.T) {
	engine := &Engine{}

	tests := []struct {
		name string
		body string
		want bool
	}{
		{"stream true", `{"model":"gpt-4","stream":true}`, true},
		{"stream false", `{"model":"gpt-4","stream":false}`, false},
		{"no stream field", `{"model":"gpt-4"}`, false},
		{"invalid json", `{broken`, false},
		{"empty body", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.extractStream([]byte(tt.body))
			if got != tt.want {
				t.Errorf("extractStream(%q) = %v, want %v", tt.body, got, tt.want)
			}
		})
	}
}

// ===== TestEngine_Init =====

func TestEngine_Init(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	discovery := &mockDiscovery{endpoints: []*Endpoint{{ID: "ep-1", Provider: "openai", Model: "gpt-4"}}}

	config := &EngineConfig{
		Pipelines: map[string]*PipelineConfig{
			"default": {
				Name:            "default",
				RequestTypes:    []RequestType{RequestTypeChatCompletion},
				InboundFilters:  []string{"test_inbound"},
				OutboundFilters: []string{"test_outbound"},
				Invoker: InvokerConfig{
					Type: "cluster",
				},
			},
		},
	}

	engine := NewEngine(config, discovery, ss, nil, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{})
	registerTestDefaults(engine)

	// 注册 filter
	inbound := &testInboundFilter{name: "test_inbound", order: 10}
	outbound := &testOutboundFilter{name: "test_outbound", order: 10}
	engine.RegisterFilter("test_inbound", inbound)
	engine.RegisterFilter("test_outbound", outbound)

	err := engine.Init()
	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	if len(engine.pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(engine.pipelines))
	}

	p := engine.pipelines["default"]
	if p == nil {
		t.Fatal("expected 'default' pipeline")
	}
	if p.Name != "default" {
		t.Errorf("pipeline name = %q, want %q", p.Name, "default")
	}
	if len(p.InboundFilters) != 1 {
		t.Errorf("expected 1 inbound filter, got %d", len(p.InboundFilters))
	}
	if len(p.OutboundFilters) != 1 {
		t.Errorf("expected 1 outbound filter, got %d", len(p.OutboundFilters))
	}
	if p.Invoker == nil {
		t.Error("expected Invoker to be set")
	}
}

func TestEngine_Init_MissingFilter(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	discovery := &mockDiscovery{endpoints: []*Endpoint{}}

	config := &EngineConfig{
		Pipelines: map[string]*PipelineConfig{
			"default": {
				Name:           "default",
				RequestTypes:   []RequestType{RequestTypeChatCompletion},
				InboundFilters: []string{"nonexistent"},
				Invoker:        InvokerConfig{Type: "cluster"},
			},
		},
	}

	engine := NewEngine(config, discovery, ss, nil, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{})
	registerTestDefaults(engine)
	// 不注册 filter

	err := engine.Init()
	if err == nil {
		t.Fatal("expected error for missing filter")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("expected error to mention filter name, got: %v", err)
	}
}

// ===== TestEngine_HandleRequest =====

func TestEngine_HandleRequest(t *testing.T) {
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	provider := &mockProvider{name: "openai"}
	discovery := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: discovery,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:         "default",
				RequestTypes: []RequestType{RequestTypeChatCompletion},
				Invoker: &testClusterInvoker{
					discovery:  discovery,
					routers:    []Router{},
					lb:         lb,
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestEngine_HandleRequest_NoPipeline(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()

	engine := &Engine{
		config:          &EngineConfig{},
		discovery:       &mockDiscovery{endpoints: []*Endpoint{}},
		pipelines:       map[string]*Pipeline{}, // 空 pipelines
		stateStore:      ss,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}

	// 验证响应是 JSON 错误
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected JSON response, got: %s", w.Body.String())
	}
	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected error object in response")
	}
	if !strings.Contains(errObj["message"].(string), "no pipeline") {
		t.Errorf("expected 'no pipeline' in error message, got: %v", errObj["message"])
	}
}

func TestEngine_HandleRequest_InboundFilterError(t *testing.T) {
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	provider := &mockProvider{name: "openai"}
	discovery := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()

	// 创建一个返回 401 错误的 InboundFilter
	authFilter := &testInboundFilter{
		name:     "auth",
		order:    10,
		onReqErr: &testHTTPError{code: 401, message: "unauthorized"},
	}

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: discovery,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:           "default",
				RequestTypes:   []RequestType{RequestTypeChatCompletion},
				InboundFilters: []InboundFilter{authFilter},
				Invoker: &testClusterInvoker{
					discovery:  discovery,
					routers:    []Router{},
					lb:         lb,
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	// 应该返回 401（从 InboundFilter 的错误中提取）
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", w.Code)
	}

	// 验证 filter 被调用
	if !authFilter.called {
		t.Error("expected auth filter to be called")
	}

	// 验证响应是 JSON 错误
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected JSON response, got: %s", w.Body.String())
	}
	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errObj["message"] != "unauthorized" {
		t.Errorf("expected error message 'unauthorized', got: %v", errObj["message"])
	}
}

func TestEngine_HandleRequest_OutboundFilterCalled(t *testing.T) {
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	provider := &mockProvider{name: "openai"}
	discovery := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()

	outbound := &testOutboundFilter{name: "metrics", order: 10}

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: discovery,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:            "default",
				RequestTypes:    []RequestType{RequestTypeChatCompletion},
				OutboundFilters: []OutboundFilter{outbound},
				Invoker: &testClusterInvoker{
					discovery:  discovery,
					routers:    []Router{},
					lb:         lb,
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// 验证 outbound filter 被调用
	if !outbound.called {
		t.Error("expected outbound filter to be called")
	}
}

func TestEngine_HandleRequest_InvokerError(t *testing.T) {
	// Provider 返回错误
	provider := &mockProvider{name: "openai", invokeErr: errors.New("upstream timeout")}
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	discovery := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: discovery,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:         "default",
				RequestTypes: []RequestType{RequestTypeChatCompletion},
				Invoker: &testClusterInvoker{
					discovery:  discovery,
					routers:    []Router{},
					lb:         lb,
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	// 应该返回 500（默认错误码）
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

// ===== TestEngine_UpdateConfig =====

func TestEngine_UpdateConfig(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	discovery := &mockDiscovery{endpoints: []*Endpoint{{ID: "ep-1", Provider: "openai", Model: "gpt-4"}}}

	config1 := &EngineConfig{
		Pipelines: map[string]*PipelineConfig{
			"default": {
				Name:         "default",
				RequestTypes: []RequestType{RequestTypeChatCompletion},
				Invoker:      InvokerConfig{Type: "cluster"},
			},
		},
	}

	engine := NewEngine(config1, discovery, ss, nil, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{})
	registerTestDefaults(engine)
	err := engine.Init()
	if err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	if len(engine.pipelines) != 1 {
		t.Fatalf("expected 1 pipeline after init, got %d", len(engine.pipelines))
	}

	// 更新配置：添加第二个 pipeline
	config2 := &EngineConfig{
		Pipelines: map[string]*PipelineConfig{
			"default": {
				Name:         "default",
				RequestTypes: []RequestType{RequestTypeChatCompletion},
				Invoker:      InvokerConfig{Type: "cluster"},
			},
			"embedding": {
				Name:         "embedding",
				RequestTypes: []RequestType{RequestTypeEmbedding},
				Invoker:      InvokerConfig{Type: "cluster"},
			},
		},
	}

	err = engine.UpdateConfig(config2)
	if err != nil {
		t.Fatalf("UpdateConfig() error: %v", err)
	}

	if len(engine.pipelines) != 2 {
		t.Errorf("expected 2 pipelines after update, got %d", len(engine.pipelines))
	}

	if engine.pipelines["embedding"] == nil {
		t.Error("expected 'embedding' pipeline after update")
	}

	// 验证 config 也被替换了
	if engine.config != config2 {
		t.Error("expected config to be replaced")
	}
}

// ===== TestEngine_RegisterFilter =====

func TestEngine_RegisterFilter(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	engine := NewEngine(&EngineConfig{}, nil, nil, nil, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{})

	inbound := &testInboundFilter{name: "auth", order: 10}
	outbound := &testOutboundFilter{name: "metrics", order: 30}

	engine.RegisterFilter("auth", inbound)
	engine.RegisterFilter("metrics", outbound)

	// 验证可以通过 getInboundFilter 获取
	f, ok := engine.getInboundFilter("auth")
	if !ok {
		t.Fatal("expected to find inbound filter 'auth'")
	}
	if f.Name() != "auth" {
		t.Errorf("expected filter name 'auth', got %q", f.Name())
	}

	// 验证可以通过 getOutboundFilter 获取
	of, ok := engine.getOutboundFilter("metrics")
	if !ok {
		t.Fatal("expected to find outbound filter 'metrics'")
	}
	if of.Name() != "metrics" {
		t.Errorf("expected filter name 'metrics', got %q", of.Name())
	}

	// 验证类型不匹配时返回 false
	_, ok = engine.getInboundFilter("metrics")
	if ok {
		t.Error("expected outbound filter to not be retrievable as inbound")
	}

	_, ok = engine.getOutboundFilter("auth")
	if ok {
		t.Error("expected inbound filter to not be retrievable as outbound")
	}
}

// ===== TestGetErrorCode =====

func TestGetErrorCode(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	engine := &Engine{logger: logger}

	tests := []struct {
		name string
		err  error
		want int
	}{
		{"http error with Code method", &testHTTPError{code: 401, message: "unauthorized"}, 401},
		{"http error with Code field (reflect)", &reflectHTTPError{Code: 429, Message: "rate limited"}, 429},
		{"no available endpoint", errors.New("no available endpoint"), http.StatusServiceUnavailable},
		{"all fallback exhausted", errors.New("all fallback models exhausted"), http.StatusServiceUnavailable},
		{"generic error", errors.New("something went wrong"), http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.getErrorCode(tt.err)
			if got != tt.want {
				t.Errorf("getErrorCode(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}

// ===== TestWriteError =====

func TestWriteError(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	engine := &Engine{logger: logger}

	w := httptest.NewRecorder()
	engine.writeError(w, http.StatusNotFound, errors.New("resource not found"), nil)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected JSON, got: %s", w.Body.String())
	}

	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected error object")
	}
	if errObj["message"] != "resource not found" {
		t.Errorf("expected message 'resource not found', got %v", errObj["message"])
	}
	if errObj["code"].(float64) != 404 {
		t.Errorf("expected code 404, got %v", errObj["code"])
	}
}

// ===== TestWriteJSON =====

func TestWriteJSON(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	engine := &Engine{logger: logger}

	w := httptest.NewRecorder()
	engine.writeJSON(w, map[string]string{"status": "ok"})

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected JSON, got: %s", w.Body.String())
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status 'ok', got %q", resp["status"])
	}
}

// ===== TestMatchPipeline =====

func TestMatchPipeline(t *testing.T) {
	pipelines := map[string]*Pipeline{
		"chat_completion": {
			Name:         "chat_completion",
			RequestTypes: []RequestType{RequestTypeChatCompletion},
		},
		"embedding": {
			Name:         "embedding",
			RequestTypes: []RequestType{RequestTypeEmbedding},
		},
		"default": {
			Name:         "default",
			RequestTypes: []RequestType{RequestTypeResponses},
		},
	}

	engine := newTestEngine(pipelines)

	tests := []struct {
		name       string
		reqType    RequestType
		expectPipe string
	}{
		{"chat completion", RequestTypeChatCompletion, "chat_completion"},
		{"embedding", RequestTypeEmbedding, "embedding"},
		{"responses falls to default", RequestTypeResponses, "default"},
		{"unknown falls to chat_completion fallback", RequestTypeImageGeneration, "chat_completion"}, // ImageGeneration 回退匹配至 chat_completion
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipe := engine.matchPipeline(tt.reqType)
			if pipe == nil {
				t.Fatal("expected pipeline, got nil")
			}
			if pipe.Name != tt.expectPipe {
				t.Errorf("expected pipeline %q, got %q", tt.expectPipe, pipe.Name)
			}
		})
	}
}

func TestMatchPipeline_NoDefault(t *testing.T) {
	pipelines := map[string]*Pipeline{
		"chat_completion": {
			Name:         "chat_completion",
			RequestTypes: []RequestType{RequestTypeChatCompletion},
		},
	}

	engine := newTestEngine(pipelines)

	pipe := engine.matchPipeline(RequestTypeEmbedding)
	if pipe != nil {
		t.Errorf("expected nil pipeline for unmatched request type without default, got %v", pipe)
	}
}

// ===== TestEngine_Close =====

func TestEngine_Close(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	disc := &mockDiscovery{endpoints: []*Endpoint{{ID: "ep-1", Provider: "openai"}}}

	engine := NewEngine(&EngineConfig{}, disc, ss, nil, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{})
	err := engine.Close()
	if err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestEngine_Close_WithCompQueue(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	disc := &mockDiscovery{endpoints: []*Endpoint{}}
	queue := &mockCompQueue{}

	engine := NewEngine(&EngineConfig{}, disc, ss, nil, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{})
	engine.SetCompQueue(queue)

	err := engine.Close()
	if err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if !queue.closed {
		t.Error("expected compQueue.Close() to be called")
	}
}

func TestEngine_Close_Idempotent(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	disc := &mockDiscovery{endpoints: []*Endpoint{}}

	engine := NewEngine(&EngineConfig{}, disc, ss, nil, logger)
	engine.SetInvokerBuilder(&testInvokerBuilder{})
	_ = engine.Close()
	_ = engine.Close() // 不应 panic
}

// ===== TestEngine_CriticalOutboundFilter_Compensation =====

// mockCompQueue 实现 compensation.Queue 接口
type mockCompQueue struct {
	enqueued []*compensation.CompensationTask
	closed   bool
}

func (q *mockCompQueue) Enqueue(_ context.Context, task *compensation.CompensationTask) error {
	q.enqueued = append(q.enqueued, task)
	return nil
}

func (q *mockCompQueue) Close() error {
	q.closed = true
	return nil
}

func TestEngine_CriticalOutboundFilter_EnqueuesCompensation(t *testing.T) {
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	provider := &mockProvider{name: "openai"}
	disc := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()
	queue := &mockCompQueue{}

	criticalFilter := &testOutboundFilter{name: "token_settlement", order: 10, onResErr: errors.New("settlement failed")}

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: disc,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:                    "default",
				RequestTypes:            []RequestType{RequestTypeChatCompletion},
				OutboundFilters:         []OutboundFilter{criticalFilter},
				CriticalOutboundFilters: map[string]bool{"token_settlement": true},
				Invoker: &testClusterInvoker{
					discovery:  disc,
					routers:    []Router{},
					lb:         lb,
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		compQueue:       queue,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	if len(queue.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued compensation task, got %d", len(queue.enqueued))
	}
	if queue.enqueued[0].FilterName != "token_settlement" {
		t.Errorf("expected filter 'token_settlement', got %q", queue.enqueued[0].FilterName)
	}
	if queue.enqueued[0].Payload["model"] != "gpt-4" {
		t.Errorf("expected model 'gpt-4' in payload, got %v", queue.enqueued[0].Payload["model"])
	}
}

func TestEngine_NonCriticalFilter_DoesNotEnqueue(t *testing.T) {
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	provider := &mockProvider{name: "openai"}
	disc := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()
	queue := &mockCompQueue{}

	nonCriticalFilter := &testOutboundFilter{name: "access_log", order: 10, onResErr: errors.New("log failed")}

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: disc,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:                    "default",
				RequestTypes:            []RequestType{RequestTypeChatCompletion},
				OutboundFilters:         []OutboundFilter{nonCriticalFilter},
				CriticalOutboundFilters: map[string]bool{}, // 不含 access_log
				Invoker: &testClusterInvoker{
					discovery:  disc,
					routers:    []Router{},
					lb:         lb,
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		compQueue:       queue,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	if len(queue.enqueued) != 0 {
		t.Errorf("expected 0 enqueued tasks for non-critical filter, got %d", len(queue.enqueued))
	}
}

func TestEngine_NilCompQueue_CriticalError_NoPanic(t *testing.T) {
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	provider := &mockProvider{name: "openai"}
	disc := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()

	criticalFilter := &testOutboundFilter{name: "token_settlement", order: 10, onResErr: errors.New("settlement failed")}

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: disc,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:                    "default",
				RequestTypes:            []RequestType{RequestTypeChatCompletion},
				OutboundFilters:         []OutboundFilter{criticalFilter},
				CriticalOutboundFilters: map[string]bool{"token_settlement": true},
				Invoker: &testClusterInvoker{
					discovery:  disc,
					routers:    []Router{},
					lb:         lb,
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		compQueue:       nil, // 没有补偿队列
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	// 不应 panic
	engine.HandleRequest(w, req)
}

func TestEngine_HandleRequest_CircuitBreakerDegradation(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()
	disc := &mockDiscovery{endpoints: []*Endpoint{}}

	degradePolicy := &policy.Policy{
		CircuitBreakPolicies: []*policy.CircuitBreakPolicy{
			{
				Name: "cb-gpt-4",
				DegradeConfig: &policy.DegradeConfig{
					ResponseCode: 503,
					ResponseBody: `{"error":{"message":"custom circuit broken error message","type":"gateway_error","code":"service_unavailable"}}`,
				},
			},
		},
	}

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: disc,
		pipelines: map[string]*Pipeline{
			"default": {
				Name:         "default",
				RequestTypes: []RequestType{RequestTypeChatCompletion},
				Invoker: &testClusterInvoker{
					discovery:  disc,
					routers:    []Router{},
					lb:         &mockLoadBalancer{},
					stateStore: ss,
				},
			},
		},
		stateStore:      ss,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
		policyProvider:  &mockPolicyProvider{policy: degradePolicy},
	}

	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	if w.Code != 503 {
		t.Errorf("expected status 503, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected JSON, got: %s", w.Body.String())
	}

	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected error object")
	}

	if errObj["message"] != "custom circuit broken error message" {
		t.Errorf("expected message 'custom circuit broken error message', got %v", errObj["message"])
	}
	if errObj["code"] != "service_unavailable" {
		t.Errorf("expected code 'service_unavailable', got %v", errObj["code"])
	}
}

type mockPolicyProvider struct {
	policy *policy.Policy
}

func (m *mockPolicyProvider) GetPolicy(ctx context.Context, tenantCode, userID, model string) (*policy.Policy, error) {
	return m.policy, nil
}

type testHedgingInvoker struct {
	called chan bool
}

func (hi *testHedgingInvoker) Invoke(gctx *GatewayContext) error {
	hi.called <- true
	return nil
}

func (hi *testHedgingInvoker) Endpoint() *Endpoint {
	return nil
}

func TestEngine_HandleRequest_PolymorphicInvoker(t *testing.T) {
	ep := &Endpoint{ID: "ep-1", Provider: "openai", Model: "gpt-4", RequestTypes: []RequestType{RequestTypeChatCompletion}}
	provider := &mockProvider{name: "openai"}
	discovery := &mockDiscovery{endpoints: []*Endpoint{ep}}
	lb := &mockLoadBalancer{provider: provider}
	ss := newMockStateStore()
	logger, _ := zap.NewDevelopment()

	hedgingInvoker := &testHedgingInvoker{called: make(chan bool, 1)}

	defaultInvoker := &testClusterInvoker{
		discovery:  discovery,
		routers:    []Router{},
		lb:         lb,
		stateStore: ss,
	}

	pipeline := &Pipeline{
		Name:         "default",
		RequestTypes: []RequestType{RequestTypeChatCompletion},
		Invoker:      defaultInvoker,
		Invokers: map[string]Invoker{
			"failover":     defaultInvoker,
			"test_hedging": hedgingInvoker,
		},
	}

	testPolicy := &policy.Policy{
		InvocationPolicy: &policy.InvocationPolicy{
			Type: "test_hedging",
		},
	}

	engine := &Engine{
		config:    &EngineConfig{},
		discovery: discovery,
		pipelines: map[string]*Pipeline{
			"default": pipeline,
		},
		stateStore:      ss,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
		policyProvider:  &mockPolicyProvider{policy: testPolicy},
	}

	body := `{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	w := httptest.NewRecorder()

	engine.HandleRequest(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	select {
	case called := <-hedgingInvoker.called:
		if !called {
			t.Error("expected testHedgingInvoker to be called")
		}
	default:
		t.Error("testHedgingInvoker was not called")
	}
}

type testCapabilityRouter struct{}

func (r *testCapabilityRouter) Name() string { return "capability" }
func (r *testCapabilityRouter) Route(gctx *GatewayContext, endpoints []*Endpoint) []*Endpoint {
	return endpoints
}

type testCircuitBreakerRouter struct {
	stateStore StateStore
	logger     *zap.Logger
}

func (r *testCircuitBreakerRouter) Name() string { return "circuit_breaker" }
func (r *testCircuitBreakerRouter) Route(gctx *GatewayContext, endpoints []*Endpoint) []*Endpoint {
	return endpoints
}

type testRoundRobin struct{}

func (lb *testRoundRobin) Select(gctx *GatewayContext, endpoints []*Endpoint) Invoker {
	if len(endpoints) == 0 {
		return nil
	}
	return &testRoundRobinInvoker{endpoint: endpoints[0]}
}

type testRoundRobinInvoker struct {
	endpoint *Endpoint
}

func (pi *testRoundRobinInvoker) Invoke(gctx *GatewayContext) error {
	return nil
}
func (pi *testRoundRobinInvoker) Endpoint() *Endpoint {
	return pi.endpoint
}

// ===== TestEndpointProtocol =====

func TestEndpointProtocol(t *testing.T) {
	ep := &Endpoint{ProviderProtocol: "anthropic"}
	if ep.Protocol() != ProtocolAnthropic {
		t.Errorf("expected ProtocolAnthropic, got %q", ep.Protocol())
	}
	ep2 := &Endpoint{ProviderProtocol: "openai"}
	if ep2.Protocol() != ProtocolOpenAI {
		t.Errorf("expected ProtocolOpenAI, got %q", ep2.Protocol())
	}
	ep3 := &Endpoint{ProviderProtocol: ""}
	if ep3.Protocol() != "" {
		t.Errorf("expected empty, got %q", ep3.Protocol())
	}
}

// ===== TestEngine_EndpointHealthCheck =====

func TestEngine_EndpointHealthCheck(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()

	// 1. 创建本地 Mock HTTP 服务作为探测目标
	var mu sync.Mutex
	hitCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hitCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK) // 成功
	}))
	defer server.Close()

	// 2. 构造 Endpoint
	ep := &Endpoint{
		ID:       "ep-test-health",
		Provider: "openai",
		Model:    "gpt-4",
		Metadata: map[string]string{
			"health_check_url": server.URL,
		},
	}

	sd := NewStaticDiscovery()
	sd.RegisterService("gpt-4", []*Endpoint{ep})

	engine := NewEngine(&EngineConfig{}, sd, ss, nil, logger)
	engine.SetStaticDiscovery(sd)

	// 3. 将熔断器强行设为 Open 状态
	engine.cbManager.RecordRaw(ep.ID, false, 5, 2, 1, 10*time.Second)
	engine.cbManager.RecordRaw(ep.ID, false, 5, 2, 1, 10*time.Second)

	// 验证确实是 Open
	assert.Equal(t, CircuitOpen, engine.cbManager.GetState(ep.ID))

	// 4. 手动触发 3 次健康检查
	for i := 0; i < 3; i++ {
		if val, ok := sd.endpointCheckStates.Load(ep.ID); ok {
			s := val.(*endpointCheckState)
			sd.endpointCheckStates.Store(ep.ID, &endpointCheckState{
				lastCheck:    time.Now().Add(-10 * time.Second),
				successCount: s.successCount,
			})
		}

		sd.runEndpointHealthChecks(context.Background(), engine.cbManager, logger)
		// 由于探测是异步的，我们稍微等待
		time.Sleep(50 * time.Millisecond)
	}

	// 5. 验证熔断状态已经重置为 Closed
	assert.Equal(t, CircuitClosed, engine.cbManager.GetState(ep.ID))
	mu.Lock()
	finalHitCount := hitCount
	mu.Unlock()
	assert.True(t, finalHitCount >= 3, "expected at least 3 health check hits")
}

// ===== TestEngine_HandleRequest_Responses =====

type mockResponsesProvider struct {
	name      string
	invokeErr error
}

func (p *mockResponsesProvider) Name() string       { return p.name }
func (p *mockResponsesProvider) Type() ProviderType { return ProviderOpenAI }
func (p *mockResponsesProvider) RequestTypes() []RequestType {
	return []RequestType{RequestTypeResponses}
}
func (p *mockResponsesProvider) Invoke(gctx *GatewayContext) error {
	gctx.UpstreamBody = []byte(`{"responses": "ok"}`)
	return p.invokeErr
}
func (p *mockResponsesProvider) HealthCheck(ctx context.Context) error { return nil }
func (p *mockResponsesProvider) ValidateConfig() error                 { return nil }

type mockResponsesInvoker struct {
	provider Provider
	endpoint *Endpoint
}

func (i *mockResponsesInvoker) Invoke(gctx *GatewayContext) error {
	gctx.SelectedEndpoint = i.endpoint
	return i.provider.Invoke(gctx)
}

func (i *mockResponsesInvoker) Endpoint() *Endpoint {
	return i.endpoint
}

func TestEngine_HandleRequest_Responses(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()

	// 1. 创建 mock responses provider
	provider := &mockResponsesProvider{name: "openai"}

	// 2. 注册 endpoint
	ep := &Endpoint{
		ID:           "ep-responses",
		Provider:     "openai",
		Model:        "gpt-4",
		RequestTypes: []RequestType{RequestTypeResponses},
	}
	sd := NewStaticDiscovery()
	sd.RegisterService("gpt-4", []*Endpoint{ep})

	// 3. 构建 Engine，并手动注册 responses 管道
	pipeline := &Pipeline{
		Name:         "responses",
		RequestTypes: []RequestType{RequestTypeResponses},
		Invoker: &mockResponsesInvoker{
			provider: provider,
			endpoint: ep,
		},
	}
	pipelines := map[string]*Pipeline{
		"responses": pipeline,
	}
	engine := newTestEngine(pipelines)
	engine.discovery = sd
	engine.stateStore = ss
	engine.logger = logger
	engine.providers = map[string]Provider{"openai": provider}

	// 4. 模拟请求发送
	reqBody := `{"model": "gpt-4", "prompt": "hello"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	engine.HandleRequest(rec, req)

	// 5. 验证结果
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, `{"responses": "ok"}`, rec.Body.String())
}

// ===== TestEngine_HandleRequest_Messages_Translation =====

type mockOpenAIMessagesProvider struct {
	t        *testing.T
	name     string
	expected map[string]interface{}
}

func (p *mockOpenAIMessagesProvider) Name() string       { return p.name }
func (p *mockOpenAIMessagesProvider) Type() ProviderType { return ProviderOpenAI }
func (p *mockOpenAIMessagesProvider) RequestTypes() []RequestType {
	return []RequestType{RequestTypeMessages}
}
func (p *mockOpenAIMessagesProvider) HealthCheck(ctx context.Context) error { return nil }
func (p *mockOpenAIMessagesProvider) ValidateConfig() error                 { return nil }

func (p *mockOpenAIMessagesProvider) Invoke(gctx *GatewayContext) error {
	// 1. 翻译请求体 (Anthropic -> OpenAI)
	var payload map[string]interface{}
	if err := json.Unmarshal(gctx.RawBody, &payload); err != nil {
		return fmt.Errorf("parse raw body: %w", err)
	}

	// 处理 system prompt
	var openAIMessages []interface{}
	if systemPrompt, ok := payload["system"].(string); ok && systemPrompt != "" {
		openAIMessages = append(openAIMessages, map[string]interface{}{
			"role":    "system",
			"content": systemPrompt,
		})
	}

	// 合并 messages
	if msgs, ok := payload["messages"].([]interface{}); ok {
		openAIMessages = append(openAIMessages, msgs...)
	}
	payload["messages"] = openAIMessages
	delete(payload, "system")

	// 映射 max_tokens 到 max_completion_tokens
	if maxTokens, ok := payload["max_tokens"]; ok {
		payload["max_completion_tokens"] = maxTokens
		delete(payload, "max_tokens")
	}

	newBody, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal translated body: %w", err)
	}
	gctx.RawBody = newBody

	// 验证被翻译后的请求参数
	var checkReq map[string]interface{}
	if err := json.Unmarshal(newBody, &checkReq); err != nil {
		return err
	}
	msgs, ok := checkReq["messages"].([]interface{})
	if !ok || len(msgs) != 2 {
		return fmt.Errorf("expected 2 messages (system + user), got: %d", len(msgs))
	}
	sysMsg, ok := msgs[0].(map[string]interface{})
	if !ok || sysMsg["role"] != "system" || sysMsg["content"] != "You are a helpful assistant" {
		return fmt.Errorf("invalid translated system message")
	}
	userMsg, ok := msgs[1].(map[string]interface{})
	if !ok || userMsg["role"] != "user" || userMsg["content"] != "hi" {
		return fmt.Errorf("invalid translated user message")
	}
	if checkReq["max_completion_tokens"].(float64) != 50 {
		return fmt.Errorf("expected max_completion_tokens to be 50, got: %v", checkReq["max_completion_tokens"])
	}

	// 2. 模拟上游返回的 OpenAI 响应
	openaiRespBytes := []byte(`{
		"id": "chatcmpl-mock",
		"choices": [{"message": {"role": "assistant", "content": "translated response content"}, "finish_reason": "stop"}],
		"usage": {"prompt_tokens": 12, "completion_tokens": 8}
	}`)
	gctx.UpstreamBody = openaiRespBytes

	// 3. 翻译响应体 (OpenAI -> Anthropic)
	var oaiResp struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(openaiRespBytes, &oaiResp); err != nil {
		return err
	}

	stopReason := "end_turn"
	if len(oaiResp.Choices) > 0 && oaiResp.Choices[0].FinishReason == "length" {
		stopReason = "max_tokens"
	}

	content := ""
	if len(oaiResp.Choices) > 0 {
		content = oaiResp.Choices[0].Message.Content
	}

	anthropicResp := map[string]interface{}{
		"id":    oaiResp.ID,
		"type":  "message",
		"role":  "assistant",
		"model": gctx.Model,
		"content": []map[string]interface{}{
			{
				"type": "text",
				"text": content,
			},
		},
		"stop_reason":   stopReason,
		"stop_sequence": nil,
		"usage": map[string]int{
			"input_tokens":  oaiResp.Usage.PromptTokens,
			"output_tokens": oaiResp.Usage.CompletionTokens,
		},
	}

	translatedBody, err := json.Marshal(anthropicResp)
	if err != nil {
		return err
	}

	gctx.UpstreamBody = translatedBody

	var result map[string]interface{}
	if err := json.Unmarshal(translatedBody, &result); err != nil {
		return err
	}
	gctx.Response = result

	return nil
}

type mockOpenAIMessagesInvoker struct {
	provider Provider
	endpoint *Endpoint
}

func (i *mockOpenAIMessagesInvoker) Invoke(gctx *GatewayContext) error {
	gctx.SelectedEndpoint = i.endpoint
	return i.provider.Invoke(gctx)
}

func (i *mockOpenAIMessagesInvoker) Endpoint() *Endpoint {
	return i.endpoint
}

func TestEngine_HandleRequest_Messages_Translation(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()

	provider := &mockOpenAIMessagesProvider{t: t, name: "openai"}

	ep := &Endpoint{
		ID:           "ep-messages-integration",
		Provider:     "openai",
		Model:        "gpt-4",
		RequestTypes: []RequestType{RequestTypeMessages},
	}
	sd := NewStaticDiscovery()
	sd.RegisterService("gpt-4", []*Endpoint{ep})

	pipeline := &Pipeline{
		Name:         "messages",
		RequestTypes: []RequestType{RequestTypeMessages},
		Invoker: &mockOpenAIMessagesInvoker{
			provider: provider,
			endpoint: ep,
		},
	}
	pipelines := map[string]*Pipeline{
		"messages": pipeline,
	}

	engine := newTestEngine(pipelines)
	engine.discovery = sd
	engine.stateStore = ss
	engine.logger = logger
	engine.providers = map[string]Provider{"openai": provider}

	reqBody := `{"model": "gpt-4", "system": "You are a helpful assistant", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 50}`
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	engine.HandleRequest(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	assert.NoError(t, err)

	assert.Equal(t, "message", resp["type"])
	assert.Equal(t, "assistant", resp["role"])
	contentList, ok := resp["content"].([]interface{})
	assert.True(t, ok)
	assert.Len(t, contentList, 1)
	firstContent, ok := contentList[0].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "text", firstContent["type"])
	assert.Equal(t, "translated response content", firstContent["text"])
	assert.Equal(t, "end_turn", resp["stop_reason"])
}
