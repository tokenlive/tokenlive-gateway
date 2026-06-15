package core

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// mockLimitFilter 模拟限流过滤器
type mockLimitFilter struct{}

func (f *mockLimitFilter) Name() string { return "rate_limit" }
func (f *mockLimitFilter) Order() int   { return 20 }
func (f *mockLimitFilter) OnRequest(gctx *GatewayContext) error {
	gctx.Cost = 100.0 // 模拟预扣费 100
	return nil
}

type TestSettleResult struct {
	PromptTokens     int
	CompletionTokens int
	TransmittedChars int
	IsStream         bool
}

// mockSettlementFilter 模拟计费结算过滤器
type mockSettlementFilter struct {
	settledChan chan TestSettleResult
}

func (f *mockSettlementFilter) Name() string                   { return "token_settlement" }
func (f *mockSettlementFilter) Order() int                     { return 10 }
func (f *mockSettlementFilter) Criticality() FilterCriticality { return Critical }
func (f *mockSettlementFilter) OnResponse(gctx *GatewayContext) error {
	// 触发字数估算降级结算测试逻辑
	if gctx.IsStream && gctx.OutputTokens == 0 && gctx.TransmittedChars > 0 {
		gctx.OutputTokens = int(float64(gctx.TransmittedChars) * 0.5)
	}
	f.settledChan <- TestSettleResult{
		PromptTokens:     gctx.InputTokens,
		CompletionTokens: gctx.OutputTokens,
		TransmittedChars: gctx.TransmittedChars,
		IsStream:         gctx.IsStream,
	}
	return nil
}

func TestEngine_HandleWebSocketRequest_SuccessFlow(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()

	// 1. 创建上游 Mock WebSocket 服务
	upgrader := websocket.Upgrader{}
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			mt, p, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req map[string]interface{}
			if err := json.Unmarshal(p, &req); err == nil {
				if req["type"] == "response.create" {
					createdEvent := `{"type": "response.created", "response": {"id": "resp_ws_123", "model": "gpt-4"}}`
					_ = conn.WriteMessage(mt, []byte(createdEvent))

					time.Sleep(10 * time.Millisecond)
					deltaEvent := `{"type": "response.output_text.delta", "response_id": "resp_ws_123", "delta": "Hello World"}`
					_ = conn.WriteMessage(mt, []byte(deltaEvent))

					time.Sleep(10 * time.Millisecond)
					doneEvent := `{
						"type": "response.done",
						"response": {
							"id": "resp_ws_123",
							"usage": {"input_tokens": 5, "output_tokens": 10}
						}
					}`
					_ = conn.WriteMessage(mt, []byte(doneEvent))
				}
			}
		}
	}))
	defer upstreamServer.Close()

	// 2. 注册端点
	ep := &Endpoint{
		ID:               "ep-ws-test",
		Provider:         "openai",
		Model:            "gpt-4",
		URL:              upstreamServer.URL,
		RequestTypes:     []RequestType{RequestTypeResponses},
		ProviderProtocol: "openai",
	}

	sd := NewStaticDiscovery()
	sd.RegisterService("gpt-4", []*Endpoint{ep})

	settleChan := make(chan TestSettleResult, 5)

	pipeline := &Pipeline{
		Name:            "responses",
		RequestTypes:    []RequestType{RequestTypeResponses},
		InboundFilters:  []InboundFilter{&mockLimitFilter{}},
		OutboundFilters: []OutboundFilter{&mockSettlementFilter{settledChan: settleChan}},
		Invoker:         nil,
	}

	engine := newTestEngine(map[string]*Pipeline{"responses": pipeline})
	engine.discovery = sd
	engine.stateStore = ss
	engine.logger = logger
	engine.config = &EngineConfig{
		Pipelines: map[string]*PipelineConfig{
			"responses": {
				Name:            "responses",
				RequestTypes:    []RequestType{RequestTypeResponses},
				InboundFilters:  []string{"rate_limit"},
				OutboundFilters: []string{"token_settlement"},
				Invoker: InvokerConfig{
					Type:         "cluster",
					Routers:      []string{"capability"},
					LoadBalancer: "round_robin",
				},
			},
		},
	}

	// 3. 启动网关 Mock 服务器
	gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
			engine.HandleWebSocketRequest(w, r)
			return
		}
		http.Error(w, "not websocket", http.StatusBadRequest)
	}))
	defer gatewayServer.Close()

	// 4. 客户端拨号网关
	gatewayWSURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http") + "/v1/responses?model=gpt-4"
	clientConn, _, err := websocket.DefaultDialer.Dial(gatewayWSURL, nil)
	assert.NoError(t, err)
	defer clientConn.Close()

	// 5. 客户端发送 response.create
	createReq := `{"type": "response.create", "response": {"modalities": ["text"]}}`
	err = clientConn.WriteMessage(websocket.TextMessage, []byte(createReq))
	assert.NoError(t, err)

	// 6. 客户端读取并校验事件
	var eventsReceived []string
	var doneWG sync.WaitGroup
	doneWG.Add(1)

	go func() {
		defer doneWG.Done()
		for {
			_, p, err := clientConn.ReadMessage()
			if err != nil {
				return
			}
			var header struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(p, &header); err == nil {
				eventsReceived = append(eventsReceived, header.Type)
				if header.Type == "response.done" {
					return
				}
			}
		}
	}()

	doneWG.Wait()

	// 7. 断言接收事件流
	assert.Contains(t, eventsReceived, "response.created")
	assert.Contains(t, eventsReceived, "response.output_text.delta")
	assert.Contains(t, eventsReceived, "response.done")

	// 8. 校验结算拦截器是否收到回调，且 Token 数量吻合
	select {
	case settledGctx := <-settleChan:
		assert.Equal(t, 5, settledGctx.PromptTokens)
		assert.Equal(t, 10, settledGctx.CompletionTokens)
		assert.Equal(t, len("Hello World"), settledGctx.TransmittedChars)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for settlement filter execution")
	}
}

func TestEngine_HandleWebSocketRequest_PrematureDisconnect_LengthFallback(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := newMockStateStore()

	// 1. 创建上游 Mock WebSocket 服务
	upgrader := websocket.Upgrader{}
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mt, p, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req map[string]interface{}
		if err := json.Unmarshal(p, &req); err == nil && req["type"] == "response.create" {
			_ = conn.WriteMessage(mt, []byte(`{"type": "response.created", "response": {"id": "resp_ws_disconnect", "model": "gpt-4"}}`))
			time.Sleep(10 * time.Millisecond)
			_ = conn.WriteMessage(mt, []byte(`{"type": "response.output_text.delta", "response_id": "resp_ws_disconnect", "delta": "PartialTextContent"}`))
			time.Sleep(2 * time.Second)
		}
	}))
	defer upstreamServer.Close()

	ep := &Endpoint{
		ID:               "ep-ws-disconnect",
		Provider:         "openai",
		Model:            "gpt-4",
		URL:              upstreamServer.URL,
		RequestTypes:     []RequestType{RequestTypeResponses},
		ProviderProtocol: "openai",
	}

	sd := NewStaticDiscovery()
	sd.RegisterService("gpt-4", []*Endpoint{ep})

	settleChan := make(chan TestSettleResult, 5)

	pipeline := &Pipeline{
		Name:            "responses",
		RequestTypes:    []RequestType{RequestTypeResponses},
		InboundFilters:  []InboundFilter{&mockLimitFilter{}},
		OutboundFilters: []OutboundFilter{&mockSettlementFilter{settledChan: settleChan}},
		Invoker:         nil,
	}

	engine := newTestEngine(map[string]*Pipeline{"responses": pipeline})
	engine.discovery = sd
	engine.stateStore = ss
	engine.logger = logger
	engine.config = &EngineConfig{
		Pipelines: map[string]*PipelineConfig{
			"responses": {
				Name:            "responses",
				RequestTypes:    []RequestType{RequestTypeResponses},
				InboundFilters:  []string{"rate_limit"},
				OutboundFilters: []string{"token_settlement"},
				Invoker: InvokerConfig{
					Type:         "cluster",
					Routers:      []string{"capability"},
					LoadBalancer: "round_robin",
				},
			},
		},
	}

	gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
			engine.HandleWebSocketRequest(w, r)
			return
		}
	}))
	defer gatewayServer.Close()

	// 2. 客户端连接
	gatewayWSURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http") + "/v1/responses?model=gpt-4"
	clientConn, _, err := websocket.DefaultDialer.Dial(gatewayWSURL, nil)
	assert.NoError(t, err)

	// 3. 发生 response.create
	err = clientConn.WriteMessage(websocket.TextMessage, []byte(`{"type": "response.create"}`))
	assert.NoError(t, err)

	// 4. 等待收到部分 delta
	_, p, err := clientConn.ReadMessage()
	assert.NoError(t, err)
	assert.Contains(t, string(p), "response.created")

	_, p, err = clientConn.ReadMessage()
	assert.NoError(t, err)
	assert.Contains(t, string(p), "response.output_text.delta")

	// 5. 模拟客户端异常断连
	clientConn.Close()

	// 6. 校验结算拦截器是否被触发，且通过“方案 3”进行粗估字数扣款
	select {
	case settledGctx := <-settleChan:
		assert.Equal(t, 9, settledGctx.CompletionTokens)
		assert.Equal(t, 0, settledGctx.PromptTokens)
		assert.Equal(t, len("PartialTextContent"), settledGctx.TransmittedChars)
		assert.True(t, settledGctx.IsStream)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for length fallback settlement")
	}
}
