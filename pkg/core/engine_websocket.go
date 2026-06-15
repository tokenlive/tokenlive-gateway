package core

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var (
	wsTempCounter int64
	wsTempMu      sync.Mutex
)

func generateTempID() string {
	wsTempMu.Lock()
	defer wsTempMu.Unlock()
	wsTempCounter++
	return fmt.Sprintf("temp_turn_%d_%d", time.Now().UnixNano(), wsTempCounter)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// HandleWebSocketRequest 处理 WebSocket 双向流 Responses 协议请求
func (e *Engine) HandleWebSocketRequest(w http.ResponseWriter, r *http.Request) {
	// 1. 握手前解析 model 参数
	modelName := r.URL.Query().Get("model")
	if modelName == "" {
		e.logger.Error("ws handshake failed: missing model query parameter")
		http.Error(w, "missing model query parameter", http.StatusBadRequest)
		return
	}

	// 2. 初始化握手上下文并前置校验
	gctx := AcquireContext(w, r)
	defer ReleaseContext(gctx)

	gctx.RequestType = RequestTypeResponses
	gctx.Model = modelName
	gctx.OriginalModel = modelName

	// 提取中间件注入的 Tenant 和 UserID
	if tenant, ok := r.Context().Value("tenant").(string); ok {
		gctx.Tenant = tenant
	} else if tenantStr := r.Header.Get("X-Tenant-Id"); tenantStr != "" {
		gctx.Tenant = tenantStr
	}

	if userID, ok := r.Context().Value("user_id").(string); ok {
		gctx.UserID = userID
	} else if userStr := r.Header.Get("X-User-Id"); userStr != "" {
		gctx.UserID = userStr
	}

	if userTenant, ok := r.Context().Value("user_tenant").(string); ok {
		gctx.UserTenant = userTenant
	} else if utStr := r.Header.Get("X-User-Tenant"); utStr != "" {
		gctx.UserTenant = utStr
	}

	// 匹配管线
	pipe := e.matchPipeline(gctx.RequestType)
	if pipe == nil {
		e.logger.Error("no pipeline matched for responses ws", zap.String("model", modelName))
		http.Error(w, "no pipeline matched", http.StatusInternalServerError)
		return
	}

	// 解析策略
	if e.policyProvider != nil {
		policy, err := e.policyProvider.GetPolicy(gctx.Ctx, gctx.Tenant, gctx.UserID, gctx.Model)
		if err != nil {
			e.logger.Error("policy resolution failed in ws handshake", zap.Error(err))
			http.Error(w, "policy resolution failed", http.StatusForbidden)
			return
		}
		gctx.Policy = policy
	}

	// 执行 auth 过滤器
	for _, f := range pipe.InboundFilters {
		if f.Name() == "auth" {
			if err := f.OnRequest(gctx); err != nil {
				e.logger.Error("auth filter rejected ws handshake", zap.Error(err))
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}
	}

	// 3. 路由选择端点并向主上游发起握手建连 (支持握手重试)
	upstreamConn, selectedEp, err := e.selectEndpointAndConnect(gctx, pipe)
	if err != nil {
		e.logger.Error("failed to connect to upstream ws", zap.Error(err))
		http.Error(w, fmt.Sprintf("failed to connect to upstream: %v", err), http.StatusBadGateway)
		return
	}

	// 4. 升级客户端连接
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		upstreamConn.Close()
		e.logger.Error("client websocket upgrade failed", zap.Error(err))
		return
	}

	// 5. 包装并发安全的连接与初始化追踪器
	safeClient := NewSafeWebSocketConn(clientConn)
	safeUpstream := NewSafeWebSocketConn(upstreamConn)
	tracker := NewWSSessionTracker()

	e.logger.Info("responses ws session established",
		zap.String("model", modelName),
		zap.String("tenant", gctx.Tenant),
		zap.String("user_id", gctx.UserID),
		zap.String("endpoint", selectedEp.ID),
	)

	// 双向透传生命周期控制
	sessionCtx, cancel := context.WithCancel(gctx.Ctx)
	defer cancel()

	errChan := make(chan error, 2)

	// Goroutine 1: Client -> Upstream
	go func() {
		defer func() {
			if r := recover(); r != nil {
				e.logger.Error("panic in client to upstream goroutine", zap.Any("panic", r))
			}
		}()
		for {
			select {
			case <-sessionCtx.Done():
				return
			default:
				msgType, payload, err := safeClient.ReadMessage()
				if err != nil {
					errChan <- fmt.Errorf("read client: %w", err)
					return
				}

				// 拦截并审计 client 的帧 (主要是 response.create)
				intercepted, err := e.handleClientEvent(payload, gctx, pipe, tracker, safeClient)
				if err != nil {
					// 拦截报错直接下发给客户端，不转发给上游
					errEvent := map[string]interface{}{
						"type": "error",
						"error": map[string]interface{}{
							"type":    "invalid_request_error",
							"message": err.Error(),
						},
					}
					if data, errBytes := json.Marshal(errEvent); errBytes == nil {
						_ = safeClient.WriteMessage(websocket.TextMessage, data)
					}
					continue
				}

				if !intercepted {
					err = safeUpstream.WriteMessage(msgType, payload)
					if err != nil {
						errChan <- fmt.Errorf("write upstream: %w", err)
						return
					}
				}
			}
		}
	}()

	// Goroutine 2: Upstream -> Client
	go func() {
		defer func() {
			if r := recover(); r != nil {
				e.logger.Error("panic in upstream to client goroutine", zap.Any("panic", r))
			}
		}()
		for {
			select {
			case <-sessionCtx.Done():
				return
			default:
				msgType, payload, err := safeUpstream.ReadMessage()
				if err != nil {
					errChan <- fmt.Errorf("read upstream: %w", err)
					return
				}

				// 拦截并审计 upstream 的帧
				e.handleUpstreamEvent(payload, tracker, pipe)

				err = safeClient.WriteMessage(msgType, payload)
				if err != nil {
					errChan <- fmt.Errorf("write client: %w", err)
					return
				}
			}
		}
	}()

	// 阻塞等待连接关闭或协程退出
	select {
	case sessionErr := <-errChan:
		e.logger.Warn("responses ws session terminated", zap.Error(sessionErr))
	case <-gctx.Ctx.Done():
		e.logger.Info("responses ws session context done")
	}

	// 6. 连接断开清理
	safeClient.Close()
	safeUpstream.Close()

	// 7. 处理异常断连下的未决 Turn 结算 (方案 3)
	unsettledTurns := tracker.GetUnsettledTurns()
	for _, turn := range unsettledTurns {
		e.logger.Warn("settling unsettled turn due to premature client disconnect",
			zap.String("response_id", turn.ResponseID),
			zap.Int("transmitted_chars", turn.SentTextTokens),
		)

		if turn.Gctx != nil {
			// 将 Gctx 标识为 Stream 结算
			turn.Gctx.IsStream = true
			// SentTextTokens 累计下发字节数，此处做最终字数降级扣费
			turn.Gctx.TransmittedChars = turn.SentTextTokens

			// 执行 Outbound 结算，由结算拦截器自动处理
			for _, f := range pipe.OutboundFilters {
				_ = f.OnResponse(turn.Gctx)
			}
			ReleaseContext(turn.Gctx)
		}
	}
}

// selectEndpointAndConnect 路由、负载均衡选择端点，并完成拨号 (包含握手重试)
func (e *Engine) selectEndpointAndConnect(gctx *GatewayContext, pipe *Pipeline) (*websocket.Conn, *Endpoint, error) {
	routerNames := []string{"capability", "circuit_breaker"}
	lbStrategy := "round_robin"

	e.mu.RLock()
	if e.config != nil && e.config.Pipelines != nil {
		if pipeCfg, exists := e.config.Pipelines[pipe.Name]; exists {
			if len(pipeCfg.Invoker.Routers) > 0 {
				routerNames = pipeCfg.Invoker.Routers
			}
			if pipeCfg.Invoker.LoadBalancer != "" {
				lbStrategy = pipeCfg.Invoker.LoadBalancer
			}
		}
	}
	e.mu.RUnlock()

	// 最大重试次数
	maxRetries := 2
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		maxRetries = gctx.Policy.InvocationPolicy.RetryPolicy.Retry
	}

	routers := e.resolveRouters(routerNames)
	excluded := make(map[string]bool)
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		gctx.ResetAttempt()

		// 1. Discovery
		endpoints, err := e.discovery.List(gctx.Ctx, gctx.Model)
		if err != nil {
			lastErr = err
			continue
		}
		if len(endpoints) == 0 {
			lastErr = fmt.Errorf("no available endpoint")
			continue
		}

		// 2. Router Chain
		for _, router := range routers {
			endpoints = router.Route(gctx, endpoints)
			if len(endpoints) == 0 {
				break
			}
		}

		// 3. 过滤已故障/已重试的 Endpoint
		var filtered []*Endpoint
		for _, ep := range endpoints {
			if !excluded[ep.ID] {
				filtered = append(filtered, ep)
			}
		}
		if len(filtered) == 0 {
			lastErr = fmt.Errorf("all endpoints filtered out")
			continue
		}

		// 4. Load Balancer 选择
		lb := e.resolveLoadBalancer(lbStrategy)
		if lb == nil {
			lastErr = fmt.Errorf("no load balancer strategy %q available", lbStrategy)
			return nil, nil, lastErr
		}

		invoker := lb.Select(gctx, filtered)
		if invoker == nil {
			lastErr = fmt.Errorf("load balancer selected nil")
			continue
		}
		selectedEp := invoker.Endpoint()
		if selectedEp == nil {
			lastErr = fmt.Errorf("load balancer selected invoker with nil endpoint")
			continue
		}

		// 5. 将选中的端点 URL 转换为 WSS 格式
		wssURL, err := convertHTTPToWS(selectedEp.URL, selectedEp.ProviderProtocol, selectedEp.Model)
		if err != nil {
			excluded[selectedEp.ID] = true
			lastErr = err
			continue
		}

		// 构建鉴权 Header，使用网关该 Endpoint 对应的真实 APIKey，防止凭证泄漏
		headers := make(http.Header)
		if selectedEp.APIKey != "" {
			headers.Set("Authorization", "Bearer "+selectedEp.APIKey)
		}

		// 6. 进行 Dial 握手
		dialer := websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
		}
		e.logger.Info("dialing upstream ws", zap.String("endpoint_id", selectedEp.ID), zap.String("url", wssURL))
		conn, _, err := dialer.DialContext(gctx.Ctx, wssURL, headers)
		if err != nil {
			excluded[selectedEp.ID] = true
			lastErr = err
			e.logger.Warn("dial upstream ws failed, retrying", zap.String("endpoint_id", selectedEp.ID), zap.Error(err))
			continue
		}

		gctx.SelectedEndpoint = selectedEp
		return conn, selectedEp, nil
	}

	return nil, nil, fmt.Errorf("ws handshake failed after retries: %w", lastErr)
}

// handleClientEvent 拦截解析客户端消息，执行交互级 Inbound 拦截 (如限流和额度预扣)
func (e *Engine) handleClientEvent(payload []byte, parentGctx *GatewayContext, pipe *Pipeline, tracker *WSSessionTracker, safeClient *SafeWebSocketConn) (bool, error) {
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &header); err != nil {
		e.logger.Warn("failed to parse client message json", zap.Error(err))
		return false, nil // 无法解析的帧不拦截，原样传给上游处理
	}

	e.logger.Info("handleClientEvent received event", zap.String("type", header.Type))

	if header.Type == "response.create" {
		// 1. 初始化独立的 Turn 上下文
		turnCtx := AcquireContext(parentGctx.ResponseWriter, parentGctx.Request)
		turnCtx.Ctx = parentGctx.Ctx
		turnCtx.Tenant = parentGctx.Tenant
		turnCtx.UserID = parentGctx.UserID
		turnCtx.UserTenant = parentGctx.UserTenant
		turnCtx.Policy = parentGctx.Policy
		turnCtx.Model = parentGctx.Model
		turnCtx.RequestType = RequestTypeResponses
		turnCtx.RawBody = payload

		// 2. 执行 Inbound 过滤器进行限流和预扣费
		var filterErr error
		for _, f := range pipe.InboundFilters {
			if err := f.OnRequest(turnCtx); err != nil {
				filterErr = err
				break
			}
		}

		if filterErr != nil {
			e.logger.Warn("filter validation failed on response.create", zap.Error(filterErr))
			ReleaseContext(turnCtx)
			return true, filterErr // 拦截该帧，不发给上游，并抛出错误事件以写回客户端
		}

		// 3. 校验通过，在 Tracker 中建立未决 Turn 跟踪
		tempID := generateTempID()
		turn := &ActiveTurn{
			TempID:        tempID,
			Tenant:        turnCtx.Tenant,
			UserID:        turnCtx.UserID,
			Model:         turnCtx.Model,
			StartTime:     time.Now(),
			PrePaidAmount: turnCtx.Cost,
			Gctx:          turnCtx, // 保存 GatewayContext 资源指针，用于后续 Done 时实扣和多退少补
		}
		tracker.AddTurn(tempID, turn)
		e.logger.Info("added temp turn to tracker", zap.String("temp_id", tempID))
	}

	return false, nil
}

// handleUpstreamEvent 拦截解析上游回传帧，管理 Response ID 状态绑定与流式字节数统计
func (e *Engine) handleUpstreamEvent(payload []byte, tracker *WSSessionTracker, pipe *Pipeline) {
	var ev struct {
		Type       string `json:"type"`
		ResponseID string `json:"response_id"`
		Response   struct {
			ID    string `json:"id"`
			Model string `json:"model"`
			Usage struct {
				InputTokens  int `json:"input_tokens"`
				OutputTokens int `json:"output_tokens"`
			} `json:"usage"`
		} `json:"response"`
		Delta string `json:"delta"`
		Part  struct {
			Delta string `json:"delta"`
		} `json:"part"`
	}

	if err := json.Unmarshal(payload, &ev); err != nil {
		e.logger.Warn("failed to parse upstream message json", zap.Error(err))
		return
	}

	e.logger.Info("handleUpstreamEvent received event", zap.String("type", ev.Type), zap.String("resp_id", ev.Response.ID), zap.String("resp_id_outer", ev.ResponseID))

	// 1. response.created: 将最早的临时 ID 与正式的 response.id 关联
	if ev.Type == "response.created" && ev.Response.ID != "" {
		turn := tracker.AssociateLatestTempID(ev.Response.ID)
		if turn != nil {
			e.logger.Info("associated temp turn to official response id", zap.String("temp_id", turn.TempID), zap.String("official_id", ev.Response.ID))
		} else {
			e.logger.Warn("failed to find temp turn to associate with", zap.String("official_id", ev.Response.ID))
		}
	}

	// 2. *.delta 事件: 统计下发流量用于异常断连粗估计费
	if strings.Contains(ev.Type, ".delta") {
		textDelta := ev.Delta
		if textDelta == "" && ev.Part.Delta != "" {
			textDelta = ev.Part.Delta
		}

		responseID := ev.ResponseID
		if responseID == "" {
			responseID = ev.Response.ID
		}

		if responseID != "" && len(textDelta) > 0 {
			if turn := tracker.GetTurn(responseID); turn != nil {
				turn.SentTextTokens += len(textDelta)
				if turn.Gctx != nil {
					turn.Gctx.TransmittedChars += len(textDelta)
				}
				e.logger.Info("updated turn sent text tokens", zap.String("response_id", responseID), zap.Int("added", len(textDelta)), zap.Int("total", turn.SentTextTokens))
			} else {
				e.logger.Warn("failed to find turn for delta update", zap.String("response_id", responseID))
			}
		}
	}

	// 3. response.done: 触发最终的 Outbound 差额结算
	if ev.Type == "response.done" {
		responseID := ev.Response.ID
		if responseID == "" {
			responseID = ev.ResponseID
		}

		if responseID != "" {
			if turn := tracker.RemoveTurn(responseID); turn != nil {
				turn.IsSettled = true
				if turn.Gctx != nil {
					// 填充最终的使用量
					turn.Gctx.InputTokens = ev.Response.Usage.InputTokens
					turn.Gctx.OutputTokens = ev.Response.Usage.OutputTokens

					e.logger.Info("triggering outbound filters on response.done", zap.String("response_id", responseID), zap.Int("input", turn.Gctx.InputTokens), zap.Int("output", turn.Gctx.OutputTokens))
					// 触发 Outbound 链对该交互进行多退少补的结算
					for _, f := range pipe.OutboundFilters {
						_ = f.OnResponse(turn.Gctx)
					}
					ReleaseContext(turn.Gctx)
				}
			} else {
				e.logger.Warn("failed to find turn for response.done settlement", zap.String("response_id", responseID))
			}
		}
	}
}

// convertHTTPToWS 将 HTTP(S) 的 Endpoint URL 转换为 WS(S) 格式，并拼装 Responses 协议查询路径
func convertHTTPToWS(httpURL string, protocol string, model string) (string, error) {
	wsURL := httpURL
	if strings.HasPrefix(wsURL, "https://") {
		wsURL = "wss://" + wsURL[8:]
	} else if strings.HasPrefix(wsURL, "http://") {
		wsURL = "ws://" + wsURL[7:]
	}

	wsURL = strings.TrimSuffix(wsURL, "/")

	if protocol == "openai" {
		wsURL = wsURL + "/responses?model=" + model
	} else {
		wsURL = wsURL + "/responses?model=" + model
	}
	return wsURL, nil
}
