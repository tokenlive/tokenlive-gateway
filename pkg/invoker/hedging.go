package invoker

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// HedgingInvoker 并行延迟对冲调用器
type HedgingInvoker struct {
	discovery         core.Discovery
	routerChain       []core.Router
	loadBalancers     map[string]core.LoadBalancer
	defaultLBStrategy string
	cbManager         *core.CircuitBreakerManager
	stateStore        core.StateStore
	logger            *zap.Logger
	fallbackInvoker   core.Invoker // 可选：当端点不足时退化到的默认串行调用器
	enableActive      bool
}

// NewHedgingInvoker 创建对冲调用器
func NewHedgingInvoker(
	discovery core.Discovery,
	routers []core.Router,
	lbs map[string]core.LoadBalancer,
	cbManager *core.CircuitBreakerManager,
	stateStore core.StateStore,
	logger *zap.Logger,
	fallback core.Invoker,
) *HedgingInvoker {
	return &HedgingInvoker{
		discovery:         discovery,
		routerChain:       routers,
		loadBalancers:     lbs,
		defaultLBStrategy: "round_robin",
		cbManager:         cbManager,
		stateStore:        stateStore,
		logger:            logger,
		fallbackInvoker:   fallback,
	}
}

// Invoke 实现 core.Invoker 接口，执行对冲双发调用
func (hi *HedgingInvoker) Invoke(gctx *core.GatewayContext) error {
	// 1. 获取端点并使用 Router 链进行路由过滤
	endpoints, err := hi.discovery.List(gctx.Ctx, gctx.Model)
	if err != nil {
		return err
	}

	gctx.Logger(hi.logger).Info("router chain: starting (hedging)",
		zap.String("model", gctx.Model),
		zap.Int("discovery_count", len(endpoints)),
		zap.Strings("discovery_endpoints", endpointIDs(endpoints)),
	)
	for _, router := range hi.routerChain {
		before := len(endpoints)
		endpoints = router.Route(gctx, endpoints)
		after := len(endpoints)
		if before != after {
			gctx.Logger(hi.logger).Info("router chain: router filtered endpoints",
				zap.String("router", router.Name()),
				zap.Int("before", before),
				zap.Int("after", after),
				zap.Strings("remaining", endpointIDs(endpoints)),
			)
		} else {
			gctx.Logger(hi.logger).Debug("router chain: router passed through",
				zap.String("router", router.Name()),
				zap.Int("count", after),
			)
		}
		if after == 0 {
			gctx.Logger(hi.logger).Warn("router chain: all endpoints eliminated by router",
				zap.String("router", router.Name()),
				zap.Int("before", before),
			)
			break
		}
	}

	// 2. 如果可用端点小于 2，或者配置不支持双发，则退化到单发/串行调用
	if len(endpoints) < 2 {
		gctx.Logger(hi.logger).Warn("hedging target endpoints less than 2, fallback to single invoker", zap.Int("endpoints", len(endpoints)))
		if hi.fallbackInvoker != nil {
			return hi.fallbackInvoker.Invoke(gctx)
		}
		return fmt.Errorf("hedging failed: less than 2 endpoints and no fallback invoker")
	}

	// 3. 动态选择两个负载均衡器推荐的端点
	epA, epB := hi.selectTwoEndpoints(gctx, endpoints)
	if epA == nil || epB == nil {
		if hi.fallbackInvoker != nil {
			return hi.fallbackInvoker.Invoke(gctx)
		}
		return fmt.Errorf("hedging failed: failed to select two endpoints")
	}

	gctx.Logger(hi.logger).Info("starting hedging calls", zap.String("epA", epA.ID), zap.String("epB", epB.ID))

	// 4. 决策对冲延迟时间 (默认 300ms)
	delay := 300 * time.Millisecond
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		if gctx.Policy.InvocationPolicy.RetryPolicy.BaseMs > 0 {
			delay = time.Duration(gctx.Policy.InvocationPolicy.RetryPolicy.BaseMs) * time.Millisecond
		}
	}

	// 5. 初始化对冲会话
	sessionCtx, sessionCancel := context.WithCancel(gctx.Ctx)
	defer sessionCancel()

	session := &hedgingSession{
		mainWriter:    gctx.ResponseWriter,
		winnerChan:    make(chan string, 2),
		failuresChan:  make(chan error, 2),
		sessionCancel: sessionCancel,
	}

	ctxA, cancelA := context.WithCancel(sessionCtx)
	defer cancelA()
	ctxB, cancelB := context.WithCancel(sessionCtx)
	defer cancelB()

	// 并发启动对 A 的子调用
	go hi.invokeSub(gctx, epA, ctxA, cancelA, session)

	// 延迟等待，如果在延迟期内 A 已经胜出，则无需启动 B
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		// A 在延迟期内未返回首字响应，正式启动对 B 的并行调用
		gctx.Logger(hi.logger).Info("delayed hedging triggered, starting sub-call B", zap.String("epB", epB.ID))
		go hi.invokeSub(gctx, epB, ctxB, cancelB, session)
	case winnerID := <-session.winnerChan:
		// A 极速胜出，直接打通
		gctx.Logger(hi.logger).Info("fast win occurred on A, skipping call B", zap.String("winner", winnerID))
	case errA := <-session.failuresChan:
		// A 发生故障报错，立即无延迟启动 B
		gctx.Logger(hi.logger).Warn("sub-call A failed early, starting sub-call B immediately", zap.Error(errA))
		go hi.invokeSub(gctx, epB, ctxB, cancelB, session)
	case <-sessionCtx.Done():
		// 会话由于外部取消
		return sessionCtx.Err()
	}

	// 6. 等待竞速终局结果 (若未确定 Winner)
	var winnerGctx *core.GatewayContext
	var finalErr error

	// 寻找最终确认的 Winner
	for {
		session.mu.Lock()
		wID := session.winnerID
		winnerGctx = session.winnerGctx
		session.mu.Unlock()

		if wID != "" {
			break
		}

		select {
		case <-session.winnerChan:
			// 胜出通知触发，再次循环确认
		case ferr := <-session.failuresChan:
			session.mu.Lock()
			if session.winnerID != "" {
				session.mu.Unlock()
				continue
			}
			session.mu.Unlock()
			// 目前对冲度为 2，如果累计发生了两次故障，则证明调用全线失败
			gctx.Logger(hi.logger).Warn("hedging sub-call encountered failure", zap.Error(ferr))
			finalErr = ferr

			// 如果两个都失败了，跳出循环
			session.mu.Lock()
			failuresCount := len(session.failuresChan) + 1 // 加上当前读取的这一个
			session.mu.Unlock()
			if failuresCount >= 2 {
				return fmt.Errorf("all hedging channels failed, last error: %w", finalErr)
			}
		case <-sessionCtx.Done():
			return sessionCtx.Err()
		}
	}

	// 7. 处理最终的胜出者响应数据与健康反馈
	session.mu.Lock()
	winnerID := session.winnerID
	if winnerID == epA.ID {
		cancelB() // 取消另一方以关闭网络连接
	} else {
		cancelA() // 取消另一方以关闭网络连接
	}
	session.mu.Unlock()

	gctx.Logger(hi.logger).Info("hedging execution finished", zap.String("winner", winnerID))

	// 同步最终数据到主 gctx
	gctx.SelectedEndpoint = winnerGctx.SelectedEndpoint
	gctx.UpstreamConnect = winnerGctx.UpstreamConnect
	gctx.UpstreamResponse = winnerGctx.UpstreamResponse
	gctx.UpstreamBody = winnerGctx.UpstreamBody
	gctx.UpstreamError = winnerGctx.UpstreamError
	gctx.TTFT = winnerGctx.TTFT
	gctx.InputTokens = winnerGctx.InputTokens
	gctx.OutputTokens = winnerGctx.OutputTokens
	gctx.Cost = winnerGctx.Cost
	gctx.Response = winnerGctx.Response

	return nil
}

// selectTwoEndpoints 利用负载均衡器选择两个独立的端点
func (hi *HedgingInvoker) selectTwoEndpoints(gctx *core.GatewayContext, endpoints []*core.Endpoint) (*core.Endpoint, *core.Endpoint) {
	var lb core.LoadBalancer
	if gctx.Policy != nil && gctx.Policy.LoadBalancePolicy != nil {
		lb = hi.loadBalancers[gctx.Policy.LoadBalancePolicy.Type]
	}
	if lb == nil {
		lb = hi.loadBalancers[hi.defaultLBStrategy]
	}
	if lb == nil {
		lb = hi.loadBalancers["round_robin"]
	}

	if lb == nil {
		return nil, nil
	}

	// 选出第一个最优端点
	invokerA := lb.Select(gctx, endpoints)
	if invokerA == nil || invokerA.Endpoint() == nil {
		return nil, nil
	}
	epA := invokerA.Endpoint()

	// 从备选列表中剔除 epA
	var remaining []*core.Endpoint
	for _, ep := range endpoints {
		if ep.ID != epA.ID {
			remaining = append(remaining, ep)
		}
	}

	if len(remaining) == 0 {
		return epA, nil
	}

	// 选出第二个最优端点
	invokerB := lb.Select(gctx, remaining)
	if invokerB == nil || invokerB.Endpoint() == nil {
		return epA, nil
	}
	return epA, invokerB.Endpoint()
}

// invokeSub 子请求协程，发起具体的上游 Provider 调用
func (hi *HedgingInvoker) invokeSub(
	gctx *core.GatewayContext,
	ep *core.Endpoint,
	ctx context.Context,
	cancel context.CancelFunc,
	session *hedgingSession,
) {
	// 创建用于子协程直写拦截的 ResponseWriter
	hw := &hedgingWriter{
		ResponseWriter: session.mainWriter,
		owner:          session,
		childCtx:       ctx,
	}

	// 复制主请求上下文，绑定可取消的 childCtx
	childGctx := &core.GatewayContext{
		Ctx:            ctx,
		Request:        gctx.Request.WithContext(ctx),
		ResponseWriter: hw,
		RawBody:        gctx.RawBody,
		RequestType:    gctx.RequestType,
		OriginalModel:  gctx.OriginalModel,
		IsStream:       gctx.IsStream,
		APIKey:         gctx.APIKey,
		UserID:         gctx.UserID,
		SessionID:      gctx.SessionID,
		Model:          gctx.Model,
		Policy:         gctx.Policy,
		Tags:           make(map[string]string),
		StartTime:      gctx.StartTime,
	}
	for k, v := range gctx.Tags {
		childGctx.Tags[k] = v
	}

	hw.childGctx = childGctx

	// 绑定当前 Endpoint 对应的 ProviderInvoker 实例
	// 我们利用 ep 关联的 ProviderImpl 去包装执行
	childGctx.SelectedEndpoint = ep
	childGctx.UpstreamConnect = time.Now()

	// 抢占半开状态下的探路许可
	serviceKey := ep.Provider + ":" + ep.Model
	if !hi.cbManager.AcquireHalfOpenPermit(serviceKey, hi.enableActive) {
		select {
		case session.failuresChan <- fmt.Errorf("service breaker half-open permit acquisition failed"):
		default:
		}
		cancel()
		return
	}
	if !hi.cbManager.AcquireHalfOpenPermit(ep.ID, hi.enableActive) {
		hi.cbManager.ReleaseHalfOpenPermit(serviceKey)
		select {
		case session.failuresChan <- fmt.Errorf("instance breaker half-open permit acquisition failed"):
		default:
		}
		cancel()
		return
	}

	// 从端点的 ProviderImpl 构建调用
	if ep != nil {
		childGctx.Logger(zap.NewNop()).Info("invoking provider endpoint (hedging)",
			zap.String("endpoint_id", ep.ID),
			zap.String("provider", ep.Provider),
			zap.String("url", ep.URL),
		)
	}
	err := ep.ProviderImpl.Invoke(childGctx)
	childGctx.RecordAttempt(err == nil)

	if err != nil {
		hi.cbManager.RecordFailure(childGctx, ep, err)
		select {
		case session.failuresChan <- err:
		default:
		}
		cancel()
		return
	}

	// 如果调用正常结束并且没有发生错误（通常为非流式返回，或流式虽读完但直写已处理）
	hi.cbManager.RecordSuccess(childGctx, ep)
	hi.stateStore.RecordLatency(childGctx.Ctx, ep.ID, time.Since(childGctx.UpstreamConnect))

	session.claimWinner(childGctx)
}

func (hi *HedgingInvoker) fallbackToSingle(gctx *core.GatewayContext, endpoints []*core.Endpoint, err error) error {
	if hi.fallbackInvoker != nil {
		return hi.fallbackInvoker.Invoke(gctx)
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("hedging failed and no fallback invoker available")
}

func (hi *HedgingInvoker) Endpoint() *core.Endpoint {
	return nil
}

// hedgingSession 集中式的对冲会话管理器
type hedgingSession struct {
	mu            sync.Mutex
	winnerID      string
	winnerGctx    *core.GatewayContext
	mainWriter    http.ResponseWriter
	winnerChan    chan string
	failuresChan  chan error
	sessionCancel context.CancelFunc
}

// claimWinner 宣誓夺冠
func (s *hedgingSession) claimWinner(childGctx *core.GatewayContext) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果已有胜出者，忽略
	if s.winnerID != "" {
		return
	}

	s.winnerID = childGctx.SelectedEndpoint.ID
	s.winnerGctx = childGctx

	// 通知主协程
	select {
	case s.winnerChan <- s.winnerID:
	default:
	}
}

// hedgingWriter 用于子请求向客户端 ResponseWriter 直写拦截的适配器
type hedgingWriter struct {
	http.ResponseWriter
	owner         *hedgingSession
	childCtx      context.Context
	childGctx     *core.GatewayContext
	mu            sync.Mutex
	isWinner      bool
	headerWritten bool
}

// Write 实现直写，第一次被调用时触发夺冠竞速
func (hw *hedgingWriter) Write(p []byte) (int, error) {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if hw.childCtx.Err() != nil {
		return 0, hw.childCtx.Err()
	}

	if hw.isWinner {
		return hw.owner.mainWriter.Write(p)
	}

	hw.owner.mu.Lock()
	// 如果其他端点已经胜出，则丢弃我们当前产生的数据
	if hw.owner.winnerID != "" && hw.owner.winnerID != hw.childGctx.SelectedEndpoint.ID {
		hw.owner.mu.Unlock()
		return len(p), nil
	}

	// 夺冠竞速：由于我们是第一个触发 Write 的子调用，我们宣誓夺冠！
	hw.isWinner = true
	hw.owner.winnerID = hw.childGctx.SelectedEndpoint.ID
	hw.owner.winnerGctx = hw.childGctx
	hw.owner.mu.Unlock()

	// 拷贝 Header 头部并写入主 ResponseWriter
	if !hw.headerWritten {
		for k, vv := range hw.ResponseWriter.Header() {
			for _, v := range vv {
				hw.owner.mainWriter.Header().Add(k, v)
			}
		}
		// 默认大模型流式输出头部设置
		hw.owner.mainWriter.Header().Set("Content-Type", "text/event-stream")
		hw.owner.mainWriter.Header().Set("Cache-Control", "no-cache")
		hw.owner.mainWriter.Header().Set("Connection", "keep-alive")

		hw.owner.mainWriter.WriteHeader(http.StatusOK)
		hw.headerWritten = true
	}

	// 发送胜出通知给主协程
	select {
	case hw.owner.winnerChan <- hw.owner.winnerID:
	default:
	}

	return hw.owner.mainWriter.Write(p)
}

// Flush 支持 http.Flusher
func (hw *hedgingWriter) Flush() {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if hw.isWinner {
		if f, ok := hw.owner.mainWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}
