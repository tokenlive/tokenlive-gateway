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

// HedgingInvoker issues delayed parallel (hedged) calls.
type HedgingInvoker struct {
	discovery         core.Discovery
	routerChain       []core.Router
	loadBalancers     map[string]core.LoadBalancer
	defaultLBStrategy string
	cbManager         *core.CircuitBreakerManager
	stateStore        core.StateStore
	logger            *zap.Logger
	fallbackInvoker   core.Invoker // serial fallback when dual-call is not possible
	enableActive      bool
}

// NewHedgingInvoker creates a HedgingInvoker.
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

// Invoke implements core.Invoker with delayed dual-call hedging.
func (hi *HedgingInvoker) Invoke(gctx *core.GatewayContext) error {
	// Discover and filter via router chain
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

	// Need ≥2 endpoints for dual-call; otherwise fall back to serial
	if len(endpoints) < 2 {
		gctx.Logger(hi.logger).Warn("hedging target endpoints less than 2, fallback to single invoker", zap.Int("endpoints", len(endpoints)))
		if hi.fallbackInvoker != nil {
			return hi.fallbackInvoker.Invoke(gctx)
		}
		return fmt.Errorf("hedging failed: less than 2 endpoints and no fallback invoker")
	}

	epA, epB := hi.selectTwoEndpoints(gctx, endpoints)
	if epA == nil || epB == nil {
		if hi.fallbackInvoker != nil {
			return hi.fallbackInvoker.Invoke(gctx)
		}
		return fmt.Errorf("hedging failed: failed to select two endpoints")
	}

	gctx.Logger(hi.logger).Info("starting hedging calls", zap.String("epA", epA.ID), zap.String("epB", epB.ID))

	// Hedge delay (default 300ms)
	delay := 300 * time.Millisecond
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		if gctx.Policy.InvocationPolicy.RetryPolicy.BaseMs > 0 {
			delay = time.Duration(gctx.Policy.InvocationPolicy.RetryPolicy.BaseMs) * time.Millisecond
		}
	}

	sessionCtx, sessionCancel := context.WithCancel(gctx.Ctx)
	defer sessionCancel()

	session := &hedgingSession{
		mainWriter:    gctx.ResponseWriter,
		winnerChan:    make(chan string, 2),
		failuresChan:  make(chan error, 2),
		sessionCancel: sessionCancel,
		completed:     map[string]chan struct{}{epA.ID: make(chan struct{}), epB.ID: make(chan struct{})},
	}

	ctxA, cancelA := context.WithCancel(sessionCtx)
	defer cancelA()
	ctxB, cancelB := context.WithCancel(sessionCtx)
	defer cancelB()

	// Sub-calls can outlive Invoke after the loser is canceled. Never let them
	// read a pooled caller context while its owner is resetting it.
	requestBase := *gctx
	requestBase.Tags = make(map[string]string, len(gctx.Tags))
	for key, value := range gctx.Tags {
		requestBase.Tags[key] = value
	}
	go hi.invokeSub(&requestBase, epA, ctxA, cancelA, session)

	// Wait for delay; skip B if A already won
	timer := time.NewTimer(delay)
	defer timer.Stop()
	failuresCount := 0

	select {
	case <-timer.C:
		// A did not win in time; start B in parallel
		gctx.Logger(hi.logger).Info("delayed hedging triggered, starting sub-call B", zap.String("epB", epB.ID))
		go hi.invokeSub(&requestBase, epB, ctxB, cancelB, session)
	case winnerID := <-session.winnerChan:
		// A won fast; skip B
		gctx.Logger(hi.logger).Info("fast win occurred on A, skipping call B", zap.String("winner", winnerID))
	case errA := <-session.failuresChan:
		failuresCount++
		if err := session.smartTerminalError(gctx); err != nil {
			return err
		}
		// A failed early; start B immediately
		gctx.Logger(hi.logger).Warn("sub-call A failed early, starting sub-call B immediately", zap.Error(errA))
		go hi.invokeSub(&requestBase, epB, ctxB, cancelB, session)
	case <-sessionCtx.Done():
		return sessionCtx.Err()
	}

	// Wait for race outcome if winner not yet set
	var winnerGctx *core.GatewayContext
	var finalErr error

	for {
		if err := session.smartTerminalError(gctx); err != nil {
			return err
		}
		session.mu.Lock()
		wID := session.winnerID
		winnerGctx = session.winnerGctx
		session.mu.Unlock()

		if wID != "" {
			break
		}

		select {
		case <-session.winnerChan:
			// Winner signaled; re-check
		case ferr := <-session.failuresChan:
			if err := session.smartTerminalError(gctx); err != nil {
				return err
			}
			session.mu.Lock()
			if session.winnerID != "" {
				session.mu.Unlock()
				continue
			}
			session.mu.Unlock()
			// Degree-2 hedge: two failures means total failure
			gctx.Logger(hi.logger).Warn("hedging sub-call encountered failure", zap.Error(ferr))
			finalErr = ferr

			failuresCount++
			if failuresCount >= 2 {
				session.mu.Lock()
				if failed := session.lastFailure; failed != nil {
					gctx.UpstreamResponse = failed.UpstreamResponse
					gctx.UpstreamError = failed.UpstreamError
					gctx.UpstreamBody = failed.UpstreamBody
				}
				session.mu.Unlock()
				return fmt.Errorf("all hedging channels failed, last error: %w", finalErr)
			}
		case <-sessionCtx.Done():
			return sessionCtx.Err()
		}
	}

	// Cancel the loser and apply winner state
	session.mu.Lock()
	winnerID := session.winnerID
	if winnerID == epA.ID {
		cancelB()
	} else {
		cancelA()
	}
	session.mu.Unlock()

	// Streaming chooses a winner at the first byte, before that provider has
	// finished writing usage/history. Let the winner finish before reading it.
	select {
	case <-session.completed[winnerID]:
	case <-sessionCtx.Done():
		return sessionCtx.Err()
	}
	gctx.Logger(hi.logger).Info("hedging execution finished", zap.String("winner", winnerID))

	// Copy winner fields onto main gctx
	gctx.SelectedEndpoint = winnerGctx.SelectedEndpoint
	gctx.SelectedInvoker = winnerGctx.SelectedInvoker
	gctx.History = append(gctx.History, winnerGctx.History...)
	gctx.AttemptCount += winnerGctx.AttemptCount
	gctx.UpstreamConnect = winnerGctx.UpstreamConnect
	gctx.UpstreamResponse = winnerGctx.UpstreamResponse
	gctx.UpstreamBody = winnerGctx.UpstreamBody
	gctx.UpstreamError = winnerGctx.UpstreamError
	gctx.TTFT = winnerGctx.TTFT
	gctx.InputTokens = winnerGctx.InputTokens
	gctx.OutputTokens = winnerGctx.OutputTokens
	gctx.CachedTokens = winnerGctx.CachedTokens
	gctx.CacheCreationTokens = winnerGctx.CacheCreationTokens
	gctx.TransmittedChars = winnerGctx.TransmittedChars
	gctx.Cost = winnerGctx.Cost
	gctx.Response = winnerGctx.Response

	return winnerGctx.UpstreamError
}

// selectTwoEndpoints picks two distinct endpoints via the load balancer.
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

	invokerA := lb.Select(gctx, endpoints)
	if invokerA == nil || invokerA.Endpoint() == nil {
		return nil, nil
	}
	epA := invokerA.Endpoint()

	var remaining []*core.Endpoint
	for _, ep := range endpoints {
		if ep.ID != epA.ID {
			remaining = append(remaining, ep)
		}
	}

	if len(remaining) == 0 {
		return epA, nil
	}

	invokerB := lb.Select(gctx, remaining)
	if invokerB == nil || invokerB.Endpoint() == nil {
		return epA, nil
	}
	return epA, invokerB.Endpoint()
}

// invokeSub runs one hedged sub-call against an endpoint.
func (hi *HedgingInvoker) invokeSub(
	gctx *core.GatewayContext,
	ep *core.Endpoint,
	ctx context.Context,
	cancel context.CancelFunc,
	session *hedgingSession,
) {
	defer func() {
		if done := session.completed[ep.ID]; done != nil {
			close(done)
		}
	}()
	hw := &hedgingWriter{
		ResponseWriter: session.mainWriter,
		owner:          session,
		childCtx:       ctx,
	}

	// Clone request context with cancellable childCtx
	childGctx := &core.GatewayContext{
		Ctx:               ctx,
		Request:           gctx.Request.WithContext(ctx),
		GovernanceRequest: gctx.GovernanceRequest,
		ResponseWriter:    hw,
		RawBody:           gctx.RawBody,
		RequestType:       gctx.RequestType,
		OriginalModel:     gctx.OriginalModel,
		IsStream:          gctx.IsStream,
		APIKey:            gctx.APIKey,
		Tenant:            gctx.Tenant,
		UserTenant:        gctx.UserTenant,
		WorkspaceID:       gctx.WorkspaceID,
		Sensitive:         gctx.Sensitive,
		MaxResponseBytes:  gctx.MaxResponseBytes,
		UserID:            gctx.UserID,
		SessionID:         gctx.SessionID,
		Model:             gctx.Model,
		Policy:            gctx.Policy,
		Tags:              make(map[string]string),
		StartTime:         gctx.StartTime,
	}
	for k, v := range gctx.Tags {
		childGctx.Tags[k] = v
	}

	hw.childGctx = childGctx

	childGctx.SelectedEndpoint = ep
	childGctx.UpstreamConnect = time.Now()

	// Acquire half-open probe permits
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

	if ep != nil {
		childGctx.Logger(zap.NewNop()).Info("invoking provider endpoint (hedging)",
			zap.String("endpoint_id", ep.ID),
			zap.String("provider", ep.Provider),
			zap.String("url", ep.URL),
		)
	}
	err := NewProviderInvoker(ep.ProviderImpl, ep).Invoke(childGctx)
	if err != nil && childGctx.UpstreamError == nil {
		childGctx.UpstreamError = err
	}
	childGctx.RecordAttempt(err == nil)

	if err != nil {
		session.mu.Lock()
		session.lastFailure = childGctx
		if childGctx.Sensitive && session.terminalFailure == nil &&
			(isExplicitlyNonRetryable(err) || childGctx.UpstreamResponse != nil &&
				childGctx.UpstreamResponse.StatusCode >= 400 && childGctx.UpstreamResponse.StatusCode < 500 &&
				childGctx.UpstreamResponse.StatusCode != http.StatusTooManyRequests) {
			session.terminalFailure = childGctx
		}
		session.mu.Unlock()
		hi.cbManager.RecordFailure(childGctx, ep, err)
		select {
		case session.failuresChan <- err:
		default:
		}
		cancel()
		return
	}

	// Success (non-stream complete, or stream already flushed via writer)
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

// hedgingSession coordinates a dual-call hedge race.
type hedgingSession struct {
	mu              sync.Mutex
	winnerID        string
	winnerGctx      *core.GatewayContext
	lastFailure     *core.GatewayContext
	terminalFailure *core.GatewayContext
	mainWriter      http.ResponseWriter
	winnerChan      chan string
	failuresChan    chan error
	sessionCancel   context.CancelFunc
	completed       map[string]chan struct{}
}

func (s *hedgingSession) smartTerminalError(g *core.GatewayContext) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	failed := s.terminalFailure
	if failed == nil {
		return nil
	}
	g.UpstreamResponse, g.UpstreamError, g.UpstreamBody = failed.UpstreamResponse, failed.UpstreamError, failed.UpstreamBody
	code := http.StatusBadRequest
	if failed.UpstreamResponse != nil {
		code = failed.UpstreamResponse.StatusCode
	}
	return &smartHTTPError{status: code, reason: "upstream rejected smart routing request"}
}

// claimWinner marks this child as the race winner (first-write wins).
func (s *hedgingSession) claimWinner(childGctx *core.GatewayContext) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.winnerID != "" {
		return
	}

	s.winnerID = childGctx.SelectedEndpoint.ID
	s.winnerGctx = childGctx

	select {
	case s.winnerChan <- s.winnerID:
	default:
	}
}

// hedgingWriter intercepts sub-call writes to the client ResponseWriter.
type hedgingWriter struct {
	http.ResponseWriter
	owner         *hedgingSession
	childCtx      context.Context
	childGctx     *core.GatewayContext
	mu            sync.Mutex
	isWinner      bool
	headerWritten bool
}

// Write claims the race on first write, then forwards to the main writer.
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
	// Another endpoint already won: discard our bytes
	if hw.owner.winnerID != "" && hw.owner.winnerID != hw.childGctx.SelectedEndpoint.ID {
		hw.owner.mu.Unlock()
		return len(p), nil
	}

	// First Write wins the race
	hw.isWinner = true
	hw.owner.winnerID = hw.childGctx.SelectedEndpoint.ID
	hw.owner.winnerGctx = hw.childGctx
	hw.owner.mu.Unlock()

	if !hw.headerWritten {
		for k, vv := range hw.ResponseWriter.Header() {
			for _, v := range vv {
				hw.owner.mainWriter.Header().Add(k, v)
			}
		}
		// Default SSE headers for streaming
		hw.owner.mainWriter.Header().Set("Content-Type", "text/event-stream")
		hw.owner.mainWriter.Header().Set("Cache-Control", "no-cache")
		hw.owner.mainWriter.Header().Set("Connection", "keep-alive")

		hw.owner.mainWriter.WriteHeader(http.StatusOK)
		hw.headerWritten = true
	}

	select {
	case hw.owner.winnerChan <- hw.owner.winnerID:
	default:
	}

	return hw.owner.mainWriter.Write(p)
}

// Flush implements http.Flusher for the winning writer.
func (hw *hedgingWriter) Flush() {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if hw.isWinner {
		if f, ok := hw.owner.mainWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}
