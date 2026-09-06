package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// RouterFactory creates a Router.
type RouterFactory func(cfg RouterConfig, stateStore StateStore, logger *zap.Logger) Router

// LoadBalancerFactory creates a LoadBalancer.
type LoadBalancerFactory func(stateStore StateStore) LoadBalancer

// AliasResolver resolves model aliases to real model_code.
type AliasResolver interface {
	Resolve(ctx context.Context, model string) (string, error)
}

// RouterConfig holds Router configuration.
type RouterConfig struct {
	Name string            `yaml:"name"`
	Tags map[string]string `yaml:"tags,omitempty"`
}

// Engine assembles pipelines and orchestrates request processing.
type Engine struct {
	config          *EngineConfig
	discovery       Discovery
	pipelines       map[string]*Pipeline
	stateStore      StateStore
	cbManager       *CircuitBreakerManager // process-level circuit breaker manager with independent in-memory store
	policyProvider  policy.PolicyProvider
	logger          *zap.Logger
	filterRegistry  map[string]interface{}
	routerFactories map[string]RouterFactory
	lbFactories     map[string]LoadBalancerFactory
	mu              sync.RWMutex

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc

	// Optional components (injected via setters)
	compQueue               compensation.Queue
	providers               map[string]Provider
	staticDiscovery         *StaticDiscovery
	invokerBuilder          InvokerBuilder
	enableActiveHealthCheck bool
	aliasService            AliasResolver
	publisher               events.Publisher
}

// NewEngine creates an Engine.
func NewEngine(config *EngineConfig, discovery Discovery, stateStore StateStore, policyProvider policy.PolicyProvider, logger *zap.Logger) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	cb := NewCircuitBreakerManager()
	cb.SetLogger(logger)
	return &Engine{
		config:          config,
		discovery:       discovery,
		stateStore:      stateStore,
		cbManager:       cb,
		policyProvider:  policyProvider,
		logger:          logger,
		filterRegistry:  make(map[string]interface{}),
		routerFactories: make(map[string]RouterFactory),
		lbFactories:     make(map[string]LoadBalancerFactory),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// RegisterFilter registers a Filter to the registry, looked up by name during buildPipeline.
// Must be called before Init().
func (e *Engine) RegisterFilter(name string, filter interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.filterRegistry[name] = filter
}

// RegisterRouterFactory registers a Router factory, created by name during Init()/buildPipeline.
func (e *Engine) RegisterRouterFactory(name string, factory RouterFactory) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routerFactories[name] = factory
}

// RegisterLoadBalancerFactory registers a LoadBalancer factory, created by name during Init()/buildPipeline.
func (e *Engine) RegisterLoadBalancerFactory(name string, factory LoadBalancerFactory) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lbFactories[name] = factory
}

// SetCompQueue injects the compensation queue (optional, call before Init).
func (e *Engine) SetCompQueue(q compensation.Queue) {
	e.compQueue = q
}

// SetProviders injects Provider implementations (optional, used for HealthCheck).
func (e *Engine) SetProviders(providers map[string]Provider) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.providers = providers
}

func (e *Engine) getProviders() map[string]Provider {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.providers
}

// SetStaticDiscovery injects static discovery (optional, used for HealthCheck status updates).
func (e *Engine) SetStaticDiscovery(sd *StaticDiscovery) {
	e.staticDiscovery = sd
}

// SetInvokerBuilder injects the Invoker builder.
func (e *Engine) SetInvokerBuilder(ib InvokerBuilder) {
	e.invokerBuilder = ib
}

// SetAliasService injects the alias resolver (optional, call before Init).
func (e *Engine) SetAliasService(as AliasResolver) {
	e.aliasService = as
}

// SetPublisher injects the event publisher (optional, call before Init).
func (e *Engine) SetPublisher(pub events.Publisher) {
	e.publisher = pub
}

// Context returns the Engine lifecycle context for graceful goroutine shutdown.
func (e *Engine) Context() context.Context {
	return e.ctx
}

// Close gracefully shuts down the Engine in order: cancel → compQueue → stateStore → discovery.
func (e *Engine) Close() error {
	var errs []error
	if e.cancel != nil {
		e.cancel()
	}
	if e.compQueue != nil {
		errs = append(errs, e.compQueue.Close())
	}
	errs = append(errs, e.stateStore.Close())
	errs = append(errs, e.discovery.Close())
	return errors.Join(errs...)
}

// StartHealthCheck starts background Provider and adaptive Endpoint health check goroutines.
func (e *Engine) StartHealthCheck(ctx context.Context, interval time.Duration, enableActive bool) {
	e.enableActiveHealthCheck = enableActive
	if e.staticDiscovery == nil {
		return
	}
	e.staticDiscovery.StartHealthCheck(ctx, e.getProviders, e.cbManager, e.logger, interval, enableActive)
}

// StartCircuitBreakerProbe starts background circuit breaker state probing, periodically evaluating Open breakers and updating Redis cache.
func (e *Engine) StartCircuitBreakerProbe(ctx context.Context, interval time.Duration) {
	if e.cbManager == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.probeCircuitBreakerStates()
			}
		}
	}()
}

func (e *Engine) probeCircuitBreakerStates() {
	e.cbManager.mu.RLock()
	keys := make([]string, 0, len(e.cbManager.entries))
	for k := range e.cbManager.entries {
		keys = append(keys, k)
	}
	e.cbManager.mu.RUnlock()

	now := time.Now()
	for _, k := range keys {
		entry := e.cbManager.getEntry(k)
		oldState, newState := entry.stateVal(now)
		if oldState != newState {
			e.cbManager.onStateChange(k, oldState, newState)
		}
		// Periodic probe: refresh metrics even if state unchanged (ensures Grafana real-time visibility)
		if e.cbManager.metrics != nil {
			entry.mu.Lock()
			mc := entry.modelCode
			entry.mu.Unlock()
			if mc == "" && strings.Contains(k, ":") {
				parts := strings.Split(k, ":")
				if len(parts) > 1 {
					mc = parts[1]
				}
			}
			e.cbManager.metrics.RecordState(k, mc, newState)
		}
	}
}

// Init builds all Pipelines from config.
func (e *Engine) Init() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pipelines := make(map[string]*Pipeline)
	for name, cfg := range e.config.Pipelines {
		p, err := e.buildPipeline(cfg)
		if err != nil {
			return fmt.Errorf("build pipeline %q: %w", name, err)
		}
		pipelines[name] = p
	}
	e.pipelines = pipelines
	e.logger.Info("engine initialized", zap.Int("pipelines", len(pipelines)))
	return nil
}

// UpdateConfig atomically replaces Pipelines (hot reload).
func (e *Engine) UpdateConfig(newConfig *EngineConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pipelines := make(map[string]*Pipeline)
	for name, cfg := range newConfig.Pipelines {
		p, err := e.buildPipeline(cfg)
		if err != nil {
			return fmt.Errorf("build pipeline %q: %w", name, err)
		}
		pipelines[name] = p
	}
	e.config = newConfig
	e.pipelines = pipelines
	e.logger.Info("engine config updated", zap.Int("pipelines", len(pipelines)))
	return nil
}

// HandleRequest is the HTTP request entry point.
func (e *Engine) HandleRequest(w http.ResponseWriter, r *http.Request) {
	gctx := AcquireContext(w, r)
	defer ReleaseContext(gctx)

	// 1. Parse request
	if err := e.parseRequest(gctx); err != nil {
		e.writeError(w, http.StatusBadRequest, err, gctx)
		return
	}

	// 1.5 Alias resolution: resolve model alias to real model_code
	if e.aliasService != nil && gctx.Model != "" {
		resolved, err := e.aliasService.Resolve(gctx.Ctx, gctx.Model)
		if err != nil {
			e.writeError(w, http.StatusBadGateway, fmt.Errorf("alias resolution error: %w", err), gctx)
			return
		}
		gctx.Model = resolved
	}

	// 2. Match Pipeline
	pipe := e.matchPipeline(gctx.RequestType)
	if pipe == nil {
		e.writeError(w, http.StatusInternalServerError, fmt.Errorf("no pipeline matched for request type: %s", gctx.RequestType), gctx)
		return
	}

	// 2.5 Match & merge Policy
	if e.policyProvider != nil {
		policy, err := e.policyProvider.GetPolicy(gctx.Ctx, gctx.Tenant, gctx.UserID, gctx.Model)
		if err != nil {
			e.writeError(w, http.StatusForbidden, fmt.Errorf("policy resolution error: %w", err), gctx)
			return
		}
		gctx.Policy = policy
	}

	// 3. Execute InboundFilters
	for _, f := range pipe.InboundFilters {
		if err := f.OnRequest(gctx); err != nil {
			gctx.Err = err
			// On Inbound rejection, execute OutboundFilters that declare InboundSafe
			for _, outf := range pipe.OutboundFilters {
				if _, ok := outf.(InboundSafeFilter); ok {
					_ = outf.OnResponse(gctx)
				}
			}
			e.writeError(w, e.getErrorCode(err), err, gctx)
			return
		}
	}

	// 4. Execute Invoker (supports dynamic model fallback)
	invoker := pipe.Invoker
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.Type != "" {
		if matchedInvoker, ok := pipe.Invokers[gctx.Policy.InvocationPolicy.Type]; ok {
			invoker = matchedInvoker
		}
	}

	var invokeErr error
	fallbackPolicy := getFallbackPolicy(gctx)
	if fallbackPolicy != nil && len(fallbackPolicy.Targets) > 0 {
		models := append([]string{gctx.Model}, fallbackPolicy.Targets...)
		for i, modelName := range models {
			if i > 0 {
				gctx.Model = modelName
				gctx.FallbackChain = append(gctx.FallbackChain, modelName)
			} else {
				gctx.FallbackChain = append(gctx.FallbackChain, modelName)
			}
			invokeErr = invoker.Invoke(gctx)
			if invokeErr == nil {
				break
			}
			// First byte already sent for streaming; cannot fallback
			if gctx.TTFT > 0 {
				break
			}
			if i == len(models)-1 {
				break
			}
			if !shouldDynamicFallback(gctx, invokeErr) {
				break
			}
		}
		gctx.Err = invokeErr
	} else {
		gctx.Err = invoker.Invoke(gctx)
	}

	// 5. Execute OutboundFilters
	for _, f := range pipe.OutboundFilters {
		if ferr := f.OnResponse(gctx); ferr != nil {
			gctx.Logger(e.logger).Error("outbound filter error",
				zap.String("filter", f.Name()),
				zap.Error(ferr),
			)
			// Critical OutboundFilter failure → enqueue compensation
			if e.compQueue != nil && pipe.CriticalOutboundFilters[f.Name()] {
				e.enqueueCompensation(gctx, f.Name(), ferr)
			}
		}
	}

	// The downstream client has already gone away. This is neither an upstream
	// failure nor a response we can still deliver, so stop without synthesizing
	// an error frame.
	if errors.Is(gctx.Err, ErrClientDisconnected) {
		return
	}

	// 6. Write response
	// Detect streams that started (TTFT>0) but never emitted a protocol completion frame.
	// Responses uses response_completed_sent; Messages uses message_stop_sent.
	if gctx.Err == nil && gctx.IsStream && gctx.TTFT > 0 {
		switch gctx.RequestType {
		case RequestTypeResponses:
			if gctx.GetTagValue("response_completed_sent") != "true" {
				gctx.Err = fmt.Errorf("upstream stream closed prematurely without completion event")
			}
		case RequestTypeMessages:
			if gctx.GetTagValue("message_stop_sent") != "true" {
				gctx.Err = fmt.Errorf("upstream stream closed prematurely without completion event")
			}
		}
	}

	if gctx.Err != nil {
		// If first byte already sent, cannot append JSON error at stream tail (would cause malformed response)
		if gctx.TTFT > 0 {
			gctx.Logger(e.logger).Error("stream reading interrupted, connection silently closed", zap.Error(gctx.Err))
			if gctx.RequestType == RequestTypeResponses {
				respID := gctx.GetTagValue("response_id")
				if respID == "" {
					respID = "resp_err_interrupted"
				}
				modelName := gctx.GetTagValue("response_model")
				if modelName == "" {
					modelName = gctx.Model
				}
				now := time.Now().Unix()

				errEvent := map[string]interface{}{
					"type": "response.done",
					"response": map[string]interface{}{
						"id":         respID,
						"object":     "response",
						"created_at": now,
						"status":     "failed",
						"model":      modelName,
						"error": map[string]interface{}{
							"message": gctx.Err.Error(),
							"type":    "upstream_error",
						},
					},
				}
				if data, err := json.Marshal(errEvent); err == nil {
					payload := fmt.Sprintf("\nevent: response.done\ndata: %s\n\n", string(data))
					_, _ = fmt.Fprintf(gctx.ResponseWriter, "%s", payload)
				}

				errEvent["type"] = "response.completed"
				if data, err := json.Marshal(errEvent); err == nil {
					payload := fmt.Sprintf("\nevent: response.completed\ndata: %s\n\n", string(data))
					_, _ = fmt.Fprintf(gctx.ResponseWriter, "%s", payload)
				}
			} else {
				// Attempt to write a final SSE error frame to deliver the real error to the client
				formatter := ErrorFormatterForRequestType(gctx.RequestType)
				payload := formatter.FormatSSE(e.getErrorCode(gctx.Err), gctx.Err)
				_, _ = fmt.Fprintf(gctx.ResponseWriter, "\n%s", payload)
			}

			if flusher, ok := gctx.ResponseWriter.(http.Flusher); ok {
				flusher.Flush()
			}
			return
		}
		e.writeError(w, e.getErrorCode(gctx.Err), gctx.Err, gctx)
		return
	}

	// Stream responses are written by ProviderInvoker; non-stream needs JSON write
	if !gctx.IsStream {
		if gctx.Response != nil {
			e.writeJSON(w, gctx.Response)
		} else if gctx.UpstreamBody != nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(gctx.UpstreamBody)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(gctx.UpstreamBody)
		}
	}
}

// enqueueCompensation builds and enqueues a compensation task (failure is logged, does not block request).
func (e *Engine) enqueueCompensation(gctx *GatewayContext, filterName string, filterErr error) {
	task := &compensation.CompensationTask{
		ID:         fmt.Sprintf("%s-%s-%d", filterName, gctx.Model, time.Now().UnixNano()),
		FilterName: filterName,
		Payload: map[string]any{
			"model":       gctx.Model,
			"api_key":     gctx.APIKey,
			"endpoint_id": "",
			"error":       filterErr.Error(),
		},
		EnqueueAt: time.Now(),
		LastError: filterErr.Error(),
	}
	if gctx.SelectedEndpoint != nil {
		task.Payload["endpoint_id"] = gctx.SelectedEndpoint.ID
	}
	if qerr := e.compQueue.Enqueue(context.Background(), task); qerr != nil {
		gctx.Logger(e.logger).Error("enqueue compensation failed",
			zap.String("filter", filterName),
			zap.Error(qerr),
		)
	}
}

// ===== InvokerDependencyResolver interface implementation =====

func (e *Engine) Discovery() Discovery                          { return e.discovery }
func (e *Engine) StateStore() StateStore                        { return e.stateStore }
func (e *Engine) CircuitBreakerManager() *CircuitBreakerManager { return e.cbManager }
func (e *Engine) Logger() *zap.Logger                           { return e.logger }

func (e *Engine) ResolveRouters(names []string) []Router {
	return e.resolveRouters(names)
}

func (e *Engine) ResolveLoadBalancer(name string) LoadBalancer {
	return e.resolveLoadBalancer(name)
}

func (e *Engine) EnableActiveHealthCheck() bool {
	return e.enableActiveHealthCheck
}

func (e *Engine) Publisher() events.Publisher {
	return e.publisher
}

func getFallbackPolicy(gctx *GatewayContext) *policy.FallbackPolicy {
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil {
		return gctx.Policy.InvocationPolicy.FallbackPolicy
	}
	return nil
}

func shouldDynamicFallback(gctx *GatewayContext, err error) bool {
	if err == nil {
		return false
	}
	if gctx != nil && gctx.FatalErr != nil {
		return false
	}
	if errors.Is(err, ErrFatalNoAvailableEndpoint) {
		return false
	}
	if isAffinityNoDegrade(gctx) {
		return false
	}
	return errors.Is(err, ErrNoAvailableEndpoint)
}

func isAffinityNoDegrade(gctx *GatewayContext) bool {
	if gctx == nil || gctx.Policy == nil || gctx.Policy.LoadBalancePolicy == nil {
		return false
	}
	lbPolicy := gctx.Policy.LoadBalancePolicy
	if lbPolicy.Type == "endpoint_affinity" {
		if lbPolicy.Params != nil {
			var allowDegrade bool
			if v, ok := lbPolicy.Params["allow_degrade"]; ok {
				switch x := v.(type) {
				case bool:
					allowDegrade = x
				case string:
					allowDegrade = (x == "true")
				case float64:
					allowDegrade = (x != 0)
				case int:
					allowDegrade = (x != 0)
				}
			} else if v, ok := lbPolicy.Params["allowDegrade"]; ok {
				switch x := v.(type) {
				case bool:
					allowDegrade = x
				case string:
					allowDegrade = (x == "true")
				case float64:
					allowDegrade = (x != 0)
				case int:
					allowDegrade = (x != 0)
				}
			}
			return !allowDegrade
		}
	}
	return false
}
