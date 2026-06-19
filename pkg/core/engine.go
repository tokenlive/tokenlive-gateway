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
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// RouterFactory 创建 Router 的工厂函数
type RouterFactory func(cfg RouterConfig, stateStore StateStore, logger *zap.Logger) Router

// LoadBalancerFactory 创建 LoadBalancer 的工厂函数
type LoadBalancerFactory func(stateStore StateStore) LoadBalancer

// AliasResolver 将 model alias 解析为真实 model_code
type AliasResolver interface {
	Resolve(ctx context.Context, model string) (string, error)
}

// RouterConfig Router 配置
type RouterConfig struct {
	Name string            `yaml:"name"`
	Tags map[string]string `yaml:"tags,omitempty"`
}

// Engine 管线组装与请求处理编排器
type Engine struct {
	config          *EngineConfig
	discovery       Discovery
	pipelines       map[string]*Pipeline
	stateStore      StateStore
	cbManager       *CircuitBreakerManager // 进程级熔断管理器，基于独立内存存储
	policyProvider  policy.PolicyProvider
	logger          *zap.Logger
	filterRegistry  map[string]interface{}
	routerFactories map[string]RouterFactory
	lbFactories     map[string]LoadBalancerFactory
	mu              sync.RWMutex

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc

	// 可选组件（通过 setter 注入）
	compQueue               compensation.Queue
	providers               map[string]Provider
	staticDiscovery         *StaticDiscovery
	invokerBuilder          InvokerBuilder
	enableActiveHealthCheck bool
	aliasService            AliasResolver
}

// NewEngine 创建 Engine
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

// RegisterFilter 注册 Filter 到注册表，buildPipeline 时按名称查找。
// 必须在 Init() 之前调用。
func (e *Engine) RegisterFilter(name string, filter interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.filterRegistry[name] = filter
}

// RegisterRouterFactory 注册 Router 工厂，Init()/buildPipeline 时按名称创建 Router。
func (e *Engine) RegisterRouterFactory(name string, factory RouterFactory) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routerFactories[name] = factory
}

// RegisterLoadBalancerFactory 注册 LoadBalancer 工厂，Init()/buildPipeline 时按名称创建 LB。
func (e *Engine) RegisterLoadBalancerFactory(name string, factory LoadBalancerFactory) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lbFactories[name] = factory
}

// SetCompQueue 注入补偿队列（可选，Init 之前调用）
func (e *Engine) SetCompQueue(q compensation.Queue) {
	e.compQueue = q
}

// SetProviders 注入 Provider 实现（可选，用于 HealthCheck）
func (e *Engine) SetProviders(providers map[string]Provider) {
	e.providers = providers
}

// SetStaticDiscovery 注入静态发现（可选，用于 HealthCheck 更新健康状态）
func (e *Engine) SetStaticDiscovery(sd *StaticDiscovery) {
	e.staticDiscovery = sd
}

// SetInvokerBuilder 注入 Invoker 构造器
func (e *Engine) SetInvokerBuilder(ib InvokerBuilder) {
	e.invokerBuilder = ib
}

// SetAliasService 注入别名解析服务（可选，Init 之前调用）
func (e *Engine) SetAliasService(as AliasResolver) {
	e.aliasService = as
}

// Context 返回 Engine 的生命周期 context，用于后台 goroutine 优雅退出
func (e *Engine) Context() context.Context {
	return e.ctx
}

// Close 优雅关闭 Engine，顺序：cancel → compQueue → stateStore → discovery
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

// StartHealthCheck 启动后台 Provider 健康检查与 Endpoint 自适应健康检查 goroutine
func (e *Engine) StartHealthCheck(ctx context.Context, interval time.Duration, enableActive bool) {
	e.enableActiveHealthCheck = enableActive
	if e.staticDiscovery == nil {
		return
	}
	e.staticDiscovery.StartHealthCheck(ctx, e.providers, e.cbManager, e.logger, interval, enableActive)
}

// StartCircuitBreakerProbe 启动后台熔断状态探测，定期对 Open 状态的熔断器进行状态求值，更新 Redis 缓存
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
		// 定期刺探：即使状态未变化也刷新指标（确保 Grafana 实时性）
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

// Init 从配置构建所有 Pipeline
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

// UpdateConfig 原子替换 Pipeline（热加载）
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

// HandleRequest HTTP 请求入口
func (e *Engine) HandleRequest(w http.ResponseWriter, r *http.Request) {
	gctx := AcquireContext(w, r)
	defer ReleaseContext(gctx)

	// 1. 解析请求
	if err := e.parseRequest(gctx); err != nil {
		e.writeError(w, http.StatusBadRequest, err, gctx)
		return
	}

	// 1.5 别名解析：将 model alias 解析为真实 model_code
	if e.aliasService != nil && gctx.Model != "" {
		resolved, err := e.aliasService.Resolve(gctx.Ctx, gctx.Model)
		if err != nil {
			e.writeError(w, http.StatusBadGateway, fmt.Errorf("alias resolution error: %w", err), gctx)
			return
		}
		gctx.Model = resolved
	}

	// 2. 匹配 Pipeline
	pipe := e.matchPipeline(gctx.RequestType)
	if pipe == nil {
		e.writeError(w, http.StatusInternalServerError, fmt.Errorf("no pipeline matched for request type: %s", gctx.RequestType), gctx)
		return
	}

	// 2.5 匹配 & 合并 Policy
	if e.policyProvider != nil {
		policy, err := e.policyProvider.GetPolicy(gctx.Ctx, gctx.Tenant, gctx.UserID, gctx.Model)
		if err != nil {
			e.writeError(w, http.StatusForbidden, fmt.Errorf("policy resolution error: %w", err), gctx)
			return
		}
		gctx.Policy = policy
	}

	// 3. 执行 InboundFilters
	for _, f := range pipe.InboundFilters {
		if err := f.OnRequest(gctx); err != nil {
			gctx.Err = err
			// Inbound 拦截报错时，手动触发统计类 OutboundFilters
			for _, outf := range pipe.OutboundFilters {
				if outf.Name() == "metrics" || outf.Name() == "status_collector" || outf.Name() == "event_publisher" {
					_ = outf.OnResponse(gctx)
				}
			}
			e.writeError(w, e.getErrorCode(err), err, gctx)
			return
		}
	}

	// 4. 执行 Invoker (支持动态模型降级)
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
			}
			invokeErr = invoker.Invoke(gctx)
			if invokeErr == nil {
				break
			}
			// 流式已发首字节，不能降级
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

	// 5. 执行 OutboundFilters
	for _, f := range pipe.OutboundFilters {
		if ferr := f.OnResponse(gctx); ferr != nil {
			gctx.Logger(e.logger).Error("outbound filter error",
				zap.String("filter", f.Name()),
				zap.Error(ferr),
			)
			// Critical OutboundFilter 失败 → 补偿入队
			if e.compQueue != nil && pipe.CriticalOutboundFilters[f.Name()] {
				e.enqueueCompensation(gctx, f.Name(), ferr)
			}
		}
	}

	// 6. 写响应
	if gctx.Err == nil && gctx.RequestType == RequestTypeResponses && gctx.TTFT > 0 && gctx.GetTagValue("response_completed_sent") != "true" {
		gctx.Err = fmt.Errorf("upstream stream closed prematurely without completion event")
	}

	if gctx.Err != nil {
		// 如果首包已经发出，不能在流尾追加 JSON 错误，否则会导致客户端解析错 (malformed response)
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
					payload := fmt.Sprintf("event: response.done\ndata: %s\n\n", string(data))
					_, _ = fmt.Fprintf(gctx.ResponseWriter, "%s", payload)
				}

				errEvent["type"] = "response.completed"
				if data, err := json.Marshal(errEvent); err == nil {
					payload := fmt.Sprintf("event: response.completed\ndata: %s\n\n", string(data))
					_, _ = fmt.Fprintf(gctx.ResponseWriter, "%s", payload)
				}
			} else {
				// 尝试写入最后一帧 SSE error 事件，把真实错误带给客户端
				formatter := ErrorFormatterForRequestType(gctx.RequestType)
				payload := formatter.FormatSSE(e.getErrorCode(gctx.Err), gctx.Err)
				_, _ = fmt.Fprintf(gctx.ResponseWriter, "%s", payload)
			}

			if flusher, ok := gctx.ResponseWriter.(http.Flusher); ok {
				flusher.Flush()
			}
			return
		}
		e.writeError(w, e.getErrorCode(gctx.Err), gctx.Err, gctx)
		return
	}

	// 流式响应已由 ProviderInvoker 写入，非流式需要写 JSON
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

// enqueueCompensation 构建补偿任务并入队（失败仅日志，不阻塞请求）
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

// ===== InvokerDependencyResolver 接口实现 =====

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

	if gctx == nil {
		return true
	}

	// 特殊放行：如果是由于服务发现或熔断导致“无可用端点”，属于模型级灾难，无条件允许降级
	if errors.Is(err, ErrNoAvailableEndpoint) {
		return true
	}

	// 1. 获取当前合并策略中的重试策略
	var rp *policy.RetryPolicy
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		rp = gctx.Policy.InvocationPolicy.RetryPolicy
	}

	// 2. 如果配置了重试策略，使用重试策略匹配机制进行评判
	if rp != nil {
		statusCode := 0
		if gctx.UpstreamResponse != nil {
			statusCode = gctx.UpstreamResponse.StatusCode
		}
		contentType := ""
		if gctx.UpstreamResponse != nil {
			contentType = gctx.UpstreamResponse.Header.Get("Content-Type")
		}
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		return rp.MatchError(statusCode, contentType, errMsg, gctx.UpstreamBody)
	}

	// 3. 严格按照配置执行：若无显式重试策略，默认允许所有错误均能降级，防止产生硬编码误解
	return true
}
