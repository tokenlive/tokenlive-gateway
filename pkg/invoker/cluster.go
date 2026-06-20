package invoker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// DefaultRetryStrategy 默认全局重试策略
var DefaultRetryStrategy = &policy.RetryPolicy{
	Retry:       0,
	BackoffType: "fixed",
	BaseMs:      100,
	ErrorCodes:  []string{},
}

// ClusterInvoker 编排器：Discovery + Router + LB + retry
type ClusterInvoker struct {
	discovery         core.Discovery
	routerChain       []core.Router
	loadBalancers     map[string]core.LoadBalancer
	defaultLBStrategy string
	retryStrategy     *policy.RetryPolicy
	cbManager         *core.CircuitBreakerManager
	stateStore        core.StateStore
	logger            *zap.Logger
	enableActive      bool
	publisher         events.Publisher
}

func NewClusterInvoker(
	discovery core.Discovery,
	routers []core.Router,
	lbs map[string]core.LoadBalancer,
	retry *policy.RetryPolicy,
	cbManager *core.CircuitBreakerManager,
	stateStore core.StateStore,
	logger *zap.Logger,
	publisher events.Publisher,
) *ClusterInvoker {
	if retry == nil {
		retry = DefaultRetryStrategy
	}
	return &ClusterInvoker{
		discovery:         discovery,
		routerChain:       routers,
		loadBalancers:     lbs,
		defaultLBStrategy: "round_robin", // 默认 round_robin
		retryStrategy:     retry,
		cbManager:         cbManager,
		stateStore:        stateStore,
		logger:            logger,
		publisher:         publisher,
	}
}

// SetDefaultLBStrategy 设置默认的负载均衡策略（用于覆盖默认 of round_robin）
func (ci *ClusterInvoker) SetDefaultLBStrategy(strategy string) {
	if strategy != "" {
		ci.defaultLBStrategy = strategy
	}
}

// SetEnableActive 设置是否开启主动健康检测状态判断
func (ci *ClusterInvoker) SetEnableActive(enable bool) {
	ci.enableActive = enable
}

// RouterChain 返回路由器链，用于测试断言
func (ci *ClusterInvoker) RouterChain() []core.Router {
	return ci.routerChain
}

// Invoke 执行集群调用（带重试）
func (ci *ClusterInvoker) Invoke(gctx *core.GatewayContext) error {
	excluded := make(map[string]bool)
	var lastErr error

	var lastInvoker core.Invoker
	var lastEndpoint *core.Endpoint
	var lastConnect time.Time
	var lastResponse *http.Response
	var lastBody []byte
	var lastUpstreamErr error
	var hasPhysicalCall bool

	// 解析并应用 TotalTimeout (请求总超时，毫秒，默认非流式 60s，流式 10分钟)
	totalTimeout := 60000
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout > 0 {
		totalTimeout = gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout
	} else if gctx.IsStream {
		totalTimeout = 600000
	}
	oldCtx := gctx.Ctx
	totalCtx, totalCancel := context.WithTimeout(oldCtx, time.Duration(totalTimeout)*time.Millisecond)
	defer func() {
		totalCancel()
		gctx.Ctx = oldCtx // 恢复旧的 Context，避免影响跨模型 fallback 降级链的后续请求
	}()
	gctx.Ctx = totalCtx

	// 动态获取最大重试次数
	maxRetries := ci.retryStrategy.Retry
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		maxRetries = gctx.Policy.InvocationPolicy.RetryPolicy.Retry
	}

	// 解析重试策略（提到循环外，供 defer 闭包捕获）
	var rp *policy.RetryPolicy
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		rp = gctx.Policy.InvocationPolicy.RetryPolicy
	} else {
		rp = ci.retryStrategy
	}

	// 请求结束后按策略条件判断是否发出 invocation_fail 事件（defer 统一覆盖所有错误出口）
	defer func() {
		if lastErr != nil && !gctx.PolicyEventEmitted {
			ci.emitPolicyErrorEvents(gctx, rp, lastErr)
		}
	}()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			var backoff time.Duration
			if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
				backoff = gctx.Policy.InvocationPolicy.RetryPolicy.CalcBackoff(attempt - 1)
			} else {
				backoff = ci.retryStrategy.CalcBackoff(attempt - 1)
			}
			time.Sleep(backoff)
		}

		gctx.ResetAttempt()

		// Discovery
		endpoints, err := ci.discovery.List(gctx.Ctx, gctx.Model)
		if err != nil {
			gctx.Logger(ci.logger).Error("discovery failed", zap.Error(err))
			lastErr = err
			continue
		}

		if len(endpoints) == 0 {
			if lastErr == nil {
				lastErr = core.ErrNoAvailableEndpoint
			}
			continue
		}

		// Router chain
		gctx.Logger(ci.logger).Info("router chain: starting",
			zap.String("model", gctx.Model),
			zap.Int("discovery_count", len(endpoints)),
			zap.Strings("discovery_endpoints", endpointIDs(endpoints)),
		)
		for _, router := range ci.routerChain {
			before := len(endpoints)
			endpoints = router.Route(gctx, endpoints)
			after := len(endpoints)
			if before != after {
				gctx.Logger(ci.logger).Info("router chain: router filtered endpoints",
					zap.String("router", router.Name()),
					zap.Int("before", before),
					zap.Int("after", after),
					zap.Strings("remaining", endpointIDs(endpoints)),
				)
			} else {
				gctx.Logger(ci.logger).Debug("router chain: router passed through",
					zap.String("router", router.Name()),
					zap.Int("count", after),
				)
			}
			if after == 0 {
				gctx.Logger(ci.logger).Warn("router chain: all endpoints eliminated by router",
					zap.String("router", router.Name()),
					zap.Int("before", before),
				)
				break
			}
		}

		if len(endpoints) == 0 {
			if lastErr == nil {
				lastErr = core.ErrNoAvailableEndpoint
			}
			continue
		}

		// 过滤已排除的
		var filtered []*core.Endpoint
		for _, ep := range endpoints {
			if !excluded[ep.ID] {
				filtered = append(filtered, ep)
			}
		}

		if len(filtered) == 0 {
			if lastErr == nil {
				lastErr = core.ErrNoAvailableEndpoint
			}
			continue
		}

		// 动态选择 LoadBalancer
		var lb core.LoadBalancer
		if gctx.Policy != nil && gctx.Policy.LoadBalancePolicy != nil {
			lb = ci.loadBalancers[gctx.Policy.LoadBalancePolicy.Type]
		}
		if lb == nil {
			lb = ci.loadBalancers[ci.defaultLBStrategy]
		}
		if lb == nil {
			lb = ci.loadBalancers["round_robin"]
		}
		if lb == nil {
			// 防御性：若无 round_robin 则取 map 中任意一个
			for _, v := range ci.loadBalancers {
				lb = v
				break
			}
		}
		if lb == nil {
			lastErr = fmt.Errorf("no load balancer strategy available")
			return lastErr
		}

		// LoadBalancer 选择
		invoker := lb.Select(gctx, filtered)
		if invoker == nil {
			if lastErr == nil {
				lastErr = core.ErrNoAvailableEndpoint
			}
			continue
		}

		selectedEp := invoker.Endpoint()
		if selectedEp != nil {
			// 真正决定使用该 Endpoint 发送流量之前，先抢占可能需要的半开探路许可
			serviceKey := selectedEp.Provider + ":" + selectedEp.Model
			if !ci.cbManager.AcquireHalfOpenPermit(serviceKey, ci.enableActive) {
				excluded[selectedEp.ID] = true
				lastErr = fmt.Errorf("service breaker half-open permit acquisition failed")
				continue
			}
			if !ci.cbManager.AcquireHalfOpenPermit(selectedEp.ID, ci.enableActive) {
				ci.cbManager.ReleaseHalfOpenPermit(serviceKey)
				excluded[selectedEp.ID] = true
				lastErr = fmt.Errorf("instance breaker half-open permit acquisition failed")
				continue
			}
		}

		// 执行调用
		err = invoker.Invoke(gctx)
		gctx.RecordAttempt(err == nil)

		lastInvoker = gctx.SelectedInvoker
		lastEndpoint = gctx.SelectedEndpoint
		lastConnect = gctx.UpstreamConnect
		lastResponse = gctx.UpstreamResponse
		lastBody = gctx.UpstreamBody
		lastUpstreamErr = gctx.UpstreamError
		hasPhysicalCall = true

		if err == nil {
			isSlowCall := false
			if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
				p := gctx.Policy.CircuitBreakPolicies[0]
				if p.SlowCallMetric == "TTFT" && gctx.TTFT > 0 {
					limit := time.Duration(p.SlowCallDurationThreshold) * time.Millisecond
					if gctx.TTFT > limit {
						isSlowCall = true
					}
				}
			}

			if isSlowCall {
				ci.cbManager.RecordFailure(gctx, gctx.SelectedEndpoint, fmt.Errorf("slow call TTFT exceeded"))
			} else {
				ci.cbManager.RecordSuccess(gctx, gctx.SelectedEndpoint)
			}
			ci.stateStore.RecordLatency(gctx.Ctx, gctx.SelectedEndpoint.ID, time.Since(gctx.UpstreamConnect))
			return nil
		}

		lastErr = err

		// 打印警告日志，包含尝试次数和错误详情
		gctx.Logger(ci.logger).Warn("endpoint invocation failed",
			zap.String("endpoint", gctx.SelectedEndpoint.ID),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)

		// 记录熔断失败 (不论后续是否可以重试，该 attempt 的失败都应当记录到熔断器中)
		ci.cbManager.RecordFailure(gctx, gctx.SelectedEndpoint, err)

		// 流式已发首字节，不能重试
		if gctx.TTFT > 0 {
			return err
		}

		// 检查是否应该重试
		shouldRetry := false
		retryReason := ""
		statusCode := getStatusCode(gctx.UpstreamResponse)

		contentType := ""
		if gctx.UpstreamResponse != nil {
			contentType = gctx.UpstreamResponse.Header.Get("Content-Type")
		}
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		shouldRetry, retryReason = rp.MatchErrorWithReason(statusCode, contentType, errMsg, gctx.UpstreamBody)

		if !shouldRetry {
			return err
		}

		// 触发重试前打印详细日志
		policyType := "static"
		if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
			policyType = "dynamic"
		}
		gctx.Logger(ci.logger).Info("triggering retry strategy",
			zap.String("policy_type", policyType),
			zap.String("reason", retryReason),
			zap.Int("next_attempt", attempt+1),
		)

		// 排除该 endpoint
		excluded[gctx.SelectedEndpoint.ID] = true
	}

	if hasPhysicalCall && (gctx.SelectedEndpoint == nil || (gctx.UpstreamResponse == nil && gctx.UpstreamError == nil)) {
		gctx.SelectedInvoker = lastInvoker
		gctx.SelectedEndpoint = lastEndpoint
		gctx.UpstreamConnect = lastConnect
		gctx.UpstreamResponse = lastResponse
		gctx.UpstreamBody = lastBody
		gctx.UpstreamError = lastUpstreamErr
	}

	return lastErr
}

func getStatusCode(resp *http.Response) int {
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}

// emitPolicyErrorEvents 在请求结束后按重试策略和熔断策略的错误条件判断是否发出 invocation_fail 事件。
// 两个策略独立判断，都命中时发两条事件；Message 前缀区分来源。
func (ci *ClusterInvoker) emitPolicyErrorEvents(gctx *core.GatewayContext, rp *policy.RetryPolicy, invokeErr error) {
	if ci.publisher == nil {
		return
	}

	statusCode := getStatusCode(gctx.UpstreamResponse)
	contentType := ""
	if gctx.UpstreamResponse != nil {
		contentType = gctx.UpstreamResponse.Header.Get("Content-Type")
	}
	errMsg := ""
	if invokeErr != nil {
		errMsg = invokeErr.Error()
	}

	traceID := ""
	if gctx.Request != nil {
		traceID = gctx.Request.Header.Get("X-Trace-ID")
	}
	if traceID == "" && gctx.ResponseWriter != nil {
		traceID = gctx.ResponseWriter.Header().Get("X-Trace-Id")
	}
	requestID := ""
	if gctx.Request != nil {
		requestID = gctx.Request.Header.Get("X-Request-ID")
	}
	if requestID == "" {
		requestID = traceID
	}

	base := events.OpsEvent{
		EventType:  events.EventTypeInvocationFail,
		TenantCode: gctx.Tenant,
		ModelCode:  gctx.OriginalModel,
		RequestID:  requestID,
		TraceID:    traceID,
		Timestamp:  time.Now().Unix(),
	}
	if gctx.SelectedEndpoint != nil {
		base.EndpointID = gctx.SelectedEndpoint.ID
		base.ProviderName = gctx.SelectedEndpoint.Provider
	}

	// 1. 重试策略匹配
	if rp != nil {
		if matched, reason := rp.MatchErrorWithReason(statusCode, contentType, errMsg, gctx.UpstreamBody); matched {
			evt := base
			evt.Message = fmt.Sprintf("retry policy matched: %s", reason)
			if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil {
				evt.PolicyID = gctx.Policy.InvocationPolicy.ID
				evt.PolicyName = gctx.Policy.InvocationPolicy.Name
			}
			_ = ci.publisher.Publish(gctx.Ctx, &evt)
			gctx.PolicyEventEmitted = true
		}
	}

	// 2. 熔断策略匹配（独立调用，不改 RecordFailure）
	if gctx.Policy != nil {
		for _, cbPolicy := range gctx.Policy.CircuitBreakPolicies {
			if matched, reason := cbPolicy.MatchErrorWithReason(statusCode, contentType, errMsg, gctx.UpstreamBody); matched {
				evt := base
				evt.Message = fmt.Sprintf("circuit breaker policy matched: %s", reason)
				evt.PolicyID = cbPolicy.ID
				evt.PolicyName = cbPolicy.Name
				_ = ci.publisher.Publish(gctx.Ctx, &evt)
				gctx.PolicyEventEmitted = true
			}
		}
	}
}

func (ci *ClusterInvoker) Endpoint() *core.Endpoint {
	return nil
}

// endpointIDs 提取 endpoint ID 列表，用于日志输出
func endpointIDs(endpoints []*core.Endpoint) []string {
	ids := make([]string, len(endpoints))
	for i, ep := range endpoints {
		ids[i] = ep.ID
	}
	return ids
}
