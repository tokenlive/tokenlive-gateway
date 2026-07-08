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

// 失败惩罚延迟的默认参数。
const (
	defaultFailurePenalty  = 3.0              // 历史平均 × 3
	defaultFailureMax      = 30 * time.Second // 惩罚上限
	minFailurePenalty      = 1.0              // 倍数下限，>= 1 以免"奖赏"失败
	defaultLatencyWindowLL = 5 * time.Minute  // 最低延迟策略默认窗口
)

// recordFailurePenalty 将失败请求作为"代理延迟"写入延迟统计序列。
// 代理延迟 = 该端点历史平均 × 惩罚倍数（无样本用上限值），写入对应 metric 的序列
// （total 写 RecordLatency，ttft 写 RecordTTFT）。可经 latency_failure_penalty=0 关闭。
// 这样失败端点的 latency 统计不再虚低，避免恢复期被 least_latency 盲目选中。
func (ci *ClusterInvoker) recordFailurePenalty(gctx *core.GatewayContext) {
	if gctx == nil || gctx.SelectedEndpoint == nil {
		return
	}
	params := lbParams(gctx)
	multiplier, maxPenalty := resolveFailurePenaltyConfig(params)
	if multiplier == 0 {
		// 显式关闭失败计入
		return
	}
	if multiplier < minFailurePenalty {
		multiplier = minFailurePenalty
	}

	window, metric := resolveLatencyConfig(params)
	epID := gctx.SelectedEndpoint.ID

	// 历史平均读取：按 metric 选序列
	histAvg := func() (time.Duration, error) {
		if metric == "ttft" {
			return ci.stateStore.GetAvgTTFT(gctx.Ctx, epID, window)
		}
		return ci.stateStore.GetAvgLatency(gctx.Ctx, epID, window)
	}
	avg, err := histAvg()
	if err != nil || avg <= 0 {
		// 无历史样本，用上限值作为惩罚
		ci.writePenalty(gctx, metric, maxPenalty)
		return
	}
	penalty := time.Duration(float64(avg) * multiplier)
	if penalty > maxPenalty {
		penalty = maxPenalty
	}
	ci.writePenalty(gctx, metric, penalty)
}

// writePenalty 按指标把惩罚延迟写入对应序列。
func (ci *ClusterInvoker) writePenalty(gctx *core.GatewayContext, metric string, penalty time.Duration) {
	epID := gctx.SelectedEndpoint.ID
	if metric == "ttft" {
		if err := ci.stateStore.RecordTTFT(gctx.Ctx, epID, penalty); err != nil {
			gctx.Logger(ci.logger).Warn("record ttft penalty failed",
				zap.String("endpoint", epID), zap.Error(err))
		}
		return
	}
	if err := ci.stateStore.RecordLatency(gctx.Ctx, epID, penalty); err != nil {
		gctx.Logger(ci.logger).Warn("record latency penalty failed",
			zap.String("endpoint", epID), zap.Error(err))
	}
}

// lbParams 安全取出 LoadBalancePolicy.Params。
func lbParams(gctx *core.GatewayContext) map[string]interface{} {
	if gctx == nil || gctx.Policy == nil || gctx.Policy.LoadBalancePolicy == nil {
		return nil
	}
	return gctx.Policy.LoadBalancePolicy.Params
}

// resolveLatencyConfig 从 Params 读取 latency_window（默认 5min）与 latency_metric（默认 total）。
func resolveLatencyConfig(params map[string]interface{}) (window time.Duration, metric string) {
	window = defaultLatencyWindowLL
	metric = "total"
	if params == nil {
		return
	}
	if v, ok := params["latency_window"]; ok {
		switch x := v.(type) {
		case float64:
			if x > 0 {
				window = time.Duration(x) * time.Second
			}
		case int:
			if x > 0 {
				window = time.Duration(x) * time.Second
			}
		case string:
			if d, err := time.ParseDuration(x); err == nil && d > 0 {
				window = d
			}
		}
	}
	if v, ok := params["latency_metric"]; ok {
		if s, ok := v.(string); ok && (s == "ttft" || s == "total") {
			metric = s
		}
	}
	return
}

// resolveFailurePenaltyConfig 从 Params 读取失败惩罚倍数与上限。
// multiplier=0 表示显式关闭失败计入。
func resolveFailurePenaltyConfig(params map[string]interface{}) (multiplier float64, maxPenalty time.Duration) {
	multiplier = defaultFailurePenalty
	maxPenalty = defaultFailureMax
	if params == nil {
		return
	}
	if v, ok := params["latency_failure_penalty"]; ok {
		switch x := v.(type) {
		case float64:
			multiplier = x
		case int:
			multiplier = float64(x)
		}
	}
	if v, ok := params["latency_failure_max"]; ok {
		switch x := v.(type) {
		case float64:
			if x > 0 {
				maxPenalty = time.Duration(x) * time.Second
			}
		case int:
			if x > 0 {
				maxPenalty = time.Duration(x) * time.Second
			}
		case string:
			if d, err := time.ParseDuration(x); err == nil && d > 0 {
				maxPenalty = d
			}
		}
	}
	return
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
	var lastSelectedEndpointID string

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

	maxAttempts := maxRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
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
			return lastErr
		}

		if len(endpoints) == 0 {
			lastErr = core.ErrNoAvailableEndpoint
			return lastErr
		}

		// 1. 优先过滤在此次调用历史中已经失败的局部排除端点 (excluded)
		var filtered []*core.Endpoint
		for _, ep := range endpoints {
			if !excluded[ep.ID] {
				filtered = append(filtered, ep)
			}
		}

		if len(filtered) == 0 {
			lastErr = core.ErrNoAvailableEndpoint
			return lastErr
		}

		// 2. 运行 Router chain 过滤熔断、优先级等
		gctx.Logger(ci.logger).Info("router chain: starting",
			zap.String("model", gctx.Model),
			zap.Int("filtered_count", len(filtered)),
			zap.Strings("filtered_endpoints", endpointIDs(filtered)),
		)
		for _, router := range ci.routerChain {
			before := len(filtered)
			filtered = router.Route(gctx, filtered)
			after := len(filtered)
			if before != after {
				gctx.Logger(ci.logger).Info("router chain: router filtered endpoints",
					zap.String("router", router.Name()),
					zap.Int("before", before),
					zap.Int("after", after),
					zap.Strings("remaining", endpointIDs(filtered)),
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
		if len(filtered) == 0 {
			lastErr = core.ErrNoAvailableEndpoint
			return lastErr
		}

		// 动态选择 LoadBalancer
		var lb core.LoadBalancer
		lbStrategy := ci.defaultLBStrategy
		if gctx.Policy != nil && gctx.Policy.LoadBalancePolicy != nil {
			lbStrategy = gctx.Policy.LoadBalancePolicy.Type
			lb = ci.loadBalancers[lbStrategy]
		}
		if lb == nil {
			lbStrategy = ci.defaultLBStrategy
			lb = ci.loadBalancers[ci.defaultLBStrategy]
		}
		if lb == nil {
			lbStrategy = "round_robin"
			lb = ci.loadBalancers["round_robin"]
		}
		if lb == nil {
			// 防御性：若无 round_robin 则取 map 中任意一个
			for name, v := range ci.loadBalancers {
				lbStrategy = name
				lb = v
				break
			}
		}
		if lb == nil {
			lastErr = fmt.Errorf("no load balancer strategy available")
			return lastErr
		}

		// LoadBalancer 选择
		var invoker core.Invoker
		if lbStrategy == "round_robin" && lastSelectedEndpointID != "" {
			nextEp := nextEndpointAfter(endpoints, excluded, lastSelectedEndpointID)
			if nextEp == nil {
				lastErr = core.ErrNoAvailableEndpoint
				return lastErr
			}
			if nextEp.ProviderImpl != nil {
				invoker = NewProviderInvoker(nextEp.ProviderImpl, nextEp)
			} else {
				invoker = lb.Select(gctx, []*core.Endpoint{nextEp})
			}
		} else {
			invoker = lb.Select(gctx, filtered)
		}
		if invoker == nil {
			lastErr = core.ErrNoAvailableEndpoint
			return lastErr
		}

		selectedEp := invoker.Endpoint()
		if selectedEp != nil {
			lastSelectedEndpointID = selectedEp.ID
			// 真正决定使用该 Endpoint 发送流量之前，先抢占可能需要的半开探路许可
			serviceKey := selectedEp.Provider + ":" + selectedEp.Model
			if !ci.cbManager.AcquireHalfOpenPermit(serviceKey, ci.enableActive) {
				excluded[selectedEp.ID] = true
				lastErr = fmt.Errorf("service breaker half-open permit acquisition failed")
				if attempt+1 >= maxAttempts {
					return lastErr
				}
				continue
			}
			if !ci.cbManager.AcquireHalfOpenPermit(selectedEp.ID, ci.enableActive) {
				ci.cbManager.ReleaseHalfOpenPermit(serviceKey)
				excluded[selectedEp.ID] = true
				lastErr = fmt.Errorf("instance breaker half-open permit acquisition failed")
				if attempt+1 >= maxAttempts {
					return lastErr
				}
				continue
			}
		}

		// 执行调用
		err = invoker.Invoke(gctx)
		if err != nil && gctx.UpstreamError == nil {
			gctx.UpstreamError = err
		}
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
			var slowReason string
			if gctx.Policy != nil {
				for _, p := range gctx.Policy.CircuitBreakPolicies {
					if p.SlowCallMetric == "TTFT" && gctx.TTFT > 0 {
						limit := time.Duration(p.SlowCallDurationThreshold) * time.Millisecond
						if gctx.TTFT > limit {
							isSlowCall = true
							slowReason = "slow call TTFT exceeded"
							break
						}
					} else if p.SlowCallMetric == "RTT" || p.SlowCallMetric == "Duration" {
						rtt := time.Since(gctx.UpstreamConnect)
						limit := time.Duration(p.SlowCallDurationThreshold) * time.Millisecond
						if rtt > limit {
							isSlowCall = true
							slowReason = "slow call RTT exceeded"
							break
						}
					}
				}
			}

			if isSlowCall {
				ci.cbManager.RecordFailure(gctx, gctx.SelectedEndpoint, fmt.Errorf("%s", slowReason))
			} else {
				ci.cbManager.RecordSuccess(gctx, gctx.SelectedEndpoint)
			}
			ci.stateStore.RecordLatency(gctx.Ctx, gctx.SelectedEndpoint.ID, time.Since(gctx.UpstreamConnect))
			// 流式请求记录首包耗时到独立 TTFT 序列，供 latency_metric=ttft 的最低延迟策略查询。
			// 非流式请求 TTFT 为 0，跳过。
			if gctx.TTFT > 0 {
				if err := ci.stateStore.RecordTTFT(gctx.Ctx, gctx.SelectedEndpoint.ID, gctx.TTFT); err != nil {
					gctx.Logger(ci.logger).Warn("record ttft failed",
						zap.String("endpoint", gctx.SelectedEndpoint.ID),
						zap.Error(err),
					)
				}
			}
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

		// 失败计入延迟统计：用"历史平均×惩罚倍数"作为代理延迟写入，
		// 避免失败端点 latency 统计虚低、恢复期被盲目选中。可经 latency_failure_penalty=0 关闭。
		ci.recordFailurePenalty(gctx)

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

		excluded[gctx.SelectedEndpoint.ID] = true

		if attempt+1 >= maxAttempts {
			return err
		}

		if !hasRemainingEndpoint(endpoints, excluded) {
			return core.ErrNoAvailableEndpoint
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

func hasRemainingEndpoint(endpoints []*core.Endpoint, excluded map[string]bool) bool {
	for _, ep := range endpoints {
		if !excluded[ep.ID] {
			return true
		}
	}
	return false
}

func nextEndpointAfter(endpoints []*core.Endpoint, excluded map[string]bool, previousID string) *core.Endpoint {
	if len(endpoints) == 0 {
		return nil
	}

	start := -1
	for i, ep := range endpoints {
		if ep.ID == previousID {
			start = i
			break
		}
	}
	for step := 1; step <= len(endpoints); step++ {
		ep := endpoints[(start+step)%len(endpoints)]
		if !excluded[ep.ID] {
			return ep
		}
	}
	return nil
}
