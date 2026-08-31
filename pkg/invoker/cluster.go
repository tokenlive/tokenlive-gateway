package invoker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// DefaultRetryStrategy is the global default retry policy.
var DefaultRetryStrategy = &policy.RetryPolicy{
	Retry:       0,
	BackoffType: "fixed",
	BaseMs:      100,
	ErrorCodes:  []string{},
}

// ClusterInvoker orchestrates Discovery + Router + LB + retry.
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
		defaultLBStrategy: "round_robin",
		retryStrategy:     retry,
		cbManager:         cbManager,
		stateStore:        stateStore,
		logger:            logger,
		publisher:         publisher,
	}
}

// SetDefaultLBStrategy overrides the default LB strategy (round_robin).
func (ci *ClusterInvoker) SetDefaultLBStrategy(strategy string) {
	if strategy != "" {
		ci.defaultLBStrategy = strategy
	}
}

// SetEnableActive enables active health-check status in routing decisions.
func (ci *ClusterInvoker) SetEnableActive(enable bool) {
	ci.enableActive = enable
}

// Default failure-penalty parameters for latency stats.
const (
	defaultFailurePenalty  = 3.0              // hist avg × 3
	defaultFailureMax      = 30 * time.Second // penalty cap
	minFailurePenalty      = 1.0              // floor so failure is never "rewarded"
	defaultLatencyWindowLL = 5 * time.Minute  // default window for least_latency
)

// recordFailurePenalty records a failed call as a synthetic latency sample.
// Penalty = endpoint hist avg × multiplier (or maxPenalty if no samples).
// Written to RecordLatency (total) or RecordTTFT (ttft). Disable with latency_failure_penalty=0.
// Keeps failed endpoints from looking artificially fast under least_latency during recovery.
func (ci *ClusterInvoker) recordFailurePenalty(gctx *core.GatewayContext) {
	if gctx == nil || gctx.SelectedEndpoint == nil {
		return
	}
	params := lbParams(gctx)
	multiplier, maxPenalty := resolveFailurePenaltyConfig(params)
	if multiplier == 0 {
		// Explicitly disabled
		return
	}
	if multiplier < minFailurePenalty {
		multiplier = minFailurePenalty
	}

	window, metric := resolveLatencyConfig(params)
	epID := gctx.SelectedEndpoint.ID

	histAvg := func() (time.Duration, error) {
		if metric == "ttft" {
			return ci.stateStore.GetAvgTTFT(gctx.Ctx, epID, window)
		}
		return ci.stateStore.GetAvgLatency(gctx.Ctx, epID, window)
	}
	avg, err := histAvg()
	if err != nil || avg <= 0 {
		// No history: use max penalty
		ci.writePenalty(gctx, metric, maxPenalty)
		return
	}
	penalty := time.Duration(float64(avg) * multiplier)
	if penalty > maxPenalty {
		penalty = maxPenalty
	}
	ci.writePenalty(gctx, metric, penalty)
}

// writePenalty writes the penalty into the series for the given metric.
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

// lbParams safely returns LoadBalancePolicy.Params.
func lbParams(gctx *core.GatewayContext) map[string]interface{} {
	if gctx == nil || gctx.Policy == nil || gctx.Policy.LoadBalancePolicy == nil {
		return nil
	}
	return gctx.Policy.LoadBalancePolicy.Params
}

// resolveLatencyConfig reads latency_window (default 5m) and latency_metric (default total).
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

// resolveFailurePenaltyConfig reads failure penalty multiplier and max from Params.
// multiplier=0 disables failure-as-latency recording.
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

// RouterChain returns the router chain (for tests).
func (ci *ClusterInvoker) RouterChain() []core.Router {
	return ci.routerChain
}

// Invoke runs a cluster call with retry.
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

	// TotalTimeout in ms: default 60s non-stream, 10m stream
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
		// Restore so cross-model fallback chain is not affected
		gctx.Ctx = oldCtx
	}()
	gctx.Ctx = totalCtx

	maxRetries := ci.retryStrategy.Retry
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		maxRetries = gctx.Policy.InvocationPolicy.RetryPolicy.Retry
	}

	// Resolve retry policy outside the loop for defer capture
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

		// Drop endpoints already failed in this request
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

		// Router chain (circuit breaker, priority, etc.)
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

		if gctx.FatalErr != nil {
			return gctx.FatalErr
		}

		// Pick LoadBalancer dynamically
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
			// Fallback: any registered LB
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

		var invoker core.Invoker
		if lbStrategy == "round_robin" && lastSelectedEndpointID != "" {
			nextEp := nextEndpointAfter(filtered, excluded, lastSelectedEndpointID)
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
			if gctx.FatalErr != nil {
				return gctx.FatalErr
			}
			lastErr = core.ErrNoAvailableEndpoint
			return lastErr
		}

		selectedEp := invoker.Endpoint()
		if selectedEp != nil {
			lastSelectedEndpointID = selectedEp.ID
			// Acquire half-open probe permits before sending traffic
			serviceKey := selectedEp.Provider + ":" + selectedEp.Model
			if !ci.cbManager.AcquireHalfOpenPermit(serviceKey, ci.enableActive) {
				if !rp.IsExcludeFailedEndpoint() {
					return fmt.Errorf("service breaker half-open permit acquisition failed: %s", serviceKey)
				}
				excluded[selectedEp.ID] = true
				lastErr = fmt.Errorf("service breaker half-open permit acquisition failed")
				if attempt+1 >= maxAttempts {
					return lastErr
				}
				continue
			}
			if !ci.cbManager.AcquireHalfOpenPermit(selectedEp.ID, ci.enableActive) {
				ci.cbManager.ReleaseHalfOpenPermit(serviceKey)
				if !rp.IsExcludeFailedEndpoint() {
					return fmt.Errorf("instance breaker half-open permit acquisition failed: %s", selectedEp.ID)
				}
				excluded[selectedEp.ID] = true
				lastErr = fmt.Errorf("instance breaker half-open permit acquisition failed")
				if attempt+1 >= maxAttempts {
					return lastErr
				}
				continue
			}
		}

		err = invoker.Invoke(gctx)
		clientDisconnected := errors.Is(err, core.ErrClientDisconnected)
		if err != nil && !clientDisconnected && gctx.UpstreamError == nil {
			gctx.UpstreamError = err
		}
		gctx.RecordAttempt(err == nil || clientDisconnected)

		lastInvoker = gctx.SelectedInvoker
		lastEndpoint = gctx.SelectedEndpoint
		lastConnect = gctx.UpstreamConnect
		lastResponse = gctx.UpstreamResponse
		lastBody = gctx.UpstreamBody
		lastUpstreamErr = gctx.UpstreamError
		hasPhysicalCall = true

		if clientDisconnected {
			gctx.Logger(ci.logger).Info("client disconnected during endpoint invocation",
				zap.String("endpoint", gctx.SelectedEndpoint.ID),
				zap.Int("attempt", attempt),
			)
			return err
		}

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
			// Stream: record TTFT for latency_metric=ttft. Non-stream TTFT is 0, skip.
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

		gctx.Logger(ci.logger).Warn("endpoint invocation failed",
			zap.String("endpoint", gctx.SelectedEndpoint.ID),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)

		// Always record failure for the breaker, even if we will not retry
		ci.cbManager.RecordFailure(gctx, gctx.SelectedEndpoint, err)

		// Synthetic latency so failed endpoints are not preferred by least_latency
		ci.recordFailurePenalty(gctx)

		// Stream already sent first byte: no retry
		if gctx.TTFT > 0 {
			return err
		}

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

		if rp.IsExcludeFailedEndpoint() {
			excluded[gctx.SelectedEndpoint.ID] = true
		}

		if attempt+1 >= maxAttempts {
			return err
		}

		if !hasRemainingEndpoint(endpoints, excluded) {
			return core.ErrNoAvailableEndpoint
		}

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

// endpointIDs returns endpoint IDs for logging.
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
