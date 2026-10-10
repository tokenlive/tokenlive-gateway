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

// RouterChain returns the router chain (for tests).
func (ci *ClusterInvoker) RouterChain() []core.Router {
	return ci.routerChain
}

// Invoke runs a cluster call with retry.
func (ci *ClusterInvoker) Invoke(gctx *core.GatewayContext) error {
	observer := &attemptObserver{cbManager: ci.cbManager, stateStore: ci.stateStore, logger: ci.logger, enableActive: ci.enableActive}
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
			// Keep the previous endpoint as the cursor in discovery order, even
			// after exclusion. Only select candidates that survived all routers.
			unavailable := make(map[string]bool, len(endpoints))
			for _, ep := range endpoints {
				unavailable[ep.ID] = true
			}
			for _, ep := range filtered {
				delete(unavailable, ep.ID)
			}
			nextEp := nextEndpointAfter(endpoints, unavailable, lastSelectedEndpointID)
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
		}
		observed, acquireErr := observer.begin(gctx, selectedEp)
		if acquireErr != nil {
			if !rp.IsExcludeFailedEndpoint() {
				return acquireErr
			}
			excluded[selectedEp.ID] = true
			lastErr = acquireErr
			if attempt+1 >= maxAttempts {
				return lastErr
			}
			continue
		}

		err = func() error {
			defer observed.release()
			invokeErr := invoker.Invoke(gctx)
			clientDisconnected := errors.Is(invokeErr, core.ErrClientDisconnected)
			if invokeErr != nil && !clientDisconnected && gctx.UpstreamError == nil {
				gctx.UpstreamError = invokeErr
			}
			// 物理尝试必须先于健康/延迟反馈记录，且许可保持到 outcome 完成。
			gctx.RecordAttempt(invokeErr == nil || clientDisconnected)
			observed.sequentialOutcome(invokeErr)
			return invokeErr
		}()
		clientDisconnected := errors.Is(err, core.ErrClientDisconnected)

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
			return nil
		}

		lastErr = err

		gctx.Logger(ci.logger).Warn("endpoint invocation failed",
			zap.String("endpoint", gctx.SelectedEndpoint.ID),
			zap.Int("attempt", attempt),
			zap.Error(err),
		)

		if isExplicitlyNonRetryable(err) {
			return err
		}

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

func isExplicitlyNonRetryable(err error) bool {
	type retryability interface {
		Retryable() bool
	}

	var classified retryability
	return errors.As(err, &classified) && !classified.Retryable()
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
