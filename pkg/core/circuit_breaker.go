package core

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type timeBucket struct {
	timestamp time.Time
	successes int
	failures  int
}

type circuitBreakerEntry struct {
	mu            sync.Mutex
	state         CircuitState
	windowType    string       // "count" or "time"
	results       []bool       // count-window samples: true=success, false=failure
	buckets       []timeBucket // time-window samples
	openSince     time.Time
	recoveryTO    time.Duration
	windowSize    int
	failThresh    int
	hoSuccessThr  int // consecutive successes required in half-open state
	activeCalls   int // in-flight half-open probe concurrency
	attemptEpoch  uint64
	nextToken     uint64
	permitToken   uint64 // 非零许可只允许对应 lease 归还
	policyVersion int64
	modelCode     string
	policyID      string
	policyName    string
	providerName  string
	endpointCode  string
	lastTenant    string
	lastTraceID   string
	lastRequestID string
	threshold     float64
	currentVal    float64
}

func (e *circuitBreakerEntry) record(success bool, now time.Time, windowType string, windowSize, failThresh, hoSuccessThr int, recoveryTO time.Duration, failureRateThreshold float64) (CircuitState, CircuitState) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state == CircuitHalfOpen && e.activeCalls > 0 && e.permitToken == 0 {
		e.activeCalls--
	}
	return e.recordLocked(success, now, windowType, windowSize, failThresh, hoSuccessThr, recoveryTO, failureRateThreshold)
}

// recordLocked 只记录健康结果；新 lease 单独归还许可，legacy record 保留自动释放。
func (e *circuitBreakerEntry) recordLocked(success bool, now time.Time, windowType string, windowSize, failThresh, hoSuccessThr int, recoveryTO time.Duration, failureRateThreshold float64) (CircuitState, CircuitState) {
	e.windowType = windowType
	if e.windowType == "" {
		e.windowType = "count"
	}
	e.windowSize = windowSize
	if e.windowSize <= 0 {
		e.windowSize = 100
	}
	e.failThresh = failThresh
	if e.failThresh <= 0 {
		e.failThresh = 5
	}
	e.hoSuccessThr = hoSuccessThr
	if e.hoSuccessThr <= 0 {
		e.hoSuccessThr = 1
	}
	e.recoveryTO = recoveryTO
	if e.recoveryTO <= 0 {
		e.recoveryTO = 30 * time.Second
	}

	oldState := e.state
	e.computeState(now)

	if e.state == CircuitHalfOpen {
		e.results = append(e.results, success)
		limit := e.hoSuccessThr
		if limit <= 0 {
			limit = 1
		}
		if len(e.results) > limit {
			e.results = e.results[1:]
		}
	} else if e.windowType == "time" {
		sec := now.Truncate(time.Second)
		if len(e.buckets) > 0 && e.buckets[len(e.buckets)-1].timestamp.Equal(sec) {
			if success {
				e.buckets[len(e.buckets)-1].successes++
			} else {
				e.buckets[len(e.buckets)-1].failures++
			}
		} else {
			b := timeBucket{timestamp: sec}
			if success {
				b.successes = 1
			} else {
				b.failures = 1
			}
			e.buckets = append(e.buckets, b)
		}
		e.expireBuckets(now)
	} else {
		e.results = append(e.results, success)
		limit := e.windowSize
		if len(e.results) > limit {
			e.results = e.results[1:]
		}
	}

	e.computeState(now)
	if e.state == CircuitOpen && oldState != CircuitOpen {
		failures := 0
		if oldState == CircuitHalfOpen {
			if failureRateThreshold > 0 {
				e.threshold = failureRateThreshold
				e.currentVal = 100.0
			} else {
				e.threshold = float64(e.failThresh)
				e.currentVal = 1.0
			}
		} else {
			totalCalls := 0
			if e.windowType == "time" {
				for _, b := range e.buckets {
					failures += b.failures
					totalCalls += b.successes + b.failures
				}
			} else {
				for _, r := range e.results {
					if !r {
						failures++
					}
				}
				totalCalls = len(e.results)
			}

			if failureRateThreshold > 0 {
				e.threshold = failureRateThreshold
				if totalCalls > 0 {
					e.currentVal = (float64(failures) / float64(totalCalls)) * 100.0
				} else {
					e.currentVal = 0.0
				}
			} else {
				e.threshold = float64(e.failThresh)
				e.currentVal = float64(failures)
			}
		}
	}
	return oldState, e.state
}

func (e *circuitBreakerEntry) stateVal(now time.Time) (CircuitState, CircuitState) {
	e.mu.Lock()
	defer e.mu.Unlock()

	oldState := e.state
	newState := e.computeState(now)
	return oldState, newState
}

func (e *circuitBreakerEntry) expireBuckets(now time.Time) {
	if e.windowSize <= 0 {
		return
	}
	limitTime := now.Add(-time.Duration(e.windowSize) * time.Second)
	idx := 0
	for idx < len(e.buckets) && e.buckets[idx].timestamp.Before(limitTime) {
		idx++
	}
	if idx > 0 {
		e.buckets = e.buckets[idx:]
	}
}

func (e *circuitBreakerEntry) computeState(now time.Time) CircuitState {
	if e.windowType == "time" {
		e.expireBuckets(now)
	}

	switch e.state {
	case CircuitClosed:
		failures := 0
		if e.windowType == "time" {
			for _, b := range e.buckets {
				failures += b.failures
			}
		} else {
			for _, r := range e.results {
				if !r {
					failures++
				}
			}
		}
		if e.failThresh > 0 && failures >= e.failThresh {
			e.state = CircuitOpen
			e.openSince = now
		}
	case CircuitOpen:
		if now.Sub(e.openSince) >= e.recoveryTO {
			e.invalidateAttempts()
			e.state = CircuitHalfOpen
			e.results = nil // clear stale results to avoid polluting half-open phase with Closed→Open failures
			e.buckets = nil
		}
	case CircuitHalfOpen:
		successes := 0
		failures := 0
		for _, r := range e.results {
			if r {
				successes++
			} else {
				failures++
			}
		}
		if failures > 0 {
			e.invalidateAttempts()
			e.state = CircuitOpen
			e.openSince = now
			e.results = nil // clear stale results
			e.buckets = nil
		} else {
			thr := e.hoSuccessThr
			if thr <= 0 {
				thr = 1
			}
			if successes >= thr {
				e.invalidateAttempts()
				e.state = CircuitClosed
			}
		}
	}
	return e.state
}

func (e *circuitBreakerEntry) invalidateAttempts() {
	e.attemptEpoch++
	e.activeCalls = 0
	e.permitToken = 0
}

func (e *circuitBreakerEntry) reset() (CircuitState, CircuitState) {
	e.mu.Lock()
	defer e.mu.Unlock()

	oldState := e.state
	e.state = CircuitClosed
	e.results = nil
	e.buckets = nil
	e.openSince = time.Time{}
	e.invalidateAttempts()
	return oldState, e.state
}

func (e *circuitBreakerEntry) checkAndResetOnVersionChange(version int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.checkAndResetOnVersionChangeLocked(version)
}

func (e *circuitBreakerEntry) checkAndResetOnVersionChangeLocked(version int64) bool {
	if e.policyVersion == 0 {
		e.policyVersion = version
		return false
	}
	if e.policyVersion != version {
		e.state = CircuitClosed
		e.results = nil
		e.buckets = nil
		e.openSince = time.Time{}
		e.invalidateAttempts()
		e.policyVersion = version
		return true
	}
	return false
}

func (e *circuitBreakerEntry) tryAcquireHalfOpenPermit(enableActive bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state != CircuitHalfOpen {
		return e.state == CircuitClosed
	}

	// With active health probing enabled, permits are 0 (real traffic cannot probe; background goroutine probes and resets)
	if enableActive {
		return false
	}

	// Without active probing, allow at most 1 concurrent real-traffic probe
	if e.activeCalls < 1 {
		e.activeCalls++
		return true
	}
	return false
}

// CircuitBreakerManager manages dual-layer circuit breakers (service and instance level).
type CircuitBreakerManager struct {
	mu           sync.RWMutex
	entries      map[string]*circuitBreakerEntry
	logger       *zap.Logger
	metrics      *CircuitBreakerMetrics // metrics emitter
	eventHandler CBEventHandler         // state-change event callback
}

// NewCircuitBreakerManager creates a circuit breaker manager with an independent in-memory state store.
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		entries: make(map[string]*circuitBreakerEntry),
	}
}

// SetLogger injects the shared logger.
func (cbm *CircuitBreakerManager) SetLogger(logger *zap.Logger) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	cbm.logger = logger
}

// CBEvent carries circuit breaker state transition context.
type CBEvent struct {
	Key          string
	ModelCode    string
	PolicyID     string
	PolicyName   string
	TenantCode   string
	ProviderName string
	EndpointCode string
	RequestID    string
	TraceID      string
	Threshold    *float64
	CurrentValue *float64
	OldState     string
	NewState     string
}

// CBEventHandler is called when the circuit breaker transitions to Open.
type CBEventHandler func(evt CBEvent)

// SetEventHandler injects the state-change event callback.
func (cbm *CircuitBreakerManager) SetEventHandler(handler CBEventHandler) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	cbm.eventHandler = handler
}

// SetMetrics injects the metrics emitter.
func (cbm *CircuitBreakerManager) SetMetrics(metrics *CircuitBreakerMetrics) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	cbm.metrics = metrics
}

// onStateChange logs state transitions, syncs to Redis sets, and emits metrics.
func (cbm *CircuitBreakerManager) onStateChange(key string, oldState, newState CircuitState) {
	if cbm.logger != nil {
		cbm.logger.Warn("circuit breaker state changed",
			zap.String("key", key),
			zap.String("old_state", oldState.String()),
			zap.String("new_state", newState.String()),
		)
	}

	modelCode := ""
	policyID := ""
	policyName := ""
	tenantCode := ""
	providerName := ""
	endpointCode := ""
	requestID := ""
	traceID := ""
	var thresholdVal *float64
	var currentVal *float64

	cbm.mu.RLock()
	e, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if exists {
		e.mu.Lock()
		modelCode = e.modelCode
		policyID = e.policyID
		policyName = e.policyName
		tenantCode = e.lastTenant
		providerName = e.providerName
		endpointCode = e.endpointCode
		requestID = e.lastRequestID
		traceID = e.lastTraceID
		tVal := e.threshold
		cVal := e.currentVal
		thresholdVal = &tVal
		currentVal = &cVal
		e.mu.Unlock()
	}
	if modelCode == "" && containsColon(key) {
		parts := strings.Split(key, ":")
		if len(parts) > 1 {
			modelCode = parts[1]
		}
	}

	// Passive trigger: emit metrics immediately on state change
	if cbm.metrics != nil {
		cbm.metrics.RecordState(key, modelCode, newState)
	}

	// Trigger event callback when state transitions to Open
	if newState == CircuitOpen && cbm.eventHandler != nil {
		cbm.eventHandler(CBEvent{
			Key:          key,
			ModelCode:    modelCode,
			PolicyID:     policyID,
			PolicyName:   policyName,
			TenantCode:   tenantCode,
			ProviderName: providerName,
			EndpointCode: endpointCode,
			RequestID:    requestID,
			TraceID:      traceID,
			Threshold:    thresholdVal,
			CurrentValue: currentVal,
			OldState:     oldState.String(),
			NewState:     newState.String(),
		})
	}
}

func (cbm *CircuitBreakerManager) getEntry(key string) *circuitBreakerEntry {
	return cbm.GetEntryWithModel(key, "")
}

func (cbm *CircuitBreakerManager) GetEntryWithModel(key string, modelCode string) *circuitBreakerEntry {
	cbm.mu.RLock()
	e, exists := cbm.entries[key]
	cbm.mu.RUnlock()

	if exists {
		if modelCode != "" {
			e.mu.Lock()
			e.modelCode = modelCode
			e.mu.Unlock()
		}
		return e
	}

	cbm.mu.Lock()
	e, exists = cbm.entries[key]
	if !exists {
		e = &circuitBreakerEntry{
			state:     CircuitClosed,
			modelCode: modelCode,
		}
		cbm.entries[key] = e
	} else if modelCode != "" {
		e.mu.Lock()
		e.modelCode = modelCode
		e.mu.Unlock()
	}
	cbm.mu.Unlock()

	return e
}

// releasePermitsWithoutPolicy releases half-open probe permits when circuit
// break policies disappeared after the permits were acquired. record() is the
// normal release path, so without this the permits leak and the breaker stays
// stuck in HALF_OPEN with no traffic able to recover it.
func (cbm *CircuitBreakerManager) releasePermitsWithoutPolicy(ep *Endpoint) {
	if ep == nil {
		return
	}
	cbm.ReleaseHalfOpenPermit(ep.Provider + ":" + ep.Model)
	cbm.ReleaseHalfOpenPermit(ep.ID)
}

// CircuitBreakerAttempt 持有一次调用的双层许可，记录或取消只会结束一次。
type CircuitBreakerAttempt struct {
	manager  *CircuitBreakerManager
	endpoint *Endpoint
	permits  []attemptPermit
	once     sync.Once
}

type attemptPermit struct {
	key     string
	entry   *circuitBreakerEntry
	epoch   uint64
	token   uint64
	version int64
}

// AcquireAttempt 先服务后端点获取，后者拒绝时回滚前者。
func (cbm *CircuitBreakerManager) AcquireAttempt(gctx *GatewayContext, ep *Endpoint, enableActive bool) (*CircuitBreakerAttempt, error) {
	a := &CircuitBreakerAttempt{manager: cbm, endpoint: ep}
	if ep == nil {
		return a, nil
	}
	for i, key := range []string{ep.Provider + ":" + ep.Model, ep.ID} {
		entry := cbm.GetEntryWithModel(key, ep.Model)
		entry.mu.Lock()
		old := entry.state
		state := entry.computeState(time.Now())
		allowed := state == CircuitClosed || state == CircuitHalfOpen && !enableActive && entry.activeCalls < 1
		permit := attemptPermit{key: key, entry: entry, epoch: entry.attemptEpoch, version: entry.policyVersion}
		if allowed && state == CircuitHalfOpen {
			entry.nextToken++
			permit.token = entry.nextToken
			entry.permitToken = permit.token
			entry.activeCalls++
		}
		entry.mu.Unlock()
		if old != state {
			cbm.onStateChange(key, old, state)
		}
		if !allowed {
			a.Release()
			level := "service"
			if i == 1 {
				level = "instance"
			}
			return nil, fmt.Errorf("%s breaker half-open permit acquisition failed: %s", level, key)
		}
		a.permits = append(a.permits, permit)
	}
	return a, nil
}

func (a *CircuitBreakerAttempt) releasePermits() {
	for _, p := range a.permits {
		p.entry.mu.Lock()
		if p.token != 0 && p.entry.attemptEpoch == p.epoch && p.entry.permitToken == p.token {
			p.entry.activeCalls--
			p.entry.permitToken = 0
		}
		p.entry.mu.Unlock()
	}
}

// Release 结束未计入健康的调用，旧 epoch/token 不能归还新一轮许可。
func (a *CircuitBreakerAttempt) Release() {
	a.once.Do(a.releasePermits)
}

func (a *CircuitBreakerAttempt) RecordSuccess(gctx *GatewayContext) {
	a.finish(gctx, true, nil)
}

func (a *CircuitBreakerAttempt) RecordFailure(gctx *GatewayContext, err error) {
	a.finish(gctx, false, err)
}

func (a *CircuitBreakerAttempt) finish(gctx *GatewayContext, success bool, err error) {
	a.once.Do(func() {
		defer a.releasePermits()
		a.manager.recordOutcome(gctx, a.endpoint, success, err, a)
	})
}

func circuitPolicyApplies(level string, service bool) bool {
	level = strings.ToUpper(level)
	if service {
		return level == "" || level == "SERVICE"
	}
	return level == "" || level == "INSTANCE" || level == "ENDPOINT"
}

func (cbm *CircuitBreakerManager) RecordSuccess(gctx *GatewayContext, ep *Endpoint) {
	cbm.recordOutcome(gctx, ep, true, nil, nil)
}

func (cbm *CircuitBreakerManager) RecordFailure(gctx *GatewayContext, ep *Endpoint, err error) {
	cbm.recordOutcome(gctx, ep, false, err, nil)
}

func (cbm *CircuitBreakerManager) recordOutcome(gctx *GatewayContext, ep *Endpoint, success bool, err error, attempt *CircuitBreakerAttempt) {
	if ep == nil {
		return
	}
	if gctx == nil || gctx.Policy == nil || len(gctx.Policy.CircuitBreakPolicies) == 0 {
		if attempt == nil {
			cbm.releasePermitsWithoutPolicy(ep)
		}
		return
	}
	statusCode := getStatusCode(gctx.UpstreamResponse)
	contentType := ""
	if gctx.UpstreamResponse != nil {
		contentType = gctx.UpstreamResponse.Header.Get("Content-Type")
	}
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	traceID, requestID := "", ""
	if gctx.Request != nil {
		traceID = gctx.Request.Header.Get("X-Trace-ID")
		requestID = gctx.Request.Header.Get("X-Request-ID")
	}
	if traceID == "" && gctx.ResponseWriter != nil {
		traceID = gctx.ResponseWriter.Header().Get("X-Trace-Id")
	}
	if requestID == "" {
		requestID = traceID
	}
	now := time.Now()
	for _, p := range gctx.Policy.CircuitBreakPolicies {
		if p == nil || !success && !p.MatchError(statusCode, contentType, errMsg, gctx.UpstreamBody) {
			continue
		}
		for i, key := range []string{ep.Provider + ":" + ep.Model, ep.ID} {
			if !circuitPolicyApplies(p.Level, i == 0) {
				continue
			}
			var entry *circuitBreakerEntry
			var permit *attemptPermit
			if attempt == nil {
				cbm.CheckAndResetOnVersionChange(key, p.Version)
				entry = cbm.GetEntryWithModel(key, ep.Model)
			} else {
				for index := range attempt.permits {
					candidate := &attempt.permits[index]
					if candidate.key == key {
						permit = candidate
						entry = candidate.entry
						break
					}
				}
				if entry == nil {
					continue
				}
			}
			entry.mu.Lock()
			if permit != nil {
				if entry.attemptEpoch != permit.epoch || entry.policyVersion != permit.version {
					entry.mu.Unlock()
					continue
				}
				// 匹配后按原顺序更新版本；自身重置/状态变化推进 lease epoch，外部重置仍拒绝。
				versionReset := entry.checkAndResetOnVersionChangeLocked(p.Version)
				permit.version = entry.policyVersion
				if versionReset {
					permit.epoch, permit.token = entry.attemptEpoch, 0
					entry.mu.Unlock()
					cbm.onStateChange(key, CircuitOpen, CircuitClosed)
					entry.mu.Lock()
					if entry.attemptEpoch != permit.epoch || entry.policyVersion != permit.version {
						entry.mu.Unlock()
						continue
					}
				}
			}
			entry.policyID, entry.policyName = p.ID, p.Name
			entry.providerName, entry.endpointCode = ep.Provider, ep.Code
			entry.lastTenant, entry.lastTraceID, entry.lastRequestID = gctx.Tenant, traceID, requestID
			if attempt == nil && entry.state == CircuitHalfOpen && entry.activeCalls > 0 && entry.permitToken == 0 {
				entry.activeCalls--
			}
			old, newStatus := entry.recordLocked(success, now, p.SlidingWindowType, p.SlidingWindowSize,
				p.MinCallsThreshold, p.AllowedCallsInHalfOpenState,
				time.Duration(p.WaitDurationInOpenState)*time.Millisecond, p.FailureRateThreshold)
			if permit != nil && permit.epoch != entry.attemptEpoch {
				permit.epoch, permit.token = entry.attemptEpoch, 0
			}
			entry.mu.Unlock()
			if old != newStatus {
				cbm.onStateChange(key, old, newStatus)
			}
		}
	}
}

func (cbm *CircuitBreakerManager) IsServiceOpen(key string) bool {
	cbm.mu.RLock()
	e, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return false
	}
	old, new := e.stateVal(time.Now())
	if old != new {
		cbm.onStateChange(key, old, new)
	}
	return new == CircuitOpen
}

func (cbm *CircuitBreakerManager) IsInstanceOpen(endpointID string) bool {
	cbm.mu.RLock()
	e, exists := cbm.entries[endpointID]
	cbm.mu.RUnlock()
	if !exists {
		return false
	}
	old, new := e.stateVal(time.Now())
	if old != new {
		cbm.onStateChange(endpointID, old, new)
	}
	return new == CircuitOpen
}

func (cbm *CircuitBreakerManager) Reset(key string) {
	e := cbm.getEntry(key)
	old, new := e.reset()
	if old != new {
		cbm.onStateChange(key, old, new)
	}
}

func (cbm *CircuitBreakerManager) CheckAndResetOnVersionChange(key string, version int64) bool {
	e := cbm.getEntry(key)
	if e.checkAndResetOnVersionChange(version) {
		cbm.onStateChange(key, CircuitOpen, CircuitClosed)
		return true
	}
	return false
}

func (cbm *CircuitBreakerManager) RecordRaw(key string, success bool, windowSize, minCalls, allowedCallsInHalfOpen int, recoveryTimeout time.Duration) {
	old, new := cbm.getEntry(key).record(success, time.Now(), "count", windowSize, minCalls, allowedCallsInHalfOpen, recoveryTimeout, 0.0)
	if old != new {
		cbm.onStateChange(key, old, new)
	}
}

func (cbm *CircuitBreakerManager) GetState(key string) CircuitState {
	cbm.mu.RLock()
	e, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return CircuitClosed
	}
	old, new := e.stateVal(time.Now())
	if old != new {
		cbm.onStateChange(key, old, new)
	}
	return new
}

func (cbm *CircuitBreakerManager) AllowRequest(key string, enableActive bool) bool {
	cbm.mu.RLock()
	entry, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return true
	}

	now := time.Now()
	oldState, newState := entry.stateVal(now)
	if oldState != newState {
		cbm.onStateChange(key, oldState, newState)
	}

	if newState == CircuitClosed {
		return true
	}
	if newState == CircuitOpen {
		return false
	}
	// Half-open state
	if enableActive {
		return false
	}
	// Without active probing, allow through as fallback if probe concurrency is not full
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.state != CircuitHalfOpen || entry.activeCalls < 1
}

// AcquireHalfOpenPermit attempts to acquire a half-open probe permit, incrementing concurrency.
func (cbm *CircuitBreakerManager) AcquireHalfOpenPermit(key string, enableActive bool) bool {
	cbm.mu.RLock()
	entry, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return true
	}
	return entry.tryAcquireHalfOpenPermit(enableActive)
}

// ReleaseHalfOpenPermit returns a previously acquired half-open probe permit.
func (cbm *CircuitBreakerManager) ReleaseHalfOpenPermit(key string) {
	cbm.mu.RLock()
	entry, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return
	}
	entry.mu.Lock()
	if entry.state == CircuitHalfOpen && entry.activeCalls > 0 && entry.permitToken == 0 {
		entry.activeCalls--
	}
	entry.mu.Unlock()
}

// GetOpenSince returns the time the breaker opened for the given key (for time-based sorting).
// Returns zero time if the key does not exist or is not in Open/HalfOpen state.
func (cbm *CircuitBreakerManager) GetOpenSince(key string) time.Time {
	cbm.mu.RLock()
	entry, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return time.Time{}
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.state == CircuitClosed {
		return time.Time{}
	}
	return entry.openSince
}

// GetOpenBreakers returns all endpoint IDs and service keys currently in Open state.
func (cbm *CircuitBreakerManager) GetOpenBreakers() ([]string, []string) {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	var openEndpoints []string
	var openServices []string

	now := time.Now()
	for key, e := range cbm.entries {
		_, state := e.stateVal(now)
		if state == CircuitOpen {
			if e.endpointCode != "" || e.providerName != "" {
				openEndpoints = append(openEndpoints, key)
			} else {
				openServices = append(openServices, key)
			}
		}
	}
	return openEndpoints, openServices
}
