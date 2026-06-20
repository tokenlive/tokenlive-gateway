package core

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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
	windowType    string       // "count" 或 "time"
	results       []bool       // 次数窗口的样本数据：true=成功, false=失败
	buckets       []timeBucket // 时间窗口的样本数据
	openSince     time.Time
	recoveryTO    time.Duration
	windowSize    int
	failThresh    int
	hoSuccessThr  int // 半开状态下所需的连续成功数
	activeCalls   int // 当前正在进行的半开探路并发数
	policyVersion int64
	modelCode     string
	policyID      string
	policyName    string
	providerName  string
	lastTenant    string
	lastTraceID   string
	lastRequestID string
	threshold     float64
	currentVal    float64
}

func (e *circuitBreakerEntry) record(success bool, now time.Time, windowType string, windowSize, failThresh, hoSuccessThr int, recoveryTO time.Duration, failureRateThreshold float64) (CircuitState, CircuitState) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 释放半开状态的探路并发数限制
	if e.state == CircuitHalfOpen && e.activeCalls > 0 {
		e.activeCalls--
	}

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
			e.state = CircuitHalfOpen
			e.results = nil // 清除旧结果，避免 Half-Open 阶段被 Closed→Open 期间的失败污染
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
			e.state = CircuitOpen
			e.openSince = now
			e.results = nil // 清除旧结果
			e.buckets = nil
		} else {
			thr := e.hoSuccessThr
			if thr <= 0 {
				thr = 1
			}
			if successes >= thr {
				e.state = CircuitClosed
			}
		}
	}
	return e.state
}

func (e *circuitBreakerEntry) reset() (CircuitState, CircuitState) {
	e.mu.Lock()
	defer e.mu.Unlock()

	oldState := e.state
	e.state = CircuitClosed
	e.results = nil
	e.buckets = nil
	e.openSince = time.Time{}
	e.activeCalls = 0
	return oldState, e.state
}

func (e *circuitBreakerEntry) checkAndResetOnVersionChange(version int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.policyVersion == 0 {
		e.policyVersion = version
		return false
	}
	if e.policyVersion != version {
		e.state = CircuitClosed
		e.results = nil
		e.buckets = nil
		e.openSince = time.Time{}
		e.activeCalls = 0
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

	// 如果开启了主动健康探测，许可数直接为 0 (禁止真实流量探路，纯靠背景探测协程探活并 Reset)
	if enableActive {
		return false
	}

	// 否则，仅限 1 个真实流量并发探路
	if e.activeCalls < 1 {
		e.activeCalls++
		return true
	}
	return false
}

// CircuitBreakerManager 管理双层熔断器。
type CircuitBreakerManager struct {
	mu           sync.RWMutex
	entries      map[string]*circuitBreakerEntry
	rdb          *redis.Client // 共享的 Redis 客户端，用于同步熔断状态
	logger       *zap.Logger
	metrics      *CircuitBreakerMetrics // 指标发射器
	eventHandler CBEventHandler         // 状态变更事件回调
}

// NewCircuitBreakerManager 创建熔断管理器，使用内部独立的内存状态存储。
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		entries: make(map[string]*circuitBreakerEntry),
	}
}

// SetRDB 注入共享 Redis 客户端
func (cbm *CircuitBreakerManager) SetRDB(rdb *redis.Client) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	cbm.rdb = rdb
}

// SetLogger 注入共享 Logger
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
	RequestID    string
	TraceID      string
	Threshold    *float64
	CurrentValue *float64
	OldState     string
	NewState     string
}

// CBEventHandler is called when the circuit breaker transitions to Open.
type CBEventHandler func(evt CBEvent)

// SetEventHandler 注入状态变更事件回调
func (cbm *CircuitBreakerManager) SetEventHandler(handler CBEventHandler) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	cbm.eventHandler = handler
}

// SetMetrics 注入指标发射器
func (cbm *CircuitBreakerManager) SetMetrics(metrics *CircuitBreakerMetrics) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	cbm.metrics = metrics
}

// onStateChange 在熔断器状态变迁时打印日志、同步写入 Redis 集合并发射指标
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

	// 被动触发：状态变更时立即发射指标
	if cbm.metrics != nil {
		cbm.metrics.RecordState(key, modelCode, newState)
	}

	// 状态变为 Open 时触发事件回调
	if newState == CircuitOpen && cbm.eventHandler != nil {
		cbm.eventHandler(CBEvent{
			Key:          key,
			ModelCode:    modelCode,
			PolicyID:     policyID,
			PolicyName:   policyName,
			TenantCode:   tenantCode,
			ProviderName: providerName,
			RequestID:    requestID,
			TraceID:      traceID,
			Threshold:    thresholdVal,
			CurrentValue: currentVal,
			OldState:     oldState.String(),
			NewState:     newState.String(),
		})
	}

	if cbm.rdb == nil {
		return
	}
	redisKey := "aigw:cb:open_endpoints"
	if strings.Contains(key, ":") {
		redisKey = "aigw:cb:open_services"
	}
	ctx := context.Background()
	if newState == CircuitOpen {
		_ = cbm.rdb.SAdd(ctx, redisKey, key).Err()
	} else if oldState == CircuitOpen && newState != CircuitOpen {
		_ = cbm.rdb.SRem(ctx, redisKey, key).Err()
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

func (cbm *CircuitBreakerManager) RecordSuccess(gctx *GatewayContext, ep *Endpoint) {
	if gctx.Policy == nil || len(gctx.Policy.CircuitBreakPolicies) == 0 {
		return
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

	now := time.Now()
	for _, p := range gctx.Policy.CircuitBreakPolicies {
		if p == nil {
			continue
		}
		ws, mc, ho, to := p.SlidingWindowSize, p.MinCallsThreshold, p.AllowedCallsInHalfOpenState, time.Duration(p.WaitDurationInOpenState)*time.Millisecond
		level := strings.ToUpper(p.Level)

		if level == "" || level == "SERVICE" {
			serviceKey := ep.Provider + ":" + ep.Model
			cbm.CheckAndResetOnVersionChange(serviceKey, p.Version)
			entry := cbm.GetEntryWithModel(serviceKey, ep.Model)
			entry.mu.Lock()
			entry.policyID = p.ID
			entry.policyName = p.Name
			entry.providerName = ep.Provider
			entry.lastTenant = gctx.Tenant
			entry.lastTraceID = traceID
			entry.lastRequestID = requestID
			entry.mu.Unlock()
			old, newStatus := entry.record(true, now, p.SlidingWindowType, ws, mc, ho, to, p.FailureRateThreshold)
			if old != newStatus {
				cbm.onStateChange(serviceKey, old, newStatus)
			}
		}
		if level == "" || level == "INSTANCE" || level == "ENDPOINT" {
			cbm.CheckAndResetOnVersionChange(ep.ID, p.Version)
			entry := cbm.GetEntryWithModel(ep.ID, ep.Model)
			entry.mu.Lock()
			entry.policyID = p.ID
			entry.policyName = p.Name
			entry.providerName = ep.Provider
			entry.lastTenant = gctx.Tenant
			entry.lastTraceID = traceID
			entry.lastRequestID = requestID
			entry.mu.Unlock()
			old, newStatus := entry.record(true, now, p.SlidingWindowType, ws, mc, ho, to, p.FailureRateThreshold)
			if old != newStatus {
				cbm.onStateChange(ep.ID, old, newStatus)
			}
		}
	}
}

func (cbm *CircuitBreakerManager) RecordFailure(gctx *GatewayContext, ep *Endpoint, err error) {
	if gctx.Policy == nil || len(gctx.Policy.CircuitBreakPolicies) == 0 {
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

	now := time.Now()
	for _, p := range gctx.Policy.CircuitBreakPolicies {
		if p == nil {
			continue
		}
		if !p.MatchError(statusCode, contentType, errMsg, gctx.UpstreamBody) {
			continue
		}

		ws, mc, ho, to := p.SlidingWindowSize, p.MinCallsThreshold, p.AllowedCallsInHalfOpenState, time.Duration(p.WaitDurationInOpenState)*time.Millisecond
		level := strings.ToUpper(p.Level)

		if level == "" || level == "SERVICE" {
			serviceKey := ep.Provider + ":" + ep.Model
			cbm.CheckAndResetOnVersionChange(serviceKey, p.Version)
			entry := cbm.GetEntryWithModel(serviceKey, ep.Model)
			entry.mu.Lock()
			entry.policyID = p.ID
			entry.policyName = p.Name
			entry.providerName = ep.Provider
			entry.lastTenant = gctx.Tenant
			entry.lastTraceID = traceID
			entry.lastRequestID = requestID
			entry.mu.Unlock()
			old, newStatus := entry.record(false, now, p.SlidingWindowType, ws, mc, ho, to, p.FailureRateThreshold)
			if old != newStatus {
				cbm.onStateChange(serviceKey, old, newStatus)
			}
		}
		if level == "" || level == "INSTANCE" || level == "ENDPOINT" {
			cbm.CheckAndResetOnVersionChange(ep.ID, p.Version)
			entry := cbm.GetEntryWithModel(ep.ID, ep.Model)
			entry.mu.Lock()
			entry.policyID = p.ID
			entry.policyName = p.Name
			entry.providerName = ep.Provider
			entry.lastTenant = gctx.Tenant
			entry.lastTraceID = traceID
			entry.lastRequestID = requestID
			entry.mu.Unlock()
			old, newStatus := entry.record(false, now, p.SlidingWindowType, ws, mc, ho, to, p.FailureRateThreshold)
			if old != newStatus {
				cbm.onStateChange(ep.ID, old, newStatus)
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
	// Half-Open 状态
	if enableActive {
		return false
	}
	// 如果没有开启主动探活，且当前探活并发数未满，则允许通过作为备选
	return entry.activeCalls < 1
}

// AcquireHalfOpenPermit 尝试抢占半开状态下的探路许可，递增并发数
func (cbm *CircuitBreakerManager) AcquireHalfOpenPermit(key string, enableActive bool) bool {
	cbm.mu.RLock()
	entry, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return true
	}
	return entry.tryAcquireHalfOpenPermit(enableActive)
}

// ReleaseHalfOpenPermit 退还已经抢占的半开状态探路许可
func (cbm *CircuitBreakerManager) ReleaseHalfOpenPermit(key string) {
	cbm.mu.RLock()
	entry, exists := cbm.entries[key]
	cbm.mu.RUnlock()
	if !exists {
		return
	}
	entry.mu.Lock()
	if entry.state == CircuitHalfOpen && entry.activeCalls > 0 {
		entry.activeCalls--
	}
	entry.mu.Unlock()
}

// GetOpenSince 返回指定 key 的熔断打开时间，用于按时间排序
// 如果 key 不存在或未处于 Open/HalfOpen 状态，返回零值时间
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
