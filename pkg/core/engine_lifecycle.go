package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

type engineLifecycle struct {
	mu        sync.Mutex
	stopped   bool
	ctx       context.Context
	cancel    context.CancelFunc
	jobs      sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func (l *engineLifecycle) start(owner, parent context.Context, run func(context.Context)) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return false
	}
	if l.ctx == nil {
		if owner == nil {
			owner = context.Background()
		}
		l.ctx, l.cancel = context.WithCancel(owner)
	}
	ctx, cancel := context.WithCancel(l.ctx)
	stopParent := func() bool { return false }
	if parent != nil {
		stopParent = context.AfterFunc(parent, cancel)
	}
	l.jobs.Add(1)
	go func() {
		defer l.jobs.Done()
		defer cancel()
		defer stopParent()
		run(ctx)
	}()
	return true
}

// StopBackgroundTasks 禁止新后台任务并等待在途探测退出，之后才可关闭依赖。
func (e *Engine) StopBackgroundTasks() {
	e.lifecycle.mu.Lock()
	e.lifecycle.stopped = true
	if e.lifecycle.cancel != nil {
		e.lifecycle.cancel()
	}
	if e.cancel != nil {
		e.cancel()
	}
	e.lifecycle.mu.Unlock()
	e.lifecycle.jobs.Wait()
	if e.staticDiscovery != nil {
		e.staticDiscovery.StopHealthChecks()
	}
}

// Close 依次停止后台任务、补偿队列、状态存储和 Discovery，重复调用不重复关闭。
func (e *Engine) Close() error {
	e.lifecycle.closeOnce.Do(func() {
		e.StopBackgroundTasks()
		var errs []error
		if e.compQueue != nil {
			errs = append(errs, e.compQueue.Close())
		}
		if e.stateStore != nil {
			errs = append(errs, e.stateStore.Close())
		}
		if e.discovery != nil {
			errs = append(errs, e.discovery.Close())
		}
		e.lifecycle.closeErr = errors.Join(errs...)
	})
	return e.lifecycle.closeErr
}

func (e *Engine) StartHealthCheck(ctx context.Context, interval time.Duration, enableActive bool) {
	e.enableActiveHealthCheck = enableActive
	if e.staticDiscovery == nil {
		return
	}
	e.lifecycle.start(e.ctx, ctx, func(ctx context.Context) {
		e.staticDiscovery.StartHealthCheck(ctx, e.getProviders, e.cbManager, e.logger, interval, enableActive)
		<-ctx.Done()
		e.staticDiscovery.StopHealthChecks()
	})
}

func (e *Engine) StartCircuitBreakerProbe(ctx context.Context, interval time.Duration) {
	if e.cbManager == nil {
		return
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	e.lifecycle.start(e.ctx, ctx, func(ctx context.Context) {
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
	})
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
