package bootstrap

import (
	"context"
	"sync"

	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"go.uber.org/zap"
)

// 构造资源时立即登记所有权，成功退出与构造失败回滚共用关闭顺序。
type gatewayLifetime struct {
	once         sync.Once
	logger       *zap.Logger
	engine       *core.Engine
	tasks        *gatewayRuntime
	stateStore   core.StateStore
	queue        compensation.Queue
	discovery    core.Discovery
	worker       *compensation.Worker
	accessLog    *outbound.AccessLogFilter
	status       *outbound.StatusCollectorFilter
	publisher    events.Publisher
	stopReporter func()
	otelCleanup  func()
	initialized  bool
}

func (l *gatewayLifetime) close() {
	l.once.Do(func() {
		if l.stopReporter != nil {
			l.stopReporter()
		}
		if l.engine != nil {
			l.engine.StopBackgroundTasks()
		}
		if l.tasks != nil {
			l.tasks.stop()
		}
		if publisher, ok := l.publisher.(*runtimePublisher); ok {
			publisher.Stop()
		}
		if l.worker != nil {
			l.worker.Close()
		}
		if l.status != nil {
			l.status.Close()
		}
		if l.accessLog != nil {
			l.accessLog.Close()
		}
		if l.otelCleanup != nil {
			l.otelCleanup()
		}
		if l.initialized {
			if err := l.engine.Close(); err != nil {
				l.logger.Error("engine close error", zap.Error(err))
			}
		} else {
			if l.queue != nil {
				_ = l.queue.Close()
			}
			// RedisStateStore 成功关闭时沿用现有客户端所有权；构造失败不关闭借入客户端。
			if _, borrowed := l.stateStore.(*store.RedisStateStore); l.stateStore != nil && !borrowed {
				_ = l.stateStore.Close()
			}
			if l.discovery != nil {
				_ = l.discovery.Close()
			}
		}
		if l.publisher != nil {
			_ = l.publisher.Close()
		}
	})
}

// 只管理装配产生的后台任务，不拥有借入的 Redis/ClickHouse 客户端。
// 准入与 WaitGroup.Add 共用关闭锁，等待开始后不再接收任务。
type gatewayRuntime struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
	once   sync.Once
}

func newGatewayRuntime(parent context.Context) *gatewayRuntime {
	ctx, cancel := context.WithCancel(parent)
	return &gatewayRuntime{ctx: ctx, cancel: cancel}
}

func (r *gatewayRuntime) admit() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.ctx.Err() != nil {
		return false
	}
	r.wg.Add(1)
	return true
}

func (r *gatewayRuntime) start(task func(context.Context)) bool {
	if !r.admit() {
		return false
	}
	go func() {
		defer r.wg.Done()
		task(r.ctx)
	}()
	return true
}

func (r *gatewayRuntime) stop() {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.cancel()
		r.mu.Unlock()
		r.wg.Wait()
	})
}

// 关闭发布通道前停止新准入，并让在途发布响应整个装配的取消信号。
type runtimePublisher struct {
	runtime  *gatewayRuntime
	delegate events.Publisher
	once     sync.Once
	err      error
}

func newRuntimePublisher(runtime *gatewayRuntime, delegate events.Publisher) *runtimePublisher {
	return &runtimePublisher{runtime: runtime, delegate: delegate}
}

func (p *runtimePublisher) Publish(ctx context.Context, event *events.OpsEvent) error {
	if !p.runtime.admit() {
		return context.Canceled
	}
	defer p.runtime.wg.Done()
	publishCtx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(p.runtime.ctx, cancel)
	defer stopCancel()
	defer cancel()
	if p.runtime.ctx.Err() != nil {
		return context.Canceled
	}
	return p.delegate.Publish(publishCtx, event)
}

func (p *runtimePublisher) Stop() {
	p.runtime.stop()
	if delegate, ok := p.delegate.(*events.AsyncPublisher); ok {
		delegate.Stop()
	}
}

func (p *runtimePublisher) Close() error {
	p.once.Do(func() {
		p.Stop()
		p.err = p.delegate.Close()
	})
	return p.err
}
