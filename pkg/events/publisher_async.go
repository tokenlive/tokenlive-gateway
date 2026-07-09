package events

import (
	"context"
	"sync"
	"time"
)

// AsyncPublisher wraps a delegate Publisher and handles events asynchronously
// using a buffered channel and a fixed background worker.
type AsyncPublisher struct {
	delegate Publisher
	eventCh  chan *OpsEvent
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	cfg      EventsConfig
}

// NewAsyncPublisher wraps an existing publisher in an asynchronous buffer.
func NewAsyncPublisher(delegate Publisher, bufferSize int) *AsyncPublisher {
	if bufferSize <= 0 {
		bufferSize = 1024
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &AsyncPublisher{
		delegate: delegate,
		eventCh:  make(chan *OpsEvent, bufferSize),
		ctx:      ctx,
		cancel:   cancel,
	}

	p.wg.Add(1)
	go p.worker()

	return p
}

// SetEventsConfig sets the toggle switches for different event types.
func (p *AsyncPublisher) SetEventsConfig(cfg EventsConfig) {
	p.cfg = cfg
}

func (p *AsyncPublisher) isEventEnabled(eventType string) bool {
	switch eventType {
	case EventTypeCircuitBreak:
		if p.cfg.CircuitBreak != nil {
			return *p.cfg.CircuitBreak
		}
	case EventTypeRateLimit:
		if p.cfg.RateLimit != nil {
			return *p.cfg.RateLimit
		}
	case EventTypeInvocationFail:
		if p.cfg.InvocationFail != nil {
			return *p.cfg.InvocationFail
		}
	case EventTypeModelFailover:
		if p.cfg.ModelFailover != nil {
			return *p.cfg.ModelFailover
		}
	case EventTypeEndpointFailover:
		if p.cfg.EndpointFailover != nil {
			return *p.cfg.EndpointFailover
		}
	case EventTypeRetryError:
		if p.cfg.RetryError != nil {
			return *p.cfg.RetryError
		}
	case EventTypeCircuitBreakerError:
		if p.cfg.CircuitBreakerError != nil {
			return *p.cfg.CircuitBreakerError
		}
	}
	return true
}

// Publish implements Publisher. It pushes the event to the buffered channel.
// If the channel is full, the event is safely dropped to avoid blocking the caller.
func (p *AsyncPublisher) Publish(ctx context.Context, event *OpsEvent) error {
	if !p.isEventEnabled(event.EventType) {
		return nil
	}

	select {
	case p.eventCh <- event:
		return nil
	default:
		// Queue full, drop the event to avoid blocking (safe drop as events are best-effort)
		return nil
	}
}

// Close stops the background worker, flushes pending events, and closes the delegate.
func (p *AsyncPublisher) Close() error {
	p.cancel()
	p.wg.Wait()

	// Drain remaining events in the channel (best effort flush)
	close(p.eventCh)
	for event := range p.eventCh {
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = p.delegate.Publish(bgCtx, event)
		cancel()
	}

	return p.delegate.Close()
}

func (p *AsyncPublisher) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case event, ok := <-p.eventCh:
			if !ok {
				return
			}
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = p.delegate.Publish(bgCtx, event)
			cancel()
		}
	}
}
