package events

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockUnderlyingPublisher struct {
	mu        sync.Mutex
	published []*OpsEvent
	errToPass error
	delay     time.Duration
}

func (m *mockUnderlyingPublisher) Publish(ctx context.Context, event *OpsEvent) error {
	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.delay):
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, event)
	return m.errToPass
}

func (m *mockUnderlyingPublisher) Close() error {
	return nil
}

func TestAsyncPublisher_NormalPublish(t *testing.T) {
	mock := &mockUnderlyingPublisher{}
	asyncPub := NewAsyncPublisher(mock, 10)
	defer asyncPub.Close()

	evt := &OpsEvent{EventType: "test_event"}
	err := asyncPub.Publish(context.Background(), evt)
	if err != nil {
		t.Fatalf("unexpected publish error: %v", err)
	}

	// 稍等让后台 worker 发送完毕
	time.Sleep(10 * time.Millisecond)

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.published) != 1 {
		t.Errorf("expected 1 published event, got %d", len(mock.published))
	}
}

func TestAsyncPublisher_QueueFullAndDrop(t *testing.T) {
	// 底层发送器故意卡住以造成通道占满
	mock := &mockUnderlyingPublisher{delay: 200 * time.Millisecond}
	// bufferSize = 1
	asyncPub := NewAsyncPublisher(mock, 1)
	defer asyncPub.Close()

	// 投递第 1 个事件，它将被 worker 从 channel 取出并进入 delay 卡住状态
	_ = asyncPub.Publish(context.Background(), &OpsEvent{EventType: "1"})

	// 投递第 2 个事件，此时 worker 还在卡住，第 2 个事件被成功放入 channel (因为 bufferSize=1)
	_ = asyncPub.Publish(context.Background(), &OpsEvent{EventType: "2"})

	// 投递第 3 个事件，由于 channel 已满，第三个应该被直接丢弃且不阻塞
	startTime := time.Now()
	err := asyncPub.Publish(context.Background(), &OpsEvent{EventType: "3"})
	duration := time.Since(startTime)

	if err != nil {
		t.Fatalf("expected nil error on queue full, got %v", err)
	}

	// 确保没有阻塞，耗时应该非常微小（例如小于 10ms）
	if duration > 10*time.Millisecond {
		t.Errorf("Publish blocked for %v, expected non-blocking select", duration)
	}
}

func TestAsyncPublisher_GracefulClose(t *testing.T) {
	mock := &mockUnderlyingPublisher{}
	asyncPub := NewAsyncPublisher(mock, 10)

	// 快速投递 5 个事件
	for i := 0; i < 5; i++ {
		_ = asyncPub.Publish(context.Background(), &OpsEvent{EventType: "event"})
	}

	// 立刻 Close，验证其是否能优雅 Flush 完毕
	err := asyncPub.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()
	if len(mock.published) != 5 {
		t.Errorf("expected all 5 events to be flushed on close, got %d", len(mock.published))
	}
}

func TestAsyncPublisher_FilterEvents(t *testing.T) {
	mock := &mockUnderlyingPublisher{}
	asyncPub := NewAsyncPublisher(mock, 10)
	defer asyncPub.Close()

	// 设置开关：禁用 circuit_break 和 rate_limit，启用 invocation_fail，不显式配置 lb_switch（应默认开启）
	falseVal := false
	trueVal := true
	cfg := EventsConfig{
		CircuitBreak:   &falseVal,
		RateLimit:      &falseVal,
		InvocationFail: &trueVal,
	}
	asyncPub.SetEventsConfig(cfg)

	// 1. 发送被禁用的事件
	_ = asyncPub.Publish(context.Background(), &OpsEvent{EventType: EventTypeCircuitBreak})
	_ = asyncPub.Publish(context.Background(), &OpsEvent{EventType: EventTypeRateLimit})

	// 2. 发送被启用的事件
	_ = asyncPub.Publish(context.Background(), &OpsEvent{EventType: EventTypeInvocationFail})

	// 3. 发送默认启用的事件（未配置的事件）
	_ = asyncPub.Publish(context.Background(), &OpsEvent{EventType: EventTypeModelFailover})

	// 等待 worker 协程发送
	time.Sleep(20 * time.Millisecond)

	mock.mu.Lock()
	defer mock.mu.Unlock()

	// 我们总共发了 4 个，但其中 2 个被禁用了，剩下 2 个应当成功发送
	if len(mock.published) != 2 {
		t.Errorf("expected 2 published events, got %d", len(mock.published))
	}

	for _, evt := range mock.published {
		if evt.EventType == EventTypeCircuitBreak || evt.EventType == EventTypeRateLimit {
			t.Errorf("received disabled event type: %s", evt.EventType)
		}
	}
}
