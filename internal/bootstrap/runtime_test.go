package bootstrap

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/events"
)

// Removing the admission lock or the join lets shutdown return while a task
// still uses resources, or lets a late task be added after Wait.
func TestGatewayRuntimeStopJoinsAndRejectsLateTasks(t *testing.T) {
	runtime := newGatewayRuntime(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	require.True(t, runtime.start(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		<-release
		close(exited)
	}))
	<-started
	stopped := make(chan struct{})
	go func() { runtime.stop(); close(stopped) }()
	<-runtime.ctx.Done()
	require.False(t, runtime.start(func(context.Context) { t.Error("late task ran") }))
	select {
	case <-stopped:
		t.Fatal("stop returned before the admitted task exited")
	default:
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop failed to join its task")
	}
	<-exited
	runtime.stop()
}

func TestRuntimePublisherCancelsInflightAndRejectsAfterClose(t *testing.T) {
	runtime := newGatewayRuntime(context.Background())
	delegate := &blockingRuntimePublisher{started: make(chan struct{}), canceled: make(chan struct{})}
	pub := newRuntimePublisher(runtime, delegate)
	published := make(chan error, 1)
	go func() { published <- pub.Publish(context.Background(), &events.OpsEvent{}) }()
	<-delegate.started
	require.NoError(t, pub.Close())
	require.ErrorIs(t, <-published, context.Canceled)
	<-delegate.canceled
	require.ErrorIs(t, pub.Publish(context.Background(), &events.OpsEvent{}), context.Canceled)
	require.NoError(t, pub.Close())
	require.EqualValues(t, 1, delegate.closes.Load())
}

type blockingRuntimePublisher struct {
	started, canceled chan struct{}
	closes            atomic.Int32
}

func (p *blockingRuntimePublisher) Publish(ctx context.Context, _ *events.OpsEvent) error {
	close(p.started)
	<-ctx.Done()
	close(p.canceled)
	return ctx.Err()
}

func (p *blockingRuntimePublisher) Close() error { p.closes.Add(1); return nil }

func TestGatewayRuntimeConcurrentAdmissionAndStop(t *testing.T) {
	runtime := newGatewayRuntime(context.Background())
	var admitted, exited atomic.Int32
	var callers sync.WaitGroup
	for range 100 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if runtime.start(func(ctx context.Context) { <-ctx.Done(); exited.Add(1) }) {
				admitted.Add(1)
			}
		}()
	}
	runtime.stop()
	callers.Wait()
	require.Equal(t, admitted.Load(), exited.Load())
	require.False(t, runtime.start(func(context.Context) {}))
}
