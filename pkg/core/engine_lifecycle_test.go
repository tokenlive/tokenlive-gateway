package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestEngine_StopBackgroundTasksWaitsAndRejectsNewJobs(t *testing.T) {
	engine := &Engine{}
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	if !engine.lifecycle.start(nil, nil, func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
	}) {
		t.Fatal("initial background task rejected")
	}
	<-started
	stopped := make(chan struct{})
	go func() { engine.StopBackgroundTasks(); close(stopped) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("background task was not canceled")
	}
	select {
	case <-stopped:
		t.Fatal("stop returned before the task exited")
	default:
	}
	if engine.lifecycle.start(nil, nil, func(context.Context) {}) {
		t.Fatal("background task admitted after stop")
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not join the completed task")
	}
	engine.StopBackgroundTasks()
}

type lifecycleDiscovery struct {
	closes int
	err    error
}

func (d *lifecycleDiscovery) List(context.Context, string) ([]*Endpoint, error) { return nil, nil }
func (d *lifecycleDiscovery) Watch(context.Context, string) (<-chan []*Endpoint, error) {
	return nil, nil
}
func (d *lifecycleDiscovery) Close() error { d.closes++; return d.err }

func TestEngine_CloseIsIdempotentAndPreservesError(t *testing.T) {
	closeErr := errors.New("discovery close")
	discovery := &lifecycleDiscovery{err: closeErr}
	engine := &Engine{discovery: discovery}
	for range 2 {
		if !errors.Is(engine.Close(), closeErr) {
			t.Fatal("close error was not preserved")
		}
	}
	if discovery.closes != 1 {
		t.Fatalf("discovery closed %d times", discovery.closes)
	}
}

type blockingHealthProvider struct {
	*countingHealthProvider
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (p *blockingHealthProvider) HealthCheck(ctx context.Context) error {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	close(p.canceled)
	<-p.release
	return ctx.Err()
}

func TestEngine_CloseWaitsForHealthProviderBeforeDiscovery(t *testing.T) {
	discovery := &lifecycleDiscovery{}
	static := NewStaticDiscovery()
	engine := NewEngine(&EngineConfig{}, discovery, nil, nil, zap.NewNop())
	engine.SetStaticDiscovery(static)
	provider := &blockingHealthProvider{
		countingHealthProvider: &countingHealthProvider{name: "blocking"},
		started:                make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}),
	}
	engine.SetProviders(map[string]Provider{"blocking": provider})
	engine.StartHealthCheck(engine.Context(), time.Millisecond, false)
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("health provider was not called")
	}
	closed := make(chan struct{})
	go func() { _ = engine.Close(); close(closed) }()
	select {
	case <-provider.canceled:
	case <-time.After(time.Second):
		t.Fatal("health provider was not canceled")
	}
	select {
	case <-closed:
		t.Fatal("engine closed dependencies before the health check returned")
	default:
	}
	close(provider.release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("engine did not join health provider")
	}
	if discovery.closes != 1 {
		t.Fatalf("discovery closed %d times", discovery.closes)
	}
}

func TestStaticDiscovery_StopHealthChecksJoinsEndpointJobs(t *testing.T) {
	discovery := NewStaticDiscovery()
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	discovery.healthLifecycle.start(context.Background(), nil, func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
	})
	<-started
	stopped := make(chan struct{})
	go func() { discovery.StopHealthChecks(); close(stopped) }()
	<-canceled
	select {
	case <-stopped:
		t.Fatal("health stop returned before an endpoint job exited")
	default:
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("health stop did not join")
	}
	discovery.StartHealthCheck(context.Background(), nil, nil, zap.NewNop(), time.Millisecond, false)
	if discovery.healthLifecycle.start(nil, nil, func(context.Context) {}) {
		t.Fatal("health checks restarted after stop")
	}
}
