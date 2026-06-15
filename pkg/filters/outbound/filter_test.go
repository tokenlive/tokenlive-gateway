package outbound

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// ---------- OutboundFilter 接口断言 ----------

func TestOutboundFilterInterface(t *testing.T) {
	var _ core.OutboundFilter = (*TokenSettlementFilter)(nil)
	var _ core.OutboundFilter = (*StickySessionFilter)(nil)
	var _ core.OutboundFilter = (*MetricsFilter)(nil)
	var _ core.OutboundFilter = (*AccessLogFilter)(nil)
}

// ---------- TokenSettlementFilter 测试 ----------

func TestTokenSettlementFilter_NameAndOrder(t *testing.T) {
	f := NewTokenSettlementFilter(nil, nil, nil)
	if f.Name() != "token_settlement" {
		t.Errorf("expected name 'token_settlement', got '%s'", f.Name())
	}
	if f.Order() != 10 {
		t.Errorf("expected order 10, got %d", f.Order())
	}
	if f.Criticality() != core.Critical {
		t.Errorf("expected Critical criticality, got %d", f.Criticality())
	}
}

func TestTokenSettlementFilter_NilPolicy(t *testing.T) {
	ss := store.NewMemoryStateStore()
	f := NewTokenSettlementFilter(ss, nil, nil)

	gctx := &core.GatewayContext{
		InputTokens:  100,
		OutputTokens: 50,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error when Policy is nil, got: %v", err)
	}
}

func TestTokenSettlementFilter_ActualEqualsEstimated(t *testing.T) {
	ss := store.NewMemoryStateStore()
	f := NewTokenSettlementFilter(ss, nil, nil)

	// RawBody 40 bytes -> estimated 10 tokens
	// actual = 5 + 5 = 10 tokens -> no adjustment needed
	gctx := &core.GatewayContext{
		Policy: &policy.Policy{
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "tpm-limit",
					Type: "token",
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 10000, TimeWindowInMs: 60000},
					},
				},
			},
		},
		UserID:       "u1",
		Model:        "gpt-4",
		RawBody:      []byte(strings.Repeat("x", 40)),
		InputTokens:  5,
		OutputTokens: 5,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error when actual equals estimated, got: %v", err)
	}

	consumed, _ := ss.RateLimitIncr(context.Background(), "u1:gpt-4:tpm-limit:1m0s", 0, time.Minute)
	if consumed != 0 {
		t.Errorf("expected consumed to be 0, got %d", consumed)
	}
}

func TestTokenSettlementFilter_RefundWhenOverEstimated(t *testing.T) {
	ss := store.NewMemoryStateStore()
	f := NewTokenSettlementFilter(ss, nil, nil)

	// 设置 EMA 预估为 0 (0.1 -> int64(0.1) == 0)
	_, _ = ss.UpdateEMA(context.Background(), "tenant:u-refund:gpt-4", 1, 1.0)
	_, _ = ss.UpdateEMA(context.Background(), "tenant:u-refund:gpt-4", 0, 0.9)

	// RawBody 4000 bytes -> estimated 1000 tokens
	// actual = 10 + 5 = 15 tokens -> refund 985
	gctx := &core.GatewayContext{
		Policy: &policy.Policy{
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "tpm-limit",
					Type: "token",
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 10000, TimeWindowInMs: 60000},
					},
				},
			},
		},
		UserID:       "u-refund",
		Model:        "gpt-4",
		RawBody:      []byte(strings.Repeat("x", 4000)),
		InputTokens:  10,
		OutputTokens: 5,
	}

	// 模拟在 OnRequest 阶段预扣了 1000 tokens
	_, _ = ss.RateLimitIncr(context.Background(), "u-refund:gpt-4:tpm-limit:1m0s", 1000, time.Minute)

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error on refund, got: %v", err)
	}

	consumed, _ := ss.RateLimitIncr(context.Background(), "u-refund:gpt-4:tpm-limit:1m0s", 0, time.Minute)
	if consumed != 15 {
		t.Errorf("expected remaining consumed tokens to be 15, got %d", consumed)
	}
}

func TestTokenSettlementFilter_ChargeWhenUnderEstimated(t *testing.T) {
	ss := store.NewMemoryStateStore()
	f := NewTokenSettlementFilter(ss, nil, nil)

	// RawBody 4 bytes -> estimated 1 token
	// actual = 100 + 200 = 300 tokens
	gctx := &core.GatewayContext{
		Policy: &policy.Policy{
			LimitPolicies: []*policy.LimitPolicy{
				{
					Name: "tpm-limit",
					Type: "token",
					SlidingWindows: []*policy.SlidingWindow{
						{Threshold: 10000, TimeWindowInMs: 60000},
					},
				},
			},
		},
		UserID:       "u-charge",
		Model:        "gpt-4",
		RawBody:      []byte("tiny"),
		InputTokens:  100,
		OutputTokens: 200,
	}

	// 模拟在 OnRequest 阶段预扣了 1 token
	_, _ = ss.RateLimitIncr(context.Background(), "u-charge:gpt-4:tpm-limit:1m0s", 1, time.Minute)

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error on charge, got: %v", err)
	}

	consumed, _ := ss.RateLimitIncr(context.Background(), "u-charge:gpt-4:tpm-limit:1m0s", 0, time.Minute)
	if consumed != 100 {
		t.Errorf("expected total consumed tokens to be 100, got %d", consumed)
	}
}

// ---------- StickySessionFilter 测试 ----------

func TestStickySessionFilter_NameAndOrder(t *testing.T) {
	f := NewStickySessionFilter(nil, 0)
	if f.Name() != "sticky_session" {
		t.Errorf("expected name 'sticky_session', got '%s'", f.Name())
	}
	if f.Order() != 20 {
		t.Errorf("expected order 20, got %d", f.Order())
	}
	if f.Criticality() != core.Critical {
		t.Errorf("expected Critical criticality, got %d", f.Criticality())
	}
}

func TestStickySessionFilter_EmptySessionID(t *testing.T) {
	ss := store.NewMemoryStateStore()
	f := NewStickySessionFilter(ss, time.Minute)

	gctx := &core.GatewayContext{
		SessionID:        "",
		SelectedEndpoint: &core.Endpoint{ID: "ep-1"},
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error when SessionID is empty, got: %v", err)
	}
}

func TestStickySessionFilter_NilEndpoint(t *testing.T) {
	ss := store.NewMemoryStateStore()
	f := NewStickySessionFilter(ss, time.Minute)

	gctx := &core.GatewayContext{
		SessionID:        "sess-1",
		SelectedEndpoint: nil,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error when SelectedEndpoint is nil, got: %v", err)
	}
}

func TestStickySessionFilter_SavesMapping(t *testing.T) {
	ss := store.NewMemoryStateStore()
	ttl := 5 * time.Minute
	f := NewStickySessionFilter(ss, ttl)

	gctx := &core.GatewayContext{
		SessionID:        "sess-abc",
		SelectedEndpoint: &core.Endpoint{ID: "ep-42"},
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// 验证映射已保存
	endpointID, err := ss.StickyGet(context.Background(), "sess-abc")
	if err != nil {
		t.Fatalf("expected sticky mapping to exist, got error: %v", err)
	}
	if endpointID != "ep-42" {
		t.Errorf("expected endpoint 'ep-42', got '%s'", endpointID)
	}
}

// ---------- MetricsFilter 测试 ----------

func newTestMetricsFilter(t *testing.T) (*MetricsFilter, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	promExporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		t.Fatalf("failed to create prometheus exporter: %v", err)
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(promExporter),
		metric.WithView(
			metric.NewView(
				metric.Instrument{Name: "gateway_request_duration_seconds"},
				metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
				}},
			),
			metric.NewView(
				metric.Instrument{Name: "gateway_ttft_seconds"},
				metric.Stream{Aggregation: metric.AggregationExplicitBucketHistogram{
					Boundaries: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0},
				}},
			),
		),
	)

	oldProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)

	t.Cleanup(func() {
		otel.SetMeterProvider(oldProvider)
		_ = mp.Shutdown(context.Background())
	})

	registry, _ := telemetry.NewMetricsRegistry(mp)
	f := NewMetricsFilter(registry, &DefaultMetricsExtractor{}, nil)
	return f, reg
}

func TestMetricsFilter_NameAndOrder(t *testing.T) {
	f, _ := newTestMetricsFilter(t)
	if f.Name() != "metrics" {
		t.Errorf("expected name 'metrics', got '%s'", f.Name())
	}
	if f.Order() != 30 {
		t.Errorf("expected order 30, got %d", f.Order())
	}
	if f.Criticality() != core.BestEffort {
		t.Errorf("expected BestEffort criticality, got %d", f.Criticality())
	}
}

func TestMetricsFilter_SuccessRequest(t *testing.T) {
	f, reg := newTestMetricsFilter(t)

	gctx := &core.GatewayContext{
		Model:            "gpt-4",
		IsStream:         false,
		StartTime:        time.Now().Add(-100 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{Provider: "openai"},
		InputTokens:      50,
		OutputTokens:     100,
		Err:              nil,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// 验证指标已注册
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	names := make(map[string]bool)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, expected := range []string{"gateway_request_duration_seconds", "gateway_request_total", "gateway_tokens_total"} {
		if !names[expected] {
			t.Errorf("expected metric '%s' to be registered", expected)
		}
	}
}

func TestMetricsFilter_ErrorRequest(t *testing.T) {
	f, _ := newTestMetricsFilter(t)

	gctx := &core.GatewayContext{
		Model:            "claude-3-opus",
		IsStream:         true,
		StartTime:        time.Now().Add(-50 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{Provider: "anthropic"},
		InputTokens:      10,
		OutputTokens:     0,
		Err:              errors.New("upstream timeout"),
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestMetricsFilter_NilEndpoint(t *testing.T) {
	f, _ := newTestMetricsFilter(t)

	gctx := &core.GatewayContext{
		Model:     "gpt-4",
		StartTime: time.Now(),
		Err:       nil,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error with nil endpoint, got: %v", err)
	}
}

// ---------- AccessLogFilter 测试 ----------

func TestAccessLogFilter_NameAndOrder(t *testing.T) {
	logger := zap.NewNop()
	f := NewAccessLogFilter(logger)
	if f.Name() != "access_log" {
		t.Errorf("expected name 'access_log', got '%s'", f.Name())
	}
	if f.Order() != 40 {
		t.Errorf("expected order 40, got %d", f.Order())
	}
	if f.Criticality() != core.BestEffort {
		t.Errorf("expected BestEffort criticality, got %d", f.Criticality())
	}
}

func TestAccessLogFilter_SuccessfulRequest(t *testing.T) {
	logger := zap.NewNop()
	f := NewAccessLogFilter(logger)

	gctx := &core.GatewayContext{
		OriginalModel:    "gpt-4",
		Model:            "gpt-4-0613",
		IsStream:         true,
		StartTime:        time.Now().Add(-200 * time.Millisecond),
		TTFT:             50 * time.Millisecond,
		SelectedEndpoint: &core.Endpoint{ID: "ep-1", Provider: "openai"},
		InputTokens:      100,
		OutputTokens:     200,
		Cost:             0.005,
		AttemptCount:     1,
		FallbackChain:    []string{"gpt-4"},
		APIKey:           "sk-test",
		UserID:           "user-001",
		SessionID:        "sess-123",
		Err:              nil,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestAccessLogFilter_RequestWithError(t *testing.T) {
	logger := zap.NewNop()
	f := NewAccessLogFilter(logger)

	gctx := &core.GatewayContext{
		OriginalModel:    "claude-3-opus",
		Model:            "claude-3-opus",
		StartTime:        time.Now().Add(-500 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{ID: "ep-2", Provider: "anthropic"},
		AttemptCount:     3,
		FallbackChain:    []string{"claude-3-opus", "gpt-4"},
		Err:              errors.New("all endpoints failed"),
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestAccessLogFilter_NilEndpoint(t *testing.T) {
	logger := zap.NewNop()
	f := NewAccessLogFilter(logger)

	gctx := &core.GatewayContext{
		OriginalModel: "gpt-4",
		Model:         "gpt-4",
		StartTime:     time.Now(),
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error with nil endpoint, got: %v", err)
	}
}
