package outbound

import (
	"context"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"

		"github.com/prometheus/client_golang/prometheus"
		dto "github.com/prometheus/client_model/go"
		"go.opentelemetry.io/otel"
		otelprom "go.opentelemetry.io/otel/exporters/prometheus"
		"go.opentelemetry.io/otel/sdk/metric"
	)

func newTestMetricsFilterHelper(t *testing.T) (*MetricsFilter, *prometheus.Registry) {
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

	// 使用新的构造函数
	registry, err := telemetry.NewMetricsRegistry(mp)
	if err != nil {
		t.Fatalf("failed to create metrics registry: %v", err)
	}

	f := NewMetricsFilter(registry, &DefaultMetricsExtractor{}, nil)
	return f, reg
}

func TestMetricsFilter_HasCostMetric(t *testing.T) {
	f, reg := newTestMetricsFilterHelper(t)

	// 触发一次 OnResponse 以生成 counter 样本，否则 Gather 不会返回零值 counter
	gctx := &core.GatewayContext{
		Model:            "gpt-4",
		StartTime:        time.Now().Add(-100 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{Provider: "openai"},
		Cost:             0.01,
	}
	_ = f.OnResponse(gctx)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range mfs {
		if mf.GetName() == "gateway_cost_total" {
			found = true
			if mf.GetHelp() != "Total cost in USD" {
				t.Errorf("expected help 'Total cost in USD', got %q", mf.GetHelp())
			}
			break
		}
	}
	if !found {
		t.Error("expected metric 'gateway_cost_total' to be registered")
	}
}

func TestMetricsFilter_CostMetricIncremented(t *testing.T) {
	f, reg := newTestMetricsFilterHelper(t)

	gctx := &core.GatewayContext{
		Model:            "gpt-4",
		StartTime:        time.Now().Add(-100 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{Provider: "openai"},
		Cost:             0.05,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	for _, mf := range mfs {
		if mf.GetName() == "gateway_cost_total" {
			metrics := mf.GetMetric()
			if len(metrics) == 0 {
				t.Fatal("expected at least one cost metric sample")
			}
			// 验证 counter 值被增加
			if metrics[0].GetCounter().GetValue() != 0.05 {
				t.Errorf("expected cost value 0.05, got %f", metrics[0].GetCounter().GetValue())
			}
			return
		}
	}
	t.Error("expected metric 'gateway_cost_total' to be present after OnResponse")
}

func TestMetricsFilter_ZeroCostNotRecorded(t *testing.T) {
	f, reg := newTestMetricsFilterHelper(t)

	gctx := &core.GatewayContext{
		Model:     "gpt-4",
		StartTime: time.Now().Add(-100 * time.Millisecond),
		Cost:      0,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	for _, mf := range mfs {
		if mf.GetName() == "gateway_cost_total" {
			t.Error("expected cost metric to not be recorded when cost is 0")
			return
		}
	}
}

func labelValue(labels []*dto.LabelPair, name string) (string, bool) {
	for _, label := range labels {
		if label.GetName() == name {
			return label.GetValue(), true
		}
	}
	return "", false
}

func TestMetricsFilter_RequestTotalEndpointLabelOnly(t *testing.T) {
	f, reg := newTestMetricsFilterHelper(t)

	gctx := &core.GatewayContext{
		Model:     "gpt-4",
		StartTime: time.Now().Add(-200 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{
			ID:       "ep-42",
			Provider: "openai",
		},
		IsStream:            true,
		TTFT:                120 * time.Millisecond,
		InputTokens:         10,
		OutputTokens:        20,
		CachedTokens:        2,
		CacheCreationTokens: 1,
		Cost:                0.03,
	}
	if err := f.OnResponse(gctx); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	wantEndpoint := map[string]bool{
		"gateway_request_total": true,
	}
	mustNotHaveEndpoint := map[string]bool{
		"gateway_request_duration_seconds": true,
		"gateway_ttft_seconds":             true,
		"gateway_tokens_total":             true,
		"gateway_cost_total":               true,
	}
	seen := map[string]bool{}

	for _, mf := range mfs {
		name := mf.GetName()
		if !wantEndpoint[name] && !mustNotHaveEndpoint[name] {
			continue
		}
		if len(mf.GetMetric()) == 0 {
			t.Fatalf("expected samples for %s", name)
		}
		seen[name] = true
		for _, m := range mf.GetMetric() {
			value, ok := labelValue(m.GetLabel(), "endpoint")
			if wantEndpoint[name] {
				if !ok {
					t.Fatalf("expected %s to include endpoint label", name)
				}
				if value != "ep-42" {
					t.Fatalf("expected %s endpoint=ep-42, got %q", name, value)
				}
				continue
			}
			if ok {
				t.Fatalf("expected %s not to include endpoint label", name)
			}
		}
	}

	for name := range wantEndpoint {
		if !seen[name] {
			t.Fatalf("expected metric %s to be recorded", name)
		}
	}
	for name := range mustNotHaveEndpoint {
		if !seen[name] {
			t.Fatalf("expected metric %s to be recorded", name)
		}
	}
}

func TestMetricsFilter_TTFTAndTenantDyeing(t *testing.T) {
	f, reg := newTestMetricsFilterHelper(t)

	// 1. 开启指标上报白名单，且有 TTFT 的流式请求
	gctx1 := &core.GatewayContext{
		Model:            "gpt-4",
		StartTime:        time.Now().Add(-200 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{Provider: "openai"},
		IsStream:         true,
		TTFT:             120 * time.Millisecond,
		Tenant:           "tenant-vip",
		Policy: &policy.Policy{
			EnableMetricsReporting: true,
		},
	}
	_ = f.OnResponse(gctx1)

	// 2. 未开启指标上报白名单，但有 TTFT 的流式请求
	gctx2 := &core.GatewayContext{
		Model:            "gpt-4",
		StartTime:        time.Now().Add(-200 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{Provider: "openai"},
		IsStream:         true,
		TTFT:             150 * time.Millisecond,
		Tenant:           "tenant-normal",
		Policy: &policy.Policy{
			EnableMetricsReporting: false,
		},
	}
	_ = f.OnResponse(gctx2)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	var foundVIP, foundNormal, foundOthers bool
	for _, mf := range mfs {
		if mf.GetName() == "gateway_ttft_seconds" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "tenant" {
						if label.GetValue() == "tenant-vip" {
							foundVIP = true
						}
						if label.GetValue() == "tenant-normal" {
							foundNormal = true
						}
						if label.GetValue() == "others" {
							foundOthers = true
						}
					}
				}
			}
		}
	}

	if !foundVIP {
		t.Error("expected to find metric with tenant='tenant-vip' for custom-dyed telemetry")
	}
	if foundNormal {
		t.Error("expected NOT to find metric with tenant='tenant-normal' because telemetry reporting is disabled")
	}
	if !foundOthers {
		t.Error("expected to find metric with tenant='others' for regular non-dyed telemetry")
	}
}
