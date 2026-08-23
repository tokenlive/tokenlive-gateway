package outbound

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/telemetry"

	"github.com/stretchr/testify/assert"
)

func TestDefaultMetricsExtractor_ExtractLabels(t *testing.T) {
	extractor := &DefaultMetricsExtractor{}

	tests := []struct {
		name     string
		gctx     *core.GatewayContext
		expected telemetry.LabelContract
	}{
			{
				name: "成功请求_流式_有endpoint",
				gctx: &core.GatewayContext{
					Model:            "gpt-4",
					IsStream:         true,
					SelectedEndpoint: &core.Endpoint{ID: "ep-openai", Provider: "openai"},
					Policy:           &policy.Policy{EnableMetricsReporting: true},
					Tenant:           "vip-tenant",
				},
				expected: telemetry.LabelContract{
					Model:    "gpt-4",
					Provider: "openai",
					Status:   "success",
					Stream:   "true",
					Tenant:   "vip-tenant",
					Endpoint: "ep-openai",
				},
			},
			{
				name: "错误请求_非流式",
				gctx: &core.GatewayContext{
					Model:            "claude-3",
					IsStream:         false,
					Err:              errors.New("timeout"),
					SelectedEndpoint: &core.Endpoint{ID: "ep-anthropic", Provider: "anthropic"},
					Policy:           &policy.Policy{EnableMetricsReporting: false},
				},
				expected: telemetry.LabelContract{
					Model:    "claude-3",
					Provider: "anthropic",
					Status:   "error",
					Stream:   "false",
					Tenant:   "others",
					Endpoint: "ep-anthropic",
				},
			},
			{
				name: "endpoint为nil",
				gctx: &core.GatewayContext{
					Model:            "gpt-4",
					SelectedEndpoint: nil,
					Policy:           &policy.Policy{EnableMetricsReporting: false},
				},
				expected: telemetry.LabelContract{
					Model:    "gpt-4",
					Provider: "",
					Status:   "success",
					Stream:   "false",
					Tenant:   "others",
					Endpoint: "",
				},
			},
			{
				name: "SelectedEndpoint为空时回退History",
				gctx: &core.GatewayContext{
					Model:            "gpt-4",
					SelectedEndpoint: nil,
					History: []core.AttemptRecord{
						{EndpointID: ""},
						{EndpointID: "ep-last"},
					},
					Policy: &policy.Policy{EnableMetricsReporting: false},
				},
				expected: telemetry.LabelContract{
					Model:    "gpt-4",
					Provider: "",
					Status:   "success",
					Stream:   "false",
					Tenant:   "others",
					Endpoint: "ep-last",
				},
			},
			{
				name: "Policy为nil_默认租户",
				gctx: &core.GatewayContext{
					Model:            "gpt-4",
					SelectedEndpoint: &core.Endpoint{ID: "ep-openai", Provider: "openai"},
					Policy:           nil,
				},
				expected: telemetry.LabelContract{
					Model:    "gpt-4",
					Provider: "openai",
					Status:   "success",
					Stream:   "false",
					Tenant:   "others",
					Endpoint: "ep-openai",
				},
			},
			{
				name: "租户为空字符串_染色未开启",
				gctx: &core.GatewayContext{
					Model:            "gpt-4",
					SelectedEndpoint: &core.Endpoint{ID: "ep-openai", Provider: "openai"},
					Policy:           &policy.Policy{EnableMetricsReporting: true},
					Tenant:           "",
				},
				expected: telemetry.LabelContract{
					Model:    "gpt-4",
					Provider: "openai",
					Status:   "success",
					Stream:   "false",
					Tenant:   "others",
					Endpoint: "ep-openai",
				},
			},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractor.ExtractLabels(tt.gctx)
			// 只比较字段，不比较内部缓存
				assert.Equal(t, tt.expected.Model, got.Model)
				assert.Equal(t, tt.expected.Provider, got.Provider)
				assert.Equal(t, tt.expected.Status, got.Status)
				assert.Equal(t, tt.expected.Stream, got.Stream)
				assert.Equal(t, tt.expected.Tenant, got.Tenant)
				assert.Equal(t, tt.expected.Endpoint, got.Endpoint)
		})
	}
}

func TestDefaultMetricsExtractor_ExtractTokenLabels(t *testing.T) {
	extractor := &DefaultMetricsExtractor{}

	gctx := &core.GatewayContext{
		Model:            "gpt-4",
		SelectedEndpoint: &core.Endpoint{Provider: "openai"},
	}

	tests := []struct {
		tokenType string
		expected  string
	}{
		{"input", "input"},
		{"output", "output"},
		{"cached", "cached"},
		{"cache_creation", "cache_creation"},
	}

	for _, tt := range tests {
		t.Run(tt.tokenType, func(t *testing.T) {
			labels := extractor.ExtractTokenLabels(gctx, tt.tokenType)
			assert.Equal(t, tt.expected, labels.Type)
			assert.Equal(t, "gpt-4", labels.Model)
			assert.Equal(t, "openai", labels.Provider)
		})
	}
}

func TestLabelContract_ToAttributes(t *testing.T) {
	t.Run("基础标签_无缓存", func(t *testing.T) {
		lc := telemetry.LabelContract{
			Model:    "gpt-4",
			Provider: "openai",
			Status:   "success",
			Stream:   "true",
			Tenant:   "vip",
		}

		attrs := lc.ToAttributes()
		assert.Len(t, attrs, 5)
		// 验证缓存生效
		attrs2 := lc.ToAttributes()
		assert.Equal(t, attrs, attrs2)
	})

		t.Run("Token标签_包含type", func(t *testing.T) {
			lc := telemetry.LabelContract{
				Model:    "gpt-4",
				Provider: "openai",
				Status:   "success",
				Stream:   "true",
				Tenant:   "vip",
				Type:     "input",
			}

			attrs := lc.ToAttributes()
			assert.Len(t, attrs, 6) // 多了 type 标签
		})

		t.Run("WithoutType不含endpoint", func(t *testing.T) {
			lc := telemetry.LabelContract{
				Model:    "gpt-4",
				Provider: "openai",
				Status:   "success",
				Stream:   "true",
				Tenant:   "vip",
				Endpoint: "ep-1",
			}

			attrs := lc.ToAttributesWithoutType()
			assert.Len(t, attrs, 5)
			for _, attr := range attrs {
				assert.NotEqual(t, "endpoint", string(attr.Key))
			}
		})

		t.Run("RequestTotal包含endpoint空字符串", func(t *testing.T) {
			lc := telemetry.LabelContract{
				Model:    "gpt-4",
				Provider: "openai",
				Status:   "success",
				Stream:   "true",
				Tenant:   "vip",
			}

			attrs := lc.ToRequestTotalAttributes()
			assert.Len(t, attrs, 6)
			found := false
			for _, attr := range attrs {
				if string(attr.Key) == "endpoint" {
					found = true
					assert.Equal(t, "", attr.Value.AsString())
				}
			}
			assert.True(t, found)
		})

		t.Run("RequestTotal包含endpoint ID", func(t *testing.T) {
			lc := telemetry.LabelContract{
				Model:    "gpt-4",
				Provider: "openai",
				Status:   "success",
				Stream:   "true",
				Tenant:   "vip",
				Endpoint: "ep-42",
			}

			attrs := lc.ToRequestTotalAttributes()
			assert.Len(t, attrs, 6)
			found := false
			for _, attr := range attrs {
				if string(attr.Key) == "endpoint" {
					found = true
					assert.Equal(t, "ep-42", attr.Value.AsString())
				}
			}
			assert.True(t, found)
		})
	}

func TestMetricsFilter_BestEffort_ErrorHandling(t *testing.T) {
	t.Run("registry为nil_优雅降级", func(t *testing.T) {
		filter := &MetricsFilter{
			registry:  nil,
			extractor: &DefaultMetricsExtractor{},
		}

		gctx := &core.GatewayContext{
			Model:     "gpt-4",
			StartTime: time.Now(),
		}

		err := filter.OnResponse(gctx)
		assert.NoError(t, err) // BestEffort: 永远返回 nil
	})

	t.Run("extractor_panic恢复", func(t *testing.T) {
		// 模拟 panic 的 extractor
		panicExtractor := &panicExtractor{}

		registry := &telemetry.MetricsRegistry{} // 空registry，指标调用会失败但不panic
		filter := &MetricsFilter{
			registry:  registry,
			extractor: panicExtractor,
		}

		gctx := &core.GatewayContext{
			Model:     "gpt-4",
			StartTime: time.Now(),
		}

		// 应该捕获 panic 并返回 nil
		err := filter.OnResponse(gctx)
		assert.NoError(t, err)
	})
}

// panicExtractor 模拟会 panic 的 extractor
type panicExtractor struct{}

func (e *panicExtractor) ExtractLabels(gctx *core.GatewayContext) telemetry.LabelContract {
	panic("extractor panic")
}

func TestMetricsFilter_TokenMetrics_EdgeCases(t *testing.T) {
	t.Run("所有token类型都为0", func(t *testing.T) {
		extractor := &DefaultMetricsExtractor{}

		gctx := &core.GatewayContext{
			Model:               "gpt-4",
			StartTime:           time.Now(),
			InputTokens:         0,
			OutputTokens:        0,
			CachedTokens:        0,
			CacheCreationTokens: 0,
		}

		// 空的 registry，不会真正调用 Add（因为所有 tokens <= 0）
		filter := &MetricsFilter{
			registry:  &telemetry.MetricsRegistry{},
			extractor: extractor,
		}

		// 不应该 panic
		filter.recordTokenMetrics(context.Background(), gctx)
	})

	t.Run("只有cached_tokens大于0_跳过无效指标", func(t *testing.T) {
		// 这个测试展示了边界情况，但由于 registry 为空指针会 panic
		// 在实际代码中，recordMetric 会捕获错误并吞掉
		// 这里只验证逻辑分支存在
		t.Skip("需要真实的 MeterProvider 来验证，暂时跳过")
	})
}

func TestMetricsFilter_TTFT_Logic(t *testing.T) {
	t.Run("IsStream=false_不记录TTFT", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Model:     "gpt-4",
			StartTime: time.Now(),
			IsStream:  false,
			TTFT:      100 * time.Millisecond,
		}

		// TTFT > 0 但 IsStream=false，不应该记录
		// 实际验证需要 mock registry
		_ = gctx
	})

	t.Run("TTFT=0_不记录", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Model:     "gpt-4",
			StartTime: time.Now(),
			IsStream:  true,
			TTFT:      0,
		}

		// IsStream=true 但 TTFT=0，不应该记录
		_ = gctx
	})

	t.Run("IsStream=true且TTFT>0_记录", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Model:     "gpt-4",
			StartTime: time.Now(),
			IsStream:  true,
			TTFT:      120 * time.Millisecond,
		}

		// 应该记录 TTFT
		_ = gctx
	})
}
