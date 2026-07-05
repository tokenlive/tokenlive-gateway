package outbound

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type mockSettlementStore struct {
	core.StateStore

	incrCalls   map[string]int64
	refundCalls map[string]int64
}

func (m *mockSettlementStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
	if m.incrCalls == nil {
		m.incrCalls = make(map[string]int64)
	}
	m.incrCalls[key] += tokens
	return 10000, nil
}

func (m *mockSettlementStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
	if m.refundCalls == nil {
		m.refundCalls = make(map[string]int64)
	}
	m.refundCalls[key] += tokens
	return nil
}

func (m *mockSettlementStore) GetEMA(ctx context.Context, key string) (float64, error) {
	return 0.0001, nil
}

func (m *mockSettlementStore) UpdateEMA(ctx context.Context, key string, actual int64, alpha float64) (float64, error) {
	return 0, nil
}

func TestTokenSettlementFilter_OnResponse(t *testing.T) {
	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-token-ratio",
				Type: "token",
				Estimator: &policy.EstimatorConfig{
					Type:  "length_ratio",
					Ratio: 0.5,
				},
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	t.Run("Actual less than estimated (Refund)", func(t *testing.T) {
		mock := &mockSettlementStore{}
		f := NewTokenSettlementFilter(mock, nil, nil)

		body := []byte("12345678901234567890") // length = 20. ratio = 0.5. estimated = 10.
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
		gctx := &core.GatewayContext{
			Ctx:          context.Background(),
			Request:      req,
			UserID:       "user-123",
			Model:        "gpt-4",
			RawBody:      body,
			Policy:       p,
			InputTokens:  3, // actual total = 3 + 4 = 7
			OutputTokens: 4,
		}

		// Actual = 7, Estimated = 10, diff = 7 - 10 = -3. We expect a Refund of 3 tokens.
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expectedKey := "user-123:gpt-4:policy-token-ratio:1m0s"
		refunded := mock.refundCalls[expectedKey]
		if refunded != 3 {
			t.Errorf("expected 3 refunded tokens, got %d", refunded)
		}
	})

	t.Run("Actual greater than estimated (Incr)", func(t *testing.T) {
		mock := &mockSettlementStore{}
		f := NewTokenSettlementFilter(mock, nil, nil)

		body := []byte("12345678901234567890") // length = 20. ratio = 0.5. estimated = 10.
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
		gctx := &core.GatewayContext{
			Ctx:          context.Background(),
			Request:      req,
			UserID:       "user-123",
			Model:        "gpt-4",
			RawBody:      body,
			Policy:       p,
			InputTokens:  10, // actual total = 10 + 5 = 15
			OutputTokens: 5,
		}

		// Actual = 15, Estimated = 10, diff = 15 - 10 = 5. We expect an Incr of 5 tokens.
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		expectedKey := "user-123:gpt-4:policy-token-ratio:1m0s"
		incred := mock.incrCalls[expectedKey]
		if incred != 5 {
			t.Errorf("expected 5 incread tokens, got %d", incred)
		}
	})
}

func TestTokenSettlementFilter_CompletionEstimation(t *testing.T) {
	p := &policy.Policy{
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-token-ratio",
				Type: "token",
				Estimator: &policy.EstimatorConfig{
					Type:  "length_ratio",
					Ratio: 0.5,
				},
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	t.Run("Stream Interrupted (No usage) - Fallback to Estimation", func(t *testing.T) {
		mock := &mockSettlementStore{}
		f := NewTokenSettlementFilter(mock, nil, nil)

		body := []byte("12345678901234567890") // length = 20. ratio = 0.5. estimated = 10.
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
		gctx := &core.GatewayContext{
			Ctx:              context.Background(),
			Request:          req,
			UserID:           "user-123",
			Model:            "gpt-4",
			RawBody:          body,
			Policy:           p,
			IsStream:         true,
			InputTokens:      10,
			OutputTokens:     0,  // 没有返回官方 completion tokens
			TransmittedChars: 10, // 但发送了 10 个字符
		}

		// 我们预期 gctx.OutputTokens 会被估算为：TransmittedChars * 0.5 (对应 LimitPolicies 里的 Estimator.Ratio)
		// 也就是说 CompletionTokens = 10 * 0.5 = 5.
		// 最终结算：Actual = PromptTokens(10) + CompletionTokens(5) = 15.
		// Estimated = 10. diff = 15 - 10 = 5. We expect an Incr of 5 tokens.
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if gctx.OutputTokens != 5 {
			t.Errorf("expected estimated completion tokens to be 5, got %d", gctx.OutputTokens)
		}
		if gctx.Tags["completion_token_estimated"] != "true" {
			t.Errorf("expected tag completion_token_estimated=true, got %s", gctx.Tags["completion_token_estimated"])
		}

		expectedKey := "user-123:gpt-4:policy-token-ratio:1m0s"
		incred := mock.incrCalls[expectedKey]
		if incred != 5 {
			t.Errorf("expected 5 incread tokens, got %d", incred)
		}
	})
}

func TestTokenSettlementFilter_OnResponse_WithCachedTokens(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:         2.0,   // 元/百万 Tokens
			OutputPrice:        4.0,
			CachedPrice:        0.2,   // 90% off
			CacheCreationPrice: 2.5,
		},
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-cost-limit",
				Type: "cost",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	// PromptTokens = 100, CompletionTokens = 50.
	// CachedTokens = 80, CacheCreationTokens = 10.
	// Non-cached prompt tokens = 100 - 80 - 10 = 10.
	// gctx.Cost = (10 * 2.0 + 80 * 0.2 + 10 * 2.5 + 50 * 4.0) / 1_000_000
	//           = (20 + 16 + 25 + 200) / 1_000_000
	//           = 261 / 1_000_000 = 0.000261
	gctx := &core.GatewayContext{
		Ctx:                 context.Background(),
		Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		UserID:              "user-caching",
		Model:               "gpt-4",
		Policy:              p,
		InputTokens:         100,
		OutputTokens:        50,
		CachedTokens:        80,
		CacheCreationTokens: 10,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// expectedCost = 0.000261 元
	if !almostEqual(gctx.Cost, 0.000261) {
		t.Errorf("expected gctx.Cost to be 0.000261, got %f", gctx.Cost)
	}

	// 小 token 量下 actualCost(厘) = int64(261/1000) = 0, estimatedCost = 0, diff = 0
	// 因此不触发滑动窗口扣减（这是正确的行为，小量请求不产生厘级费用）
	expectedKey := "user-caching:gpt-4:policy-cost-limit:1m0s"
	incred := mock.incrCalls[expectedKey]
	if incred != 0 {
		t.Errorf("expected 0 incred cost (diff rounds to 0 for small token counts), got %d", incred)
	}
}

func TestTokenSettlementFilter_OnResponse_WithExcessiveCachedTokens(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:         2.0,  // 元/百万 Tokens
			OutputPrice:        4.0,
			CachedPrice:        0.2,
			CacheCreationPrice: 2.5,
		},
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name: "policy-cost-limit",
				Type: "cost",
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	// 异常输入：CachedTokens (200) + CacheCreationTokens (50) > InputTokens (100)
	// 新实现不再回写 gctx 状态，而是做防御性 clamp 后计算费用：
	//   cached 钳到 100，cacheCreation 钳到 0，nonCachedPrompt = 100 - 100 - 0 = 0
	//   Cost = (0*2.0 + 100*0.2 + 0*2.5 + 50*4.0) / 1_000_000
	gctx := &core.GatewayContext{
		Ctx:                 context.Background(),
		Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		UserID:              "user-caching-excessive",
		Model:               "gpt-4",
		Policy:              p,
		InputTokens:         100,
		OutputTokens:        50,
		CachedTokens:        200,
		CacheCreationTokens: 50,
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// gctx 状态应保持原值不被污染（clamp 只作用于费用计算）
	if gctx.CachedTokens != 200 {
		t.Errorf("gctx.CachedTokens should be preserved at 200, got %d", gctx.CachedTokens)
	}
	if gctx.CacheCreationTokens != 50 {
		t.Errorf("gctx.CacheCreationTokens should be preserved at 50, got %d", gctx.CacheCreationTokens)
	}

	// Cost 应基于 clamp 后的值：cached=100, cacheCreation=0
	expectedCost := (0.0*2.0 + 100.0*0.2 + 0.0*2.5 + 50.0*4.0) / 1_000_000.0
	if !almostEqual(gctx.Cost, expectedCost) {
		t.Errorf("expected Cost %v, got %v", expectedCost, gctx.Cost)
	}
}

// TestTokenSettlementFilter_AnthropicCacheBilling_Regression 防止缺陷1回归。
// 修复前：Anthropic 的 InputTokens 仅存「未命中缓存」部分，但结算公式按 OpenAI 语义
// 做 nonCached = InputTokens - Cached - CacheCreation，导致负数或被错误截断。
// 修复后：提取阶段已将 InputTokens 归一化为「总输入」，故此处模拟归一化后的 gctx，
// 验证四段计费正确且不为负。
//   input_tokens(未命中)=500, cache_read=1000, cache_creation=200
//   提取后 InputTokens = 500 + 1000 + 200 = 1700 (总输入)
//   nonCached = 1700 - 1000 - 200 = 500
//   Cost = (500*2.0 + 1000*0.2 + 200*2.5 + 100*4.0) / 1_000_000
func TestTokenSettlementFilter_AnthropicCacheBilling_Regression(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:         2.0,
			OutputPrice:        4.0,
			CachedPrice:        0.2,
			CacheCreationPrice: 2.5,
		},
	}
	gctx := &core.GatewayContext{
		Ctx:                 context.Background(),
		Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		Model:               "claude-3-5-sonnet",
		Policy:              p,
		InputTokens:         1700, // 总输入（提取阶段已归一化）
		OutputTokens:        100,
		CachedTokens:        1000,
		CacheCreationTokens: 200,
	}

	f := NewTokenSettlementFilter(&mockSettlementStore{}, nil, nil)
	if err := f.OnResponse(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if gctx.Cost < 0 {
		t.Fatalf("Cost must not be negative, got %v", gctx.Cost)
	}
	// nonCached=500, cached=1000, cacheCreation=200, output=100
	expectedCost := (500.0*2.0 + 1000.0*0.2 + 200.0*2.5 + 100.0*4.0) / 1_000_000.0
	if !almostEqual(gctx.Cost, expectedCost) {
		t.Errorf("expected Cost %v, got %v", expectedCost, gctx.Cost)
	}
}

// mockCreditsDeductor 记录最后一次扣减的 credits，用于断言 Credits 扣减行为
type mockCreditsDeductor struct {
	deducted int64
	calls    int
}

func (m *mockCreditsDeductor) DeductCredits(ctx context.Context, apiKey string, credits int64) (int64, error) {
	m.deducted += credits
	m.calls++
	return 10000000 - credits, nil
}

// TestTokenSettlementFilter_CreditsBilling_Regression 验证积分扣减行为
// 积分以微元为单位原子扣减（1 元 = 1,000,000 微元）
func TestTokenSettlementFilter_CreditsBilling_Regression(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:         2.0,
			OutputPrice:        4.0,
			CachedPrice:        0.2,
			CacheCreationPrice: 2.5,
		},
	}

	// 用户A：全部未命中缓存
	// 费用计算：(1000 * 2.0 + 100 * 4.0) / 1_000_000 = 0.0024 元
	// Credits 扣减 = 2400 微元
	qdA := &mockCreditsDeductor{}
	fA := NewTokenSettlementFilter(&mockSettlementStore{}, qdA, nil)
	gctxA := &core.GatewayContext{
		Ctx:          context.Background(),
		Request:      httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		UserID:       "user-a",
		APIKey:       "key-a",
		Model:        "gpt-4",
		Policy:       p,
		InputTokens:  1000,
		OutputTokens: 100,
	}
	if err := fA.OnResponse(gctxA); err != nil {
		t.Fatalf("user A: expected no error, got %v", err)
	}

	// 用户B：输入全部命中缓存
	// 费用计算：(0 * 2.0 + 1000 * 0.2 + 100 * 4.0) / 1_000_000 = 0.0006 元
	// Credits 扣减 = 600 微元
	qdB := &mockCreditsDeductor{}
	fB := NewTokenSettlementFilter(&mockSettlementStore{}, qdB, nil)
	gctxB := &core.GatewayContext{
		Ctx:           context.Background(),
		Request:       httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		UserID:        "user-b",
		APIKey:        "key-b",
		Model:         "gpt-4",
		Policy:        p,
		InputTokens:   1000,
		OutputTokens:  100,
		CachedTokens:  1000,
	}
	if err := fB.OnResponse(gctxB); err != nil {
		t.Fatalf("user B: expected no error, got %v", err)
	}

	// 用户B（重度缓存）扣减的积分应明显少于用户A
	if qdB.deducted >= qdA.deducted {
		t.Errorf("cache-heavy user should deduct fewer credits: A=%d, B=%d", qdA.deducted, qdB.deducted)
	}

	if qdA.deducted != 2400 {
		t.Errorf("user A expected 2400 micro-credits, got %d", qdA.deducted)
	}

	if qdB.deducted != 600 {
		t.Errorf("user B expected 600 micro-credits, got %d", qdB.deducted)
	}
}

// TestTokenSettlementFilter_StreamInterrupt_InputBackfill_Regression 防止缺陷3回归。
// 修复前：流式异常中断时 usage 缺失，仅估算 OutputTokens，InputTokens 留为 0，
// 导致 computeActualCost 漏算全部输入费用（网关收入损失）。
// 修复后：当 InputTokens==0 且 RawBody 可用时，用 EstimateInputTokens 补齐输入并打标。
func TestTokenSettlementFilter_StreamInterrupt_InputBackfill_Regression(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:  2.0,
			OutputPrice: 4.0,
		},
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name:      "policy-token-ratio",
				Type:      "token",
				Estimator: &policy.EstimatorConfig{Type: "length_ratio", Ratio: 0.5},
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	// 输入体长度 20，ratio 0.5 → 估算输入 10 token；不提供任何 usage
	body := []byte("12345678901234567890")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	gctx := &core.GatewayContext{
		Ctx:              context.Background(),
		Request:          req,
		UserID:           "user-stream",
		Model:            "gpt-4",
		RawBody:          body,
		Policy:           p,
		IsStream:         true,
		InputTokens:      0, // usage 缺失
		OutputTokens:     0,
		TransmittedChars: 10, // 已发送 10 字符
	}

	if err := f.OnResponse(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 输出应被估算为 10 * 0.5 = 5
	if gctx.OutputTokens != 5 {
		t.Errorf("expected estimated output tokens 5, got %d", gctx.OutputTokens)
	}
	// 输入应被补齐：EstimateInputTokens 用同策略 ratio=0.5 → 20 * 0.5 = 10
	if gctx.InputTokens != 10 {
		t.Errorf("expected backfilled input tokens 10, got %d", gctx.InputTokens)
	}
	if gctx.Tags["input_token_estimated"] != "true" {
		t.Errorf("expected tag input_token_estimated=true, got %v", gctx.Tags["input_token_estimated"])
	}

	// Cost 应包含输入与输出两部分，不再是「仅输出」
	expectedCost := (10.0*2.0 + 5.0*4.0) / 1_000_000.0
	if !almostEqual(gctx.Cost, expectedCost) {
		t.Errorf("expected Cost %v, got %v", expectedCost, gctx.Cost)
	}
}

// TestTokenSettlementFilter_CostEstimate_CacheAlignment_Regression 防止缺陷2回归。
// 修复前：成本限流预估只用 inputPrice/outputPrice，忽略已知缓存 token，
// 导致缓存命中场景预估系统性偏高、diff 长期为负、频繁退还。
// 修复后：预估复用已知的 cached/cacheCreation 值并按各自单价计费，与实际口径对齐。
func TestTokenSettlementFilter_CostEstimate_CacheAlignment_Regression(t *testing.T) {
	p := &policy.Policy{
		Billing: &policy.BillingPolicy{
			InputPrice:         2.0,
			OutputPrice:        4.0,
			CachedPrice:        0.2,
			CacheCreationPrice: 2.5,
		},
		LimitPolicies: []*policy.LimitPolicy{
			{
				Name:      "policy-cost",
				Type:      "cost",
				Estimator: &policy.EstimatorConfig{Type: "length_ratio", Ratio: 0.5},
				SlidingWindows: []*policy.SlidingWindow{
					{Threshold: 1_000_000, TimeWindowInMs: 60000},
				},
			},
		},
	}

	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	// 构造：总输入 1000（其中 cached=1000 已知），输出 100。
	// InputTokens 在 OnResponse 进入时已由提取层归一化为总输入。
	// 为让「预估总输入」与「实际总输入」一致，使 RawBody 的估算值恰好等于 1000：
	//   EstimateInputTokens 用 ratio=0.5 → len(RawBody)*0.5，故 RawBody 长度需为 2000。
	body := make([]byte, 2000)
	for i := range body {
		body[i] = 'a'
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
	gctx := &core.GatewayContext{
		Ctx:          context.Background(),
		Request:      req,
		Tenant:       "tenant-x",
		Model:        "gpt-4",
		RawBody:      body,
		Policy:       p,
		InputTokens:  1000,
		OutputTokens: 100,
		CachedTokens: 1000,
	}

	if err := f.OnResponse(gctx); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 预估口径与实际口径对齐：缓存部分(1000) 按 cachedPrice(0.2) 计费而非 inputPrice(2.0)。
	//   actualCost(厘)  = (0*2.0 + 1000*0.2 + 0*2.5 + 100*4.0)/1000 = 600/1000 = 0.6 → int64=0
	//   estimatedCost(厘)= (0*2.0 + 1000*0.2 + 100*4.0)/1000        = 600/1000 = 0.6 → int64=0
	//   diff = 0，不应触发 Incr/Refund。
	// （输出 EMA 在 mockSettlementStore 中返回 0.0001≈0，故预估输出≈0；但因数值过小整除为 0。）
	for k, v := range mock.incrCalls {
		t.Errorf("expected no incr when estimate aligns with actual, key=%s incr=%d", k, v)
	}
	for k, v := range mock.refundCalls {
		t.Errorf("expected no refund when estimate aligns with actual, key=%s refund=%d", k, v)
	}
}

func almostEqual(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 1e-9
}

func TestTokenSettlementFilter_OnResponse_PriceInheritanceAndOverride(t *testing.T) {
	mock := &mockSettlementStore{}
	f := NewTokenSettlementFilter(mock, nil, nil)

	t.Run("Inherit from model level billing policy", func(t *testing.T) {
		p := &policy.Policy{
			Billing: &policy.BillingPolicy{
				InputPrice:         5.0,  // 元/百万 Tokens
				OutputPrice:        8.0,
				CachedPrice:        1.0,
				CacheCreationPrice: 6.0,
			},
		}
		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-1",
			Model:               "gpt-4",
			Policy:              p,
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		// cost = (nonCachedPromptTokens * inputPrice + cachedTokens * cachedPrice + cacheCreationTokens * cacheCreationPrice + outputTokens * outputPrice) / 1_000_000.0
		// nonCachedPromptTokens = 100 - 20 - 10 = 70.
		// cost = (70 * 5.0 + 20 * 1.0 + 10 * 6.0 + 50 * 8.0) / 1_000_000.0
		//      = (350 + 20 + 60 + 400) / 1_000_000.0
		//      = 830 / 1_000_000.0 = 0.00083
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.00083) {
			t.Errorf("expected cost to be 0.00083, got %f", gctx.Cost)
		}
	})

	t.Run("Override by Endpoint level configuration (Full)", func(t *testing.T) {
		p := &policy.Policy{
			Billing: &policy.BillingPolicy{
				InputPrice:         5.0,  // 元/百万 Tokens
				OutputPrice:        8.0,
				CachedPrice:        1.0,
				CacheCreationPrice: 6.0,
			},
		}
		inputVal := 10.0
		outputVal := 20.0
		cachedVal := 3.0
		creationVal := 15.0

		endpoint := &core.Endpoint{
			InputPrice:         &inputVal,
			OutputPrice:        &outputVal,
			CachedPrice:        &cachedVal,
			CacheCreationPrice: &creationVal,
		}

		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-2",
			Model:               "gpt-4",
			Policy:              p,
			SelectedEndpoint:    endpoint,
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		// cost = (70 * 10.0 + 20 * 3.0 + 10 * 15.0 + 50 * 20.0) / 1_000_000.0
		//      = (700 + 60 + 150 + 1000) / 1_000_000.0 = 1910 / 1_000_000.0 = 0.00191
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.00191) {
			t.Errorf("expected cost to be 0.00191, got %f", gctx.Cost)
		}
	})

	t.Run("Override by Endpoint level configuration (Partial/Inheritance)", func(t *testing.T) {
		p := &policy.Policy{
			Billing: &policy.BillingPolicy{
				InputPrice:         5.0,  // 元/百万 Tokens
				OutputPrice:        8.0,
				CachedPrice:        1.0,
				CacheCreationPrice: 6.0,
			},
		}
		inputVal := 10.0
		outputVal := 20.0

		endpoint := &core.Endpoint{
			InputPrice:         &inputVal,
			OutputPrice:        &outputVal,
			CachedPrice:        nil,
			CacheCreationPrice: nil,
		}

		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-3",
			Model:               "gpt-4",
			Policy:              p,
			SelectedEndpoint:    endpoint,
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.00178) {
			t.Errorf("expected cost to be 0.00178, got %f", gctx.Cost)
		}
	})

	t.Run("Fallback to default 2.0 when policy and endpoint are nil", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:                 context.Background(),
			Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			UserID:              "user-4",
			Model:               "gpt-4",
			InputTokens:         100,
			OutputTokens:        50,
			CachedTokens:        20,
			CacheCreationTokens: 10,
		}
		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !almostEqual(gctx.Cost, 0.0003) {
			t.Errorf("expected cost to be 0.0003, got %f", gctx.Cost)
		}
	})
}
