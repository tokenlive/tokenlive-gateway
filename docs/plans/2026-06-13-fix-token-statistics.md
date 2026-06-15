# 修复 Token 统计与缓存占比逻辑 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复网关后端数据统计逻辑中缓存 Token 大于输入 Token 的 Bug，回写修正后的 Context 字段，并优化控制台前端饼图的 Token 占比展示，确保数据源与展示层皆完全准确且不再重叠。

**Architecture:**

1. 后端：在 `token_settlement.go` 过滤器中将合理校验截断后的 cachedTokens 与 cacheCreationTokens 覆盖写回 `GatewayContext` (`gctx`)。
2. 后端防御性统计：在 `status_collector.go` 中同样对累加给 Redis 的 cached 字段进行校验截断，防止发生溢出污染。
3. 前端：在饼图展示配置中将输入 Token 数量扣除缓存部分，实现互斥切片显示。

**Tech Stack:** Go (Gateway Backend), Vue 3 + ECharts (Admin Console Frontend)

---

### Task 1: 编写后端回归测试以复现并验证缓存截断功能

**Files:**

- Modify: `pkg/filters/outbound/token_settlement_test.go`

- [ ] **Step 1: 在 `token_settlement_test.go` 中新增测试用例 `TestTokenSettlementFilter_OnResponse_WithExcessiveCachedTokens`**

```go
func TestTokenSettlementFilter_OnResponse_WithExcessiveCachedTokens(t *testing.T) {
 p := &policy.Policy{
  Billing: &policy.BillingPolicy{
   InputPrice:         0.002,
   OutputPrice:        0.004,
   CachedPrice:        0.0002,
   CacheCreationPrice: 0.0025,
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
 f := NewTokenSettlementFilter(mock)

 // CachedTokens (200) + CacheCreationTokens (50) > InputTokens (100)
 // 预期 OnResponse 执行后，gctx.CachedTokens 被修正回写为 100，gctx.CacheCreationTokens 修正为 0
 gctx := &core.GatewayContext{
  Ctx:                 context.Background(),
  Request:             httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
  UserID:              "user-caching-excessive",
  Model:               "gpt-4",
  Policy:              p,
  InputTokens:        100,
  OutputTokens:    50,
  CachedTokens:        200,
  CacheCreationTokens: 50,
 }

 err := f.OnResponse(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }

 if gctx.CachedTokens != 100 {
  t.Errorf("expected gctx.CachedTokens to be corrected to 100, got %d", gctx.CachedTokens)
 }

 if gctx.CacheCreationTokens != 0 {
  t.Errorf("expected gctx.CacheCreationTokens to be corrected to 0, got %d", gctx.CacheCreationTokens)
 }
}
```

- [ ] **Step 2: 运行测试并确认失败**

运行：

```bash
go test ./pkg/filters/outbound/ -v -run TestTokenSettlementFilter_OnResponse_WithExcessiveCachedTokens
```

预期输出：测试失败，提示 `gctx.CachedTokens` 的实际值仍为 200 而非 100。

- [ ] **Step 3: 提交当前更改**

```bash
git add pkg/filters/outbound/token_settlement_test.go
git commit -m "test: add test case for excessive cached tokens correction"
```

---

### Task 2: 修复网关后端 `token_settlement` 过滤器

**Files:**

- Modify: `pkg/filters/outbound/token_settlement.go`

- [ ] **Step 1: 在 `token_settlement.go` 开头增加将合理截断后的 Cached 状态写回 `gctx` 的逻辑**

修改 `pkg/filters/outbound/token_settlement.go`：

```go
// 在 OnResponse 方法的成本计算前：
  if gctx.CachedTokens+gctx.CacheCreationTokens > gctx.InputTokens {
   gctx.CachedTokens = gctx.InputTokens
   gctx.CacheCreationTokens = 0
  }

  cachedTokens := gctx.CachedTokens
  cacheCreationTokens := gctx.CacheCreationTokens
  nonCachedPromptTokens := gctx.InputTokens - cachedTokens - cacheCreationTokens
```

- [ ] **Step 2: 运行单元测试验证修正通过**

运行：

```bash
go test ./pkg/filters/outbound/ -v -run TestTokenSettlementFilter_OnResponse_WithExcessiveCachedTokens
```

预期输出：测试通过 (PASS)。

- [ ] **Step 3: 运行该目录下所有的单元测试**

运行：

```bash
go test ./pkg/filters/outbound/ -v
```

预期输出：全部通过 (PASS)。

- [ ] **Step 4: 提交当前更改**

```bash
git add pkg/filters/outbound/token_settlement.go
git commit -m "fix: correct cached tokens on GatewayContext and write back in token_settlement filter"
```

---

### Task 3: 后端增加 `status_collector` 防御性截断保护

**Files:**

- Modify: `pkg/filters/outbound/status_collector.go`

- [ ] **Step 1: 在 `status_collector.go` 转换局部变量时加入防御性截断**

修改 `pkg/filters/outbound/status_collector.go` 的第 36-39 行：

```go
 inputTokens := int64(gctx.InputTokens)
 outputTokens := int64(gctx.OutputTokens)
 cachedTokens := int64(gctx.CachedTokens)
 cacheCreationTokens := int64(gctx.CacheCreationTokens)

 // 防御性截断，确保写入 Redis 日指标的数据逻辑绝对成立
 if cachedTokens+cacheCreationTokens > inputTokens {
  cachedTokens = inputTokens
  cacheCreationTokens = 0
 }
```

- [ ] **Step 2: 执行网关编译校验**

运行：

```bash
make build
```

预期输出：网关服务端顺利编译成功。

- [ ] **Step 3: 提交当前更改**

```bash
git add pkg/filters/outbound/status_collector.go
git commit -m "fix: add defensive token boundary checks in status_collector daily metrics logging"
```

---

### Task 4: 修复控制台前端 ECharts 饼图 Token 分布占比计算

**Files:**

- Modify: `tokenlive-gateway-admin/frontend/src/views/home/index.vue`

- [ ] **Step 1: 修改 `index.vue` 中 `tokenChartOptions` 里的饼图数据逻辑**

修改 `tokenChartOptions` 中饼图扇区的数据结构，将 `input`（全部输入）替换为扣除已缓存部分的纯输入/未缓存输入 Token (`inputNonCache`)：

```javascript
// 在 686 行附近的 tokenChartOptions 计算中：
const tokenChartOptions = computed(() => {
    const isDark = appStore.config.theme === 'dark'
    const input = metrics.dailyPromptTokens
    const output = metrics.dailyCompletionTokens
    const cached = metrics.dailyCachedTokens
    const cacheCreation = metrics.dailyCacheCreationTokens
    const total = input + output

    // 独立计算扣除缓存命中的纯未缓存输入
    const inputNonCache = Math.max(0, input - cached - cacheCreation)

    return {
        // ...
                data: [
                    { value: inputNonCache, name: t('pages.dashboard.tokens.input'), itemStyle: { color: '#722ed1' } },
                    {
                        value: output,
                        name: t('pages.dashboard.tokens.output'),
                        itemStyle: { color: '#b37feb' },
                    },
                    {
                        value: cached,
                        name: t('pages.dashboard.tokens.cached'),
                        itemStyle: { color: '#52c41a' },
                    },
                    {
                        value: cacheCreation,
                        name: t('pages.dashboard.tokens.cache_creation'),
                        itemStyle: { color: '#13c2c2' },
                    },
                ],
        // ...
```

- [ ] **Step 2: 前端代码格式化**

运行：

```bash
npx prettier --config .prettierrc --write src/views/home/index.vue
```

（在 `tokenlive-gateway-admin/frontend` 目录下运行）

- [ ] **Step 3: 运行前端打包编译，确认无语法或语法检查报错**

运行：

```bash
npm run build
```

（在 `tokenlive-gateway-admin/frontend` 目录下运行）

- [ ] **Step 4: 提交当前更改**

```bash
git add src/views/home/index.vue
git commit -m "fix: refine console pie chart prompt tokens deduction to make segments mutually exclusive"
```
