# 第二阶段：动态策略执行与结算（Filters & Invokers）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完全落地大模型网关的动态策略执行体系：实现 Token 策略化分词估算与 per-limiter 差额结算、实现 `"cost"` 消费额度限流器、支持基于首包延迟（TTFT）的慢调用熔断判定以及兼容 OpenAI 的熔断降级响应。

**Architecture:**

- **限流与结算**：重构 `limiter.EstimatePromptTokens` 与 `TokenSettlementFilter`，支持按 `LimitPolicy` 的 `Estimator` 进行精细化预估；注册 `"cost"` 限流执行器并在结算期多退少补。
- **动态熔断**：在 `ClusterInvoker` 中提取并传递动态熔断参数；成功请求耗时若超过 `SlowCallDurationThreshold`（当配置 `SlowCallMetric == "TTFT"` 时）计为慢调用失败；当整个模型熔断时由网关返回标准 OpenAI 格式的 JSON 错误。
- **超时绑定**：在 `ProviderInvoker` 发起请求时读取 `gctx.Policy` 超时定义并应用。

**Tech Stack:** Go (Golang), go-test, JSON

---

### Task 1: 策略化 Token 预估与 per-limiter 精确结算

**Files:**

- Modify: `pkg/limiter/token.go`
- Modify: `pkg/filters/token_settlement.go`
- Test: `pkg/filters/limit_test.go`

- [ ] **Step 1: 修改 Token 预估函数，使其解析 `LimitPolicy.Estimator`**
  
  修改 `pkg/limiter/token.go`：

  ```go
  // 修改函数签名，引入 LimitPolicy 参数
  func EstimatePromptTokens(gctx *core.GatewayContext, lp *policy.LimitPolicy) int64 {
   if lp != nil && lp.Estimator != nil {
    switch lp.Estimator.Type {
    case "length_ratio":
     if lp.Estimator.Ratio > 0 {
      return int64(float64(len(gctx.RawBody)) * lp.Estimator.Ratio)
     }
    }
   }
   return int64(len(gctx.RawBody)) / 4 // 兜底
  }
  ```

  同时将 `token.go` 中调用 `EstimatePromptTokens` 的地方都对齐传入 `lp`：
  - `Execute` 方法中：`estimate := EstimatePromptTokens(gctx, lp)`
  - `Refund` 方法中：`estimate := EstimatePromptTokens(gctx, lp)`

- [ ] **Step 2: 修改 `TokenSettlementFilter` 支持独立预估和高精度结算**
  
  修改 `pkg/filters/token_settlement.go`：

  ```go
  func (f *TokenSettlementFilter) OnResponse(gctx *core.GatewayContext) error {
   policy := gctx.Policy
   if policy == nil || len(policy.LimitPolicies) == 0 {
    return nil
   }

   actual := int64(gctx.PromptTokens + gctx.CompletionTokens)

   for _, lp := range policy.LimitPolicies {
    if lp.Type != "token" && lp.Type != "cost" {
     continue
    }
    // 自治条件判断
    if !matchLimitPolicyConditions(gctx, lp) {
     continue
    }

    // 核心修改：在循环内部针对不同限制策略单独计算预估值
    estimated := limiter.EstimatePromptTokens(gctx, lp)
    diff := actual - estimated
    if diff == 0 {
     continue
    }

    limitKey := "rl:" + gctx.UserID + ":" + gctx.Model + ":" + lp.Name
    for _, sw := range lp.SlidingWindows {
     window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
     if window <= 0 {
      window = time.Minute
     }
     windowKey := limitKey + ":" + window.String()

     if diff < 0 {
      if err := f.stateStore.RateLimitRefund(context.Background(), windowKey, -diff); err != nil {
       return err
      }
     } else {
      if _, err := f.stateStore.RateLimitIncr(context.Background(), windowKey, diff, window); err != nil {
       return err
      }
     }
    }
   }
   return nil
  }
  ```

- [ ] **Step 3: 编写测试用例验证不同估算系数下的限流和结算差额**
  
  修改 `pkg/filters/limit_test.go`，编写 `TestRateLimitFilter_Estimator` 测试用例：

  ```go
  func TestRateLimitFilter_Estimator(t *testing.T) {
   // TDD: 验证比例为 0.5 时，RawBody=100字节 预估为 50 tokens
   lp := &policy.LimitPolicy{
    Name: "test-ratio-limit",
    Type: "token",
    Estimator: &policy.EstimatorConfig{
     Type:  "length_ratio",
     Ratio: 0.5,
    },
   }
   gctx := &core.GatewayContext{
    RawBody: []byte(string("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890")), // 100 字节
   }
   est := limiter.EstimatePromptTokens(gctx, lp)
   if est != 50 {
    t.Errorf("expected estimated 50 tokens, got %d", est)
   }
  }
  ```

- [ ] **Step 4: 运行测试验证**
  
  运行：`go test -v ./pkg/filters -run TestRateLimitFilter_Estimator`
  预期结果：PASS。

---

### Task 2: 实现 `"cost"` 消费额度限流器与差额扣减

**Files:**

- Create: `pkg/limiter/cost.go`
- Modify: `pkg/filters/limit.go`
- Modify: `pkg/policy/policy.go`

- [ ] **Step 1: 在 `policy.Policy` 结构体中添加计费策略 (BillingPolicy)**
  
  修改 `pkg/policy/policy.go`：

  ```go
  type Policy struct {
   LoadBalancePolicy    *LoadBalancePolicy     `yaml:"loadBalancePolicy" json:"loadBalancePolicy"`
   InvocationPolicy     *InvocationPolicy      `yaml:"invocationPolicy" json:"invocationPolicy"`
   LimitPolicies        []*LimitPolicy         `yaml:"limitPolicies" json:"limitPolicies"`
   RoutePolicies        []*RoutePolicy         `yaml:"routePolicies" json:"routePolicies"`
   CircuitBreakPolicies []*CircuitBreakPolicy  `yaml:"circuitBreakPolicies" json:"circuitBreakPolicies"`
   TagPolicies          []*TagPolicy           `yaml:"tagPolicies" json:"tagPolicies"`
   Permissions          []string               `yaml:"permissions" json:"permissions"`
   
   // 新增：动态计费价格表 (厘/1000 Tokens)
   Billing              *BillingPolicy         `yaml:"billing" json:"billing"`
  }

  type BillingPolicy struct {
   InputPrice  float64 `yaml:"inputPrice" json:"inputPrice"`   // 每 1000 字符/Token 价格
   OutputPrice float64 `yaml:"outputPrice" json:"outputPrice"` // 每 1000 字符/Token 价格
  }
  ```

- [ ] **Step 2: 实现 `CostLimitExecutor` 限流执行器**
  
  创建 `pkg/limiter/cost.go`：

  ```go
  package limiter

  import (
   "context"
   "net/http"
   "time"

   "tokenlive-gateway/pkg/core"
   "tokenlive-gateway/pkg/policy"
  )

  type CostLimitExecutor struct {
   stateStore core.StateStore
  }

  func NewCostLimitExecutor(ss core.StateStore) *CostLimitExecutor {
   return &CostLimitExecutor{stateStore: ss}
  }

  func (e *CostLimitExecutor) Execute(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
   limitKey := "rl:" + gctx.UserID + ":" + gctx.Model + ":" + lp.Name
   
   // 获取单价
   price := 0.002 // 默认兜底价格
   if gctx.Policy != nil && gctx.Policy.Billing != nil {
    price = gctx.Policy.Billing.InputPrice
   }

   // 估算 Token -> 换算估算费用
   estimateTokens := EstimatePromptTokens(gctx, lp)
   estimateCost := int64(float64(estimateTokens) * price * 1000) // 以厘为单位进行整型限流

   for _, sw := range lp.SlidingWindows {
    window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
    if window <= 0 {
     window = time.Minute
    }

    remaining, err := e.stateStore.RateLimitIncr(ctx, limitKey+":"+window.String(), estimateCost, window)
    if err != nil {
     return err
    }

    current := int64(10000) - remaining
    if remaining < 0 || current > sw.Threshold {
     _ = e.stateStore.RateLimitRefund(ctx, limitKey+":"+window.String(), estimateCost)
     return &HTTPError{Code: http.StatusTooManyRequests, Message: "cost limit exceeded (daily budget blown)"}
    }
   }
   return nil
  }

  func (e *CostLimitExecutor) Refund(ctx context.Context, gctx *core.GatewayContext, lp *policy.LimitPolicy) error {
   limitKey := "rl:" + gctx.UserID + ":" + gctx.Model + ":" + lp.Name
   
   price := 0.002
   if gctx.Policy != nil && gctx.Policy.Billing != nil {
    price = gctx.Policy.Billing.InputPrice
   }
   estimateTokens := EstimatePromptTokens(gctx, lp)
   estimateCost := int64(float64(estimateTokens) * price * 1000)

   for _, sw := range lp.SlidingWindows {
    window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
    if window <= 0 {
     window = time.Minute
    }
    _ = e.stateStore.RateLimitRefund(ctx, limitKey+":"+window.String(), estimateCost)
   }
   return nil
  }
  ```

- [ ] **Step 3: 在 `RateLimitFilter` 工厂中注册 `"cost"` 限流器**
  
  修改 `pkg/filters/limit.go`：

  ```go
  func NewRateLimitFilter(ss core.StateStore) *RateLimitFilter {
   if ss != nil {
    core.DefaultLimitExecutorFactory.Register("request", limiter.NewRequestLimitExecutor(ss))
    core.DefaultLimitExecutorFactory.Register("token", limiter.NewTokenLimitExecutor(ss))
    // 新增注册 cost 消费额度限流器
    core.DefaultLimitExecutorFactory.Register("cost", limiter.NewCostLimitExecutor(ss))
   }
   return &RateLimitFilter{stateStore: ss}
  }
  ```

- [ ] **Step 4: 运行所有限流单元测试验证正确性**
  
  运行：`go test -v ./pkg/filters/...`
  预期结果：PASS。

---

### Task 3: 落地基于首字延迟 (TTFT) 的慢调用熔断与 OpenAI 兼容降级

**Files:**

- Modify: `pkg/core/circuit_breaker.go`
- Modify: `pkg/store/redis.go`
- Modify: `pkg/store/memory.go`
- Modify: `pkg/invoker/cluster.go`
- Modify: `internal/handler/llm.go` (或 `pkg/core/engine.go`)

- [ ] **Step 1: 支持动态熔断参数传递与存储**
  
  修改 `pkg/store/store.go` 中 `CircuitBreakerRecord` 的定义，使其支持传入动态滑动窗口配置：

  ```go
  type StateStore interface {
   ...
   CircuitBreakerRecord(ctx context.Context, key string, success bool, windowSize, minCalls int, recoveryTimeout time.Duration) error
  }
  ```

  修改 `pkg/store/redis.go` 和 `pkg/store/memory.go` 调整签名，使 Redis Lua 脚本接收从策略中拉取出的窗口参数，而非使用底层静态常量。

- [ ] **Step 2: 重构 `CircuitBreakerManager` 以消费 `gctx.Policy` 参数**
  
  修改 `pkg/core/circuit_breaker.go` 传递动态策略：

  ```go
  func (cbm *CircuitBreakerManager) RecordSuccess(gctx *GatewayContext, ep *Endpoint) {
   ctx := gctx.Ctx
   // 默认参数
   ws, mc, to := 100, 5, 30*time.Second
   if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
    p := gctx.Policy.CircuitBreakPolicies[0]
    ws, mc, to = p.SlidingWindowSize, p.MinCallsThreshold, time.Duration(p.WaitDurationInOpenState)*time.Millisecond
   }
   cbm.stateStore.CircuitBreakerRecord(ctx, ep.Provider+":"+ep.Model, true, ws, mc, to)
   cbm.stateStore.CircuitBreakerRecord(ctx, ep.ID, true, ws, mc, to)
  }

  func (cbm *CircuitBreakerManager) RecordFailure(gctx *GatewayContext, ep *Endpoint, err error) {
   ctx := gctx.Ctx
   ws, mc, to := 100, 5, 30*time.Second
   if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
    p := gctx.Policy.CircuitBreakPolicies[0]
    ws, mc, to = p.SlidingWindowSize, p.MinCallsThreshold, time.Duration(p.WaitDurationInOpenState)*time.Millisecond
   }
   cbm.stateStore.CircuitBreakerRecord(ctx, ep.Provider+":"+ep.Model, false, ws, mc, to)
   cbm.stateStore.CircuitBreakerRecord(ctx, ep.ID, false, ws, mc, to)
  }
  ```

- [ ] **Step 3: 在 `ClusterInvoker.Invoke` 中植入 TTFT 首字慢调用熔断判定**
  
  修改 `pkg/invoker/cluster.go`，在请求执行成功但耗时（即 `gctx.TTFT`）超过慢调用阈值时，将其记录为熔断失败：

  ```go
    // 执行调用成功
    if err == nil {
     // 核心熔断判断：判定是否为首包慢调用
     isSlowCall := false
     if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
      p := gctx.Policy.CircuitBreakPolicies[0]
      if p.SlowCallMetric == "TTFT" && gctx.TTFT > 0 {
       limit := time.Duration(p.SlowCallDurationThreshold) * time.Millisecond
       if gctx.TTFT > limit {
        isSlowCall = true
       }
      }
     }

     if isSlowCall {
      ci.logger.Warn("successful request marked as CB slow-call failure", zap.Duration("ttft", gctx.TTFT))
      ci.cbManager.RecordFailure(gctx, gctx.SelectedEndpoint, fmt.Errorf("slow call TTFT exceeded"))
     } else {
      ci.cbManager.RecordSuccess(gctx, gctx.SelectedEndpoint)
     }
     
     ci.stateStore.RecordLatency(gctx.Ctx, gctx.SelectedEndpoint.ID, time.Since(gctx.UpstreamConnect))
     return nil
    }
  ```

- [ ] **Step 4: 实现熔断时的 OpenAI 兼容 JSON 降级响应**
  
  在 Gin Web 适配器层（或 Core Engine 的全局错误拦截阶段），当请求因熔断（或者 Invoker 调用失败）导致无法继续服务时，捕获异常并基于 `gctx.Policy.CircuitBreakPolicies[0].DegradeConfig` 动态返回符合格式的 OpenAI 报错：
  
  修改 `pkg/core/engine.go` 在 HandleRequest 发生错误时的处理：

  ```go
  // 在错误返回阶段，动态评估 DegradeConfig
  if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
   degrade := gctx.Policy.CircuitBreakPolicies[0].DegradeConfig
   if degrade != nil && degrade.Type == "OPENAI_ERROR" {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(degrade.ResponseCode)
    // 返回 OpenAI 标准的 JSON 错误
    errMsg := fmt.Sprintf(`{"error":{"message":%q,"type":"gateway_error","code":"service_unavailable"}}`, degrade.ErrorMessage)
    _, _ = w.Write([]byte(errMsg))
    return
   }
  }
  ```
