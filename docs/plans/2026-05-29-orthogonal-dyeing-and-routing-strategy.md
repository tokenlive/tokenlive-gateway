# 染色与筛选正交路由设计（ADR 0006）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 ADR 0006，在 `ClusterInvoker` 路由链中实现基于已染色标签（`gctx.Tags`）与 `RoutePolicies` 规则的动态路由筛选路由器 `DynamicRoutePolicyRouter`，替换旧的微服务遗留逻辑，支持 VIP 直连降级逃生。

**Architecture:** 在 `pkg/routers` 下实现符合 `core.Router` 契约的路由器；在 InboundFilter `TaggingFilter` 染色完毕后，通过路由器对候选 `Endpoints` 进行硬性筛选与加权分流；若筛选结果为空自动退避回默认候选集。

**Tech Stack:** Go (Golang), go-test, YAML

---

### Task 1: 扩展并对齐策略定义结构体 (Policy Schema Alignment)

**Files:**

- Modify: `pkg/policy/invoke.go`
- Modify: `pkg/policy/limit.go`
- Modify: `pkg/policy/circuit_break.go`
- Modify: `pkg/policy/policy_test.go`

- [ ] **Step 1: 在 policy 结构体中添加 LLM 专用策略配置字段**
  
  修改 `pkg/policy/invoke.go` 扩展 `RetryPolicy` 超时定义：

  ```go
  type RetryPolicy struct {
   Retry      int      `yaml:"retry" json:"retry"`
   Interval   int      `yaml:"interval" json:"interval"` // 毫秒
   Timeout    int      `yaml:"timeout" json:"timeout"`   // 毫秒
   ErrorCodes []int    `yaml:"errorCodes" json:"errorCodes"`
   Methods    []string `yaml:"methods" json:"methods"`
   Exceptions []string `yaml:"exceptions" json:"exceptions"`
   Version    int64    `yaml:"version" json:"version"`

   // 新增 LLM 特性超时
   ConnectTimeout       int `yaml:"connectTimeout" json:"connectTimeout"`             // 连接建立超时(ms)
   TTFTimeout           int `yaml:"ttftTimeout" json:"ttftTimeout"`                   // 首字超时(ms)
   TotalTimeout         int `yaml:"totalTimeout" json:"totalTimeout"`                 // 请求整超时(ms)
  }
  ```

  修改 `pkg/policy/limit.go` 支持 Token 精确分词器估算 `Estimator` 配置：

  ```go
  type LimitPolicy struct {
   Name           string          `yaml:"name" json:"name"`
   Version        int64           `yaml:"version" json:"version"`
   Type           string          `yaml:"type" json:"type"` // "request", "token", "cost"
   SlidingWindows []*SlidingWindow `yaml:"slidingWindows" json:"slidingWindows"`
   MaxWaitMs      int             `yaml:"maxWaitMs" json:"maxWaitMs"`
   RelationType   string          `yaml:"relationType" json:"relationType"` // "AND", "OR"
   Conditions     []*matcher.Condition `yaml:"conditions" json:"conditions"`

   // 新增分词估算器配置
   Estimator      *EstimatorConfig `yaml:"estimator" json:"estimator"`
  }

  type EstimatorConfig struct {
   Type  string  `yaml:"type" json:"type"`   // e.g. "length_ratio", "tiktoken"
   Ratio float64 `yaml:"ratio" json:"ratio"` // 字符数转 Token 的换算比率
  }
  ```

  修改 `pkg/policy/circuit_break.go` 引入慢调用指标与降级类型：

  ```go
  type CircuitBreakPolicy struct {
   Name                      string         `yaml:"name" json:"name"`
   Level                     string         `yaml:"level" json:"level"` // e.g. "SERVICE", "INSTANCE"
   SlidingWindowType         string         `yaml:"slidingWindowType" json:"slidingWindowType"`
   SlidingWindowSize         int            `yaml:"slidingWindowSize" json:"slidingWindowSize"`
   MinCallsThreshold         int            `yaml:"minCallsThreshold" json:"minCallsThreshold"`
   CodePolicy                *CodePolicy    `yaml:"codePolicy" json:"codePolicy"`
   ErrorCodes                []string       `yaml:"errorCodes" json:"errorCodes"`
   FailureRateThreshold      float64        `yaml:"failureRateThreshold" json:"failureRateThreshold"`
   SlowCallRateThreshold     float64        `yaml:"slowCallRateThreshold" json:"slowCallRateThreshold"`
   SlowCallDurationThreshold int            `yaml:"slowCallDurationThreshold" json:"slowCallDurationThreshold"`
   WaitDurationInOpenState   int            `yaml:"waitDurationInOpenState" json:"waitDurationInOpenState"`
   AllowedCallsInHalfOpenState int          `yaml:"allowedCallsInHalfOpenState" json:"allowedCallsInHalfOpenState"`
   ForceOpen                 bool           `yaml:"forceOpen" json:"forceOpen"`
   RealizeType               string         `yaml:"realizeType" json:"realizeType"`
   DegradeConfig             *DegradeConfig `yaml:"degradeConfig" json:"degradeConfig"`
   Version                   int            `yaml:"version" json:"version"`

   // 新增 LLM 特性熔断字段
   SlowCallMetric            string         `yaml:"slowCallMetric" json:"slowCallMetric"` // e.g. "TTFT"
  }

  type DegradeConfig struct {
   ResponseCode int               `yaml:"responseCode" json:"responseCode"`
   Attributes   map[string]string `yaml:"attributes" json:"attributes"`
   ResponseBody string            `yaml:"responseBody" json:"responseBody"`

   // 新增标准 OpenAI 降级类型
   Type         string            `yaml:"type" json:"type"`         // e.g. "OPENAI_ERROR", "OPENAI_COMPLETION"
   ErrorMessage string            `yaml:"errorMessage" json:"errorMessage"`
  }
  ```

- [ ] **Step 2: 编写 Policy 反序列化测试用例以验证新增字段匹配**
  
  修改 `pkg/policy/policy_test.go`，添加 `TestUnmarshalLLMPolicies` 验证包含新增字段的 JSON 是否能正确反序列化：

  ```go
  func TestUnmarshalLLMPolicies(t *testing.T) {
   rawJSON := `{
    "invocationPolicy": {
     "type": "failover",
     "retryPolicy": {
      "retry": 3,
      "connectTimeout": 2000,
      "ttftTimeout": 5000,
      "totalTimeout": 60000
     }
    },
    "limitPolicies": [
     {
      "name": "tpm-limit",
      "type": "token",
      "estimator": {
       "type": "length_ratio",
       "ratio": 0.5
      }
     }
    ],
    "circuitBreakPolicies": [
     {
      "name": "cb-gpt-4",
      "slowCallMetric": "TTFT",
      "degradeConfig": {
       "type": "OPENAI_ERROR",
       "errorMessage": "service overloaded"
      }
     }
    ]
   }`

   var p Policy
   err := json.Unmarshal([]byte(rawJSON), &p)
   if err != nil {
    t.Fatalf("failed to unmarshal: %v", err)
   }

   if p.InvocationPolicy.RetryPolicy.TTFTimeout != 5000 {
    t.Errorf("expected ttftTimeout 5000, got %d", p.InvocationPolicy.RetryPolicy.TTFTimeout)
   }
   if p.LimitPolicies[0].Estimator.Type != "length_ratio" {
    t.Errorf("expected estimator length_ratio, got %s", p.LimitPolicies[0].Estimator.Type)
   }
   if p.CircuitBreakPolicies[0].SlowCallMetric != "TTFT" {
    t.Errorf("expected slowCallMetric TTFT, got %s", p.CircuitBreakPolicies[0].SlowCallMetric)
   }
   if p.CircuitBreakPolicies[0].DegradeConfig.Type != "OPENAI_ERROR" {
    t.Errorf("expected degrade type OPENAI_ERROR, got %s", p.CircuitBreakPolicies[0].DegradeConfig.Type)
   }
  }
  ```

- [ ] **Step 3: 运行测试以确保失败 (红)**
  
  运行命令：

  ```bash
  go test -v ./pkg/policy -run TestUnmarshalLLMPolicies
  ```

  预期结果：编译失败（结构体字段不存在）。

- [ ] **Step 4: 补全字段定义并使测试通过 (绿)**
  
  在前面提到的结构体中定义好字段，并导入 `encoding/json` 执行测试：

  ```bash
  go test -v ./pkg/policy -run TestUnmarshalLLMPolicies
  ```

  预期结果：PASS。

- [ ] **Step 5: 提交当前修改**
  
  ```bash
  git add pkg/policy/
  git commit -m "feat: align policy structures with LLM-specific fields"
  ```

---

### Task 2: 编写 `DynamicRoutePolicyRouter` 动态路由筛选器

**Files:**

- Create: `pkg/routers/route_policy.go`
- Create: `pkg/routers/route_policy_test.go`

- [ ] **Step 1: 编写路由筛选器测试用例，明确筛选与加权分流场景**
  
  在 `pkg/routers/route_policy_test.go` 中编写红灯测试：

  ```go
  package routers

  import (
   "context"
   "testing"

   "tokenlive-gateway/pkg/core"
   "tokenlive-gateway/pkg/matcher"
   "tokenlive-gateway/pkg/policy"
   "go.uber.org/zap"
  )

  func TestDynamicRoutePolicyRouter_Route(t *testing.T) {
   logger := zap.NewNop()
   router := NewDynamicRoutePolicyRouter(logger)

   // 准备候选端点
   epPremium := &core.Endpoint{
    ID:     "ep-premium",
    Weight: 100,
    Metadata: map[string]string{
     "endpoint_tier": "premium",
    },
   }
   epStandard := &core.Endpoint{
    ID:     "ep-standard",
    Weight: 100,
    Metadata: map[string]string{
     "endpoint_tier": "standard",
    },
   }
   endpoints := []*core.Endpoint{epPremium, epStandard}

   // 1. VIP 染色路由测试
   gctx := &core.GatewayContext{
    Ctx:  context.Background(),
    Tags: map[string]string{"priority": "high"},
    Policy: &policy.Policy{
     RoutePolicies: []*policy.RoutePolicy{
      {
       Name:  "vip-route",
       Order: 1,
       TagRules: []*policy.TagRule{
        {
         RelationType: "AND",
         Conditions: []*matcher.Condition{
          {
           Type:   "tag",
           OpType: "EQUAL",
           Key:    "priority",
           Values: []string{"high"},
          },
         },
         Destinations: []*policy.Destination{
          {
           Weight:       100,
           RelationType: "AND",
           Conditions: []*matcher.Condition{
            {
             OpType: "EQUAL",
             Key:    "endpoint_tier",
             Values: []string{"premium"},
            },
           },
          },
         },
        },
       },
      },
     },
    },
   }

   res := router.Route(gctx, endpoints)
   if len(res) != 1 || res[0].ID != "ep-premium" {
    t.Errorf("expected routed to premium endpoint, got %+v", res)
   }

   // 2. 降级逃生测试：如果 premium 专线端点全部不可用，应退避逃生回默认端点
   resEscape := router.Route(gctx, []*core.Endpoint{epStandard})
   if len(resEscape) != 1 || resEscape[0].ID != "ep-standard" {
    t.Errorf("expected escape to standard endpoint, got %+v", resEscape)
   }
  }
  ```

- [ ] **Step 2: 运行测试以确保失败 (红)**
  
  ```bash
  go test -v ./pkg/routers -run TestDynamicRoutePolicyRouter_Route
  ```

  预期结果：编译失败（`NewDynamicRoutePolicyRouter` 未定义）。

- [ ] **Step 3: 实现 `DynamicRoutePolicyRouter` 筛选逻辑 (绿)**
  
  创建 `pkg/routers/route_policy.go` 并实现过滤与加权：

  ```go
  package routers

  import (
   "math/rand"
   "sort"
   "strings"

   "tokenlive-gateway/pkg/core"
   "tokenlive-gateway/pkg/matcher"
   "tokenlive-gateway/pkg/policy"
   "go.uber.org/zap"
  )

  type DynamicRoutePolicyRouter struct {
   logger *zap.Logger
  }

  func NewDynamicRoutePolicyRouter(logger *zap.Logger) *DynamicRoutePolicyRouter {
   return &DynamicRoutePolicyRouter{logger: logger}
  }

  func (r *DynamicRoutePolicyRouter) Name() string { return "dynamic_route" }

  func (r *DynamicRoutePolicyRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
   if gctx.Policy == nil || len(gctx.Policy.RoutePolicies) == 0 {
    return endpoints
   }

   // 按 Order 排序 RoutePolicies
   routePolicies := make([]*policy.RoutePolicy, len(gctx.Policy.RoutePolicies))
   copy(routePolicies, gctx.Policy.RoutePolicies)
   sort.Slice(routePolicies, func(i, j int) bool {
    return routePolicies[i].Order < routePolicies[j].Order
   })

   for _, rp := range routePolicies {
    // 按 TagRules 优先级排序并匹配
    tagRules := rp.TagRules
    sort.Slice(tagRules, func(i, j int) bool {
     return tagRules[i].Order < tagRules[j].Order
    })

    for _, rule := range tagRules {
     if r.matchConditions(gctx, rule.Conditions, rule.RelationType) {
      // 匹配成功，开始进行加权 Destination 选择
      dest := r.selectDestination(rule.Destinations)
      if dest == nil {
       continue
      }

      // 筛选出符合所选 Destination 条件的下游 Endpoints
      filtered := r.filterEndpoints(endpoints, dest.Conditions, dest.RelationType)
      if len(filtered) > 0 {
       r.logger.Info("dynamic route matched successful subset",
        zap.String("policy", rp.Name),
        zap.Int("selected_subset_size", len(filtered)))
       return filtered
      }

      // 降级逃生逻辑：如果筛选出的专用通道无可达端点，记录警告，继续寻找其它规则或退避
      r.logger.Warn("dynamic route matched subset is empty, triggering fallback to default pool",
       zap.String("policy", rp.Name))
     }
    }
   }

   return endpoints
  }

  func (r *DynamicRoutePolicyRouter) matchConditions(gctx *core.GatewayContext, conds []*matcher.Condition, relation string) bool {
   if len(conds) == 0 {
    return true
   }
   isAnd := relation != "OR"
   for _, cond := range conds {
    m := matcher.DefaultTagMatcherFactory.Get(cond.Type)
    matched := m != nil && m.Match(gctx.Ctx, cond, gctx)
    if isAnd && !matched {
     return false
    }
    if !isAnd && matched {
     return true
    }
   }
   return isAnd
  }

  func (r *DynamicRoutePolicyRouter) selectDestination(dests []*policy.Destination) *policy.Destination {
   if len(dests) == 0 {
    return nil
   }
   totalWeight := 0
   for _, d := range dests {
    totalWeight += d.Weight
   }
   if totalWeight <= 0 {
    return dests[0]
   }
   val := rand.Intn(totalWeight)
   curr := 0
   for _, d := range dests {
    curr += d.Weight
    if val < curr {
     return d
    }
   }
   return dests[0]
  }

  func (r *DynamicRoutePolicyRouter) filterEndpoints(endpoints []*core.Endpoint, conds []*matcher.Condition, relation string) []*core.Endpoint {
   var result []*core.Endpoint
   for _, ep := range endpoints {
    if r.matchEndpointMetadata(ep, conds, relation) {
     result = append(result, ep)
    }
   }
   return result
  }

  func (r *DynamicRoutePolicyRouter) matchEndpointMetadata(ep *core.Endpoint, conds []*matcher.Condition, relation string) bool {
   if len(conds) == 0 {
    return true
   }
   isAnd := relation != "OR"
   for _, cond := range conds {
    val := ep.Metadata[cond.Key]
    var matched bool
    if val != "" {
     matched = cond.MatchValues([]string{val})
    } else {
     matched = cond.MatchValues(nil)
    }

    if isAnd && !matched {
     return false
    }
    if !isAnd && matched {
     return true
    }
   }
   return isAnd
  }
  ```

- [ ] **Step 4: 再次执行测试验证成功 (绿)**
  
  ```bash
  go test -v ./pkg/routers -run TestDynamicRoutePolicyRouter_Route
  ```

  预期结果：PASS。

- [ ] **Step 5: 提交组件实现**
  
  ```bash
  git add pkg/routers/route_policy.go pkg/routers/route_policy_test.go
  git commit -m "feat: implement DynamicRoutePolicyRouter with weighted select and escape fallback"
  ```

---

### Task 3: 在网关接线层注册新路由器并启用

**Files:**

- Modify: `cmd/server/wire/provider.go:128-137`
- Modify: `config/local.yml:151-200`

- [ ] **Step 1: 在 `provider.go` 的路由器工厂中注册 `dynamic_route`**
  
  修改 `cmd/server/wire/provider.go`，在 `engine.RegisterRouterFactory` 区块添加动态路由注册：

  ```go
   // 注册 Router 工厂
   engine.RegisterRouterFactory("capability", func(cfg core.RouterConfig, _ core.StateStore, _ *zap.Logger) core.Router {
    return &routers.CapabilityRouter{}
   })
   engine.RegisterRouterFactory("circuit_breaker", func(cfg core.RouterConfig, ss core.StateStore, l *zap.Logger) core.Router {
    return routers.NewCircuitBreakerRouter(ss, l)
   })
   engine.RegisterRouterFactory("tag", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
    return routers.NewTagRouter(cfg.Tags, l)
   })
   // 新增：注册 ADR 0006 动态路由筛选路由器
   engine.RegisterRouterFactory("dynamic_route", func(cfg core.RouterConfig, _ core.StateStore, l *zap.Logger) core.Router {
    return routers.NewDynamicRoutePolicyRouter(l)
   })
  ```

- [ ] **Step 2: 在 `local.yml` 默认管线的 Invoker 路由器链中开启新路由器**
  
  修改 `config/local.yml` 的 `pipelines` 节点，在 `chat_completion` 和 `embedding` 的 `routers` 链中，在 `capability` 与 `circuit_breaker` 之间插入 `"dynamic_route"`：

  ```yaml
    chat_completion:
      name: "chat_completion"
      request_types: ["chat_completion"]
      inbound_filters: ["auth", "session_reader", "validate"]
      outbound_filters: ["token_settlement", "sticky_session", "metrics", "access_log"]
      critical_outbound_filters: ["token_settlement", "sticky_session"]
      invoker:
        type: "cluster"
        routers: ["capability", "dynamic_route", "circuit_breaker"]
        load_balancer: "round_robin"
  ```

  ```yaml
    embedding:
      name: "embedding"
      request_types: ["embedding"]
      inbound_filters: ["auth", "session_reader", "validate"]
      outbound_filters: ["token_settlement", "sticky_session", "metrics", "access_log"]
      critical_outbound_filters: ["token_settlement", "sticky_session"]
      invoker:
        type: "cluster"
        routers: ["capability", "dynamic_route", "circuit_breaker"]
        load_balancer: "round_robin"
  ```

- [ ] **Step 3: 运行全局单元测试以保证注册后没有任何编译与逻辑断档 (绿)**
  
  ```bash
  go test -v ./...
  ```

  预期结果：所有测试全部通过（PASS）。

- [ ] **Step 4: 提交注册与配置修改**
  
  ```bash
  git add cmd/server/wire/provider.go config/local.yml
  git commit -m "feat: register dynamic_route factory and enable it in local pipeline configuration"
  ```
