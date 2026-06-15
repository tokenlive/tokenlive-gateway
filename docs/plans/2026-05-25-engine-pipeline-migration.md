# Engine Pipeline 架构迁移实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 tokenlive-gateway 从单体 LLM 路由层迁移到 "Gin Shell + Engine Pipeline" 架构，实现三层 Filter 模型 + Invoker 抽象 + Router 链过滤。

**Architecture:** Engine 作为纯 net/http 核心管线，在 Gin Handler 内部运行。请求流经 InboundFilter(1x) → FallbackInvoker(可重试) → OutboundFilter(1x)。ClusterInvoker 编排 Discovery → RouterChain → LoadBalancer → ProviderInvoker。

**Tech Stack:** Go, Gin, Redis (StateStore + CompensationQueue), Prometheus, zap, gjson

---

## 概述

本计划将架构迁移拆分为 8 个阶段，共 25 个任务。每个阶段产出独立可测试的代码。

| 阶段 | 内容 | 依赖 |
|------|------|------|
| Phase 1 | 基础类型与接口 | 无 |
| Phase 2 | StateStore | Phase 1 |
| Phase 3 | Discovery 适配 + Router | Phase 1 |
| Phase 4 | LoadBalancer 适配 | Phase 1, 3 |
| Phase 5 | Invoker 层级 | Phase 1-4 |
| Phase 6 | Filter 体系 | Phase 1-2, 5 |
| Phase 7 | SSE 拦截 + Engine 组装 | Phase 1-6 |
| Phase 8 | Handler 迁移 + 配置热加载 | Phase 7 |

---

## Phase 1: 基础类型与接口

### Task 1: 创建 pkg/gateway 包基础类型

**Files:**

- Create: `pkg/gateway/types.go`
- Create: `pkg/gateway/types_test.go`

- [ ] **Step 1: 创建 RequestType 常量和基础类型**

```go
// pkg/gateway/types.go
package gateway

import (
 "context"
 "net/http"
 "sync"
 "time"
)

// RequestType 请求类型枚举
type RequestType string

const (
 RequestTypeChatCompletion  RequestType = "chat_completion"
 RequestTypeEmbedding       RequestType = "embedding"
 RequestTypeImageGeneration RequestType = "image_generation"
 RequestTypeResponses       RequestType = "responses"
 RequestTypeModelList       RequestType = "model_list"
)

// Endpoint Gateway 层的端点视图
type Endpoint struct {
 ID           string
 URL          string
 Provider     string
 Model        string
 Metadata     map[string]string
 Weight       int
 RequestTypes []RequestType
}

// SupportsRequestType 检查端点是否支持指定请求类型
func (ep *Endpoint) SupportsRequestType(rt RequestType) bool {
 for _, c := range ep.RequestTypes {
  if c == rt {
   return true
  }
 }
 return false
}

// CostPerToken 从 metadata 获取每 token 成本
func (ep *Endpoint) CostPerToken() float64 {
 if v, ok := ep.Metadata["cost_per_token"]; ok {
  var f float64
  _, _ = fmt.Sscanf(v, "%f", &f)
  return f
 }
 return 0
}

// AttemptRecord 单次尝试记录
type AttemptRecord struct {
 Model      string
 EndpointID string
 Provider   string
 Latency    time.Duration
 StatusCode int
 Error      string
 Timestamp  time.Time
}

// CircuitState 熔断器状态
type CircuitState int

const (
 CircuitClosed CircuitState = iota
 CircuitOpen
 CircuitHalfOpen
)
```

- [ ] **Step 2: 创建 GatewayContext 结构体**

```go
// pkg/gateway/context.go
package gateway

import (
 "context"
 "net/http"
 "sync"
 "time"
)

// GatewayContext 贯穿整个管线的请求上下文
// 不实现 context.Context 接口（强类型字段优先）
type GatewayContext struct {
 // ===== 请求常量（不可变） =====
 Ctx            context.Context
 Request        *http.Request
 ResponseWriter http.ResponseWriter
 RawBody        []byte
 RequestType    RequestType
 OriginalModel  string
 IsStream       bool

 // InboundFilter 填充
 APIKey    string
 UserID    string
 SessionID string

 // ===== 决策结果（Fallback 可重写 Model） =====
 Model  string
 Policy *Policy

 // ===== Per-attempt（ResetAttempt 清空） =====
 SelectedInvoker  *ProviderInvoker
 SelectedEndpoint *Endpoint
 UpstreamConnect  time.Time
 UpstreamResponse *http.Response
 UpstreamBody     []byte
 UpstreamError    error
 TTFT             time.Duration

 // ===== 累积字段 =====
 AttemptCount  int
 FallbackChain []string
 History       []AttemptRecord
 StartTime     time.Time
 TotalLatency  time.Duration

 // ===== 最终结果 =====
 PromptTokens     int
 CompletionTokens int
 Cost             float64
 Response         interface{}
 Err              error
}

// ResetAttempt 清空 per-attempt 字段
func (c *GatewayContext) ResetAttempt() {
 c.SelectedInvoker = nil
 c.SelectedEndpoint = nil
 c.UpstreamConnect = time.Time{}
 c.UpstreamResponse = nil
 c.UpstreamBody = nil
 c.UpstreamError = nil
 // TTFT 不重置 —— 一旦置位表示已发首字节
}

// RecordAttempt 推一条 attempt 记录
func (c *GatewayContext) RecordAttempt(success bool) {
 c.History = append(c.History, AttemptRecord{
  Model:      c.Model,
  EndpointID: c.SelectedEndpoint.ID,
  Provider:   c.SelectedEndpoint.Provider,
  Latency:    time.Since(c.UpstreamConnect),
  StatusCode: getStatusCode(c.UpstreamResponse),
  Error:      getErrorString(c.UpstreamError),
  Timestamp:  time.Now(),
 })
 c.AttemptCount++
}

func getStatusCode(resp *http.Response) int {
 if resp != nil {
  return resp.StatusCode
 }
 return 0
}

func getErrorString(err error) string {
 if err != nil {
  return err.Error()
 }
 return ""
}

// ===== 池化 =====
var ctxPool = sync.Pool{
 New: func() any { return &GatewayContext{} },
}

// AcquireContext 从池中获取并初始化 GatewayContext
func AcquireContext(w http.ResponseWriter, r *http.Request) *GatewayContext {
 gctx := ctxPool.Get().(*GatewayContext)
 gctx.Ctx = r.Context()
 gctx.Request = r
 gctx.ResponseWriter = w
 gctx.StartTime = time.Now()
 return gctx
}

// ReleaseContext 归还 GatewayContext 到池
func ReleaseContext(gctx *GatewayContext) {
 *gctx = GatewayContext{}
 ctxPool.Put(gctx)
}
```

- [ ] **Step 3: 创建 Invoker 接口**

```go
// pkg/gateway/invoker.go
package gateway

// Invoker 统一的"可被调用"抽象
type Invoker interface {
 Invoke(gctx *GatewayContext) error
}
```

- [ ] **Step 4: 创建 ErrorMatcher 原语**

```go
// pkg/gateway/error_matcher.go
package gateway

import "regexp"

// ErrorMatcher 错误识别原语
type ErrorMatcher struct {
 StatusCodes     []int
 ErrorCodes      []string
 MessagePatterns []string // regex
 compiledRegexps []*regexp.Regexp
}

// Match 检查错误是否匹配
func (em *ErrorMatcher) Match(statusCode int, errCode string, errMsg string) bool {
 for _, code := range em.StatusCodes {
  if code == statusCode {
   return true
  }
 }
 for _, code := range em.ErrorCodes {
  if code == errCode {
   return true
  }
 }
 for _, re := range em.compiledRegexps {
  if re.MatchString(errMsg) {
   return true
  }
 }
 return false
}

// Compile 编译正则表达式
func (em *ErrorMatcher) Compile() error {
 em.compiledRegexps = make([]*regexp.Regup, len(em.MessagePatterns))
 for i, pattern := range em.MessagePatterns {
  re, err := regexp.Compile(pattern)
  if err != nil {
   return err
  }
  em.compiledRegexps[i] = re
 }
 return nil
}

// RetryRule 重试规则
type RetryRule struct {
 Matcher ErrorMatcher
 Retry   bool
}

// CircuitBreakerRule 熔断规则
type CircuitBreakerRule struct {
 Matcher ErrorMatcher
 Failure bool
}

// FallbackRule 降级规则
type FallbackRule struct {
 Matcher  ErrorMatcher
 Fallback bool
}
```

- [ ] **Step 5: 编写基础类型测试**

```go
// pkg/gateway/types_test.go
package gateway

import (
 "testing"
 "net/http/httptest"
 "net/http"
)

func TestEndpoint_SupportsRequestType(t *testing.T) {
 ep := &Endpoint{
  ID:           "ep1",
  RequestTypes: []RequestType{RequestTypeChatCompletion, RequestTypeEmbedding},
 }

 if !ep.SupportsRequestType(RequestTypeChatCompletion) {
  t.Error("expected support for chat_completion")
 }
 if ep.SupportsRequestType(RequestTypeImageGeneration) {
  t.Error("unexpected support for image_generation")
 }
}

func TestGatewayContext_ResetAttempt(t *testing.T) {
 gctx := &GatewayContext{
  Model:            "gpt-4",
  SelectedEndpoint: &Endpoint{ID: "ep1"},
  TTFT:             100,
 }

 gctx.ResetAttempt()

 if gctx.SelectedEndpoint != nil {
  t.Error("expected SelectedEndpoint to be nil after reset")
 }
 if gctx.TTFT != 100 {
  t.Error("expected TTFT to be preserved after reset")
 }
}

func TestGatewayContext_Pool(t *testing.T) {
 w := httptest.NewRecorder()
 r := httptest.NewRequest("POST", "/v1/chat/completions", nil)

 gctx := AcquireContext(w, r)
 if gctx.ResponseWriter != w {
  t.Error("expected ResponseWriter to be set")
 }

 ReleaseContext(gctx)

 // 获取同一个对象（池化验证）
 gctx2 := AcquireContext(w, r)
 if gctx2 == nil {
  t.Error("expected non-nil from pool")
 }
}

func TestErrorMatcher_Match(t *testing.T) {
 em := &ErrorMatcher{
  StatusCodes: []int{429, 500, 502, 503},
  ErrorCodes:  []string{"rate_limit_exceeded"},
 }

 if !em.Match(429, "", "") {
  t.Error("expected match for status 429")
 }
 if !em.Match(0, "rate_limit_exceeded", "") {
  t.Error("expected match for error code")
 }
 if em.Match(400, "", "") {
  t.Error("unexpected match for status 400")
 }
}
```

- [ ] **Step 6: 运行测试**

Run: `go test ./pkg/gateway/... -v`
Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add pkg/gateway/
git commit -m "feat(gateway): 添加基础类型、GatewayContext、Invoker 接口和 ErrorMatcher"
```

---

### Task 2: 创建 PolicyMatcher

**Files:**

- Create: `pkg/gateway/policy.go`
- Create: `pkg/gateway/policy_test.go`

- [ ] **Step 1: 定义 Policy 和 PolicyMatcher**

```go
// pkg/gateway/policy.go
package gateway

import "strings"

// Policy 单个 Filter 内部的策略配置
type Policy struct {
 ID         string
 RPM        int64
 TPM        int64
 Match      PolicyMatchKey
 Extra      map[string]any
}

// PolicyMatchKey 策略匹配键
type PolicyMatchKey struct {
 APIKey string
 Model  string
 User   string
}

// PolicyMatcher 维度优先级匹配器
// 优先级: apikey+model+user > apikey+model > model > apikey > default
type PolicyMatcher struct {
 policies []*Policy
}

// NewPolicyMatcher 创建 PolicyMatcher
func NewPolicyMatcher(policies []*Policy) *PolicyMatcher {
 return &PolicyMatcher{policies: policies}
}

// Match 按维度优先级匹配策略
func (pm *PolicyMatcher) Match(gctx *GatewayContext) *Policy {
 var best *Policy
 bestScore := 0

 for _, p := range pm.policies {
  score := pm.calcScore(p.Match, gctx)
  if score > bestScore {
   bestScore = score
   best = p
  }
 }

 return best
}

// calcScore 计算匹配分数
// apikey+model+user = 7, apikey+model = 6, model = 4, apikey = 2, default = 0
func (pm *PolicyMatcher) calcScore(key PolicyMatchKey, gctx *GatewayContext) int {
 score := 0

 if key.APIKey != "" {
  if !matchWildcard(key.APIKey, gctx.APIKey) {
   return 0
  }
  score += 2
 }

 if key.Model != "" {
  if key.Model != gctx.Model {
   return 0
  }
  score += 4
 }

 if key.User != "" {
  if key.User != gctx.UserID {
   return 0
  }
  score += 1
 }

 return score
}

// matchWildcard 支持 * 通配符匹配
func matchWildcard(pattern, s string) bool {
 if pattern == "" {
  return true
 }
 if !strings.Contains(pattern, "*") {
  return pattern == s
 }
 // 简单通配符实现
 if pattern == "*" {
  return true
 }
 if strings.HasSuffix(pattern, "*") {
  return strings.HasPrefix(s, pattern[:len(pattern)-1])
 }
 if strings.HasPrefix(pattern, "*") {
  return strings.HasSuffix(s, pattern[1:])
 }
 return pattern == s
}
```

- [ ] **Step 2: 编写 PolicyMatcher 测试**

```go
// pkg/gateway/policy_test.go
package gateway

import "testing"

func TestPolicyMatcher_Priority(t *testing.T) {
 policies := []*Policy{
  {ID: "default", Match: PolicyMatchKey{}},
  {ID: "by_model", Match: PolicyMatchKey{Model: "gpt-4"}},
  {ID: "by_apikey", Match: PolicyMatchKey{APIKey: "ak_*"}},
  {ID: "by_model_apikey", Match: PolicyMatchKey{Model: "gpt-4", APIKey: "ak_*"}},
  {ID: "full_match", Match: PolicyMatchKey{Model: "gpt-4", APIKey: "ak_test", User: "u1"}},
 }

 pm := NewPolicyMatcher(policies)

 tests := []struct {
  name     string
  gctx     *GatewayContext
  expected string
 }{
  {
   name: "full match wins",
   gctx: &GatewayContext{Model: "gpt-4", APIKey: "ak_test", UserID: "u1"},
   expected: "full_match",
  },
  {
   name: "model+apikey beats model",
   gctx: &GatewayContext{Model: "gpt-4", APIKey: "ak_other", UserID: "u2"},
   expected: "by_model_apikey",
  },
  {
   name: "model beats apikey",
   gctx: &GatewayContext{Model: "gpt-4", APIKey: "bk_test", UserID: "u2"},
   expected: "by_model",
  },
  {
   name: "apikey only",
   gctx: &GatewayContext{Model: "gpt-3.5", APIKey: "ak_test", UserID: "u2"},
   expected: "by_apikey",
  },
  {
   name: "default fallback",
   gctx: &GatewayContext{Model: "gpt-3.5", APIKey: "bk_test", UserID: "u2"},
   expected: "default",
  },
 }

 for _, tt := range tests {
  t.Run(tt.name, func(t *testing.T) {
   p := pm.Match(tt.gctx)
   if p == nil {
    t.Fatal("expected non-nil policy")
   }
   if p.ID != tt.expected {
    t.Errorf("expected %s, got %s", tt.expected, p.ID)
   }
  })
 }
}

func TestMatchWildcard(t *testing.T) {
 tests := []struct {
  pattern string
  s       string
  want    bool
 }{
  {"", "anything", true},
  {"exact", "exact", true},
  {"exact", "other", false},
  {"*", "anything", true},
  {"ak_*", "ak_test", true},
  {"ak_*", "bk_test", false},
  {"*_end", "test_end", true},
 }

 for _, tt := range tests {
  if got := matchWildcard(tt.pattern, tt.s); got != tt.want {
   t.Errorf("matchWildcard(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
  }
 }
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/gateway/... -v -run TestPolicy`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add pkg/gateway/policy.go pkg/gateway/policy_test.go
git commit -m "feat(gateway): 添加 PolicyMatcher 维度优先级匹配"
```

---

## Phase 2: StateStore

### Task 3: StateStore 接口与 Memory 实现

**Files:**

- Create: `pkg/store/store.go`
- Create: `pkg/store/memory.go`
- Create: `pkg/store/memory_test.go`

- [ ] **Step 1: 定义 StateStore 接口**

```go
// pkg/store/store.go
package store

import (
 "context"
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// StateStore 跨请求状态抽象
type StateStore interface {
 // 限流：投机预扣 + 精确结算
 RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (remaining int64, err error)
 RateLimitRefund(ctx context.Context, key string, tokens int64) error

 // 熔断：滑动窗口
 CircuitBreakerRecord(ctx context.Context, key string, success bool) error
 CircuitBreakerState(ctx context.Context, key string) (gateway.CircuitState, error)
 CircuitBreakerReset(ctx context.Context, key string) error

 // Sticky Session
 StickyGet(ctx context.Context, sessionKey string) (endpointID string, err error)
 StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error

 // 延迟统计
 RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error
 GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error)

 Close() error
}
```

- [ ] **Step 2: 实现 MemoryStateStore**

```go
// pkg/store/memory.go
package store

import (
 "context"
 "sync"
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// MemoryStateStore 单机开发/测试用内存实现
type MemoryStateStore struct {
 mu sync.RWMutex

 // 限流
 rateLimits map[string]*rateLimitBucket

 // 熔断
 circuitBreakers map[string]*circuitBreakerState

 // Sticky
 sticky map[string]*stickyEntry

 // 延迟
 latencies map[string]*latencyRing
}

type rateLimitBucket struct {
 tokens   int64
 window   time.Duration
 resetAt  time.Time
}

type circuitBreakerState struct {
 state       gateway.CircuitState
 failures    int
 successes   int
 total       int
 windowStart time.Time
 window      time.Duration
 openUntil   time.Time
 halfOpenMax int
 halfOpenCnt int
}

type stickyEntry struct {
 endpointID string
 expireAt   time.Time
}

type latencyRing struct {
 data     []time.Duration
 pos      int
 size     int
 window   time.Duration
 timestamps []time.Time
}

// NewMemoryStateStore 创建内存 StateStore
func NewMemoryStateStore() *MemoryStateStore {
 return &MemoryStateStore{
  rateLimits:      make(map[string]*rateLimitBucket),
  circuitBreakers: make(map[string]*circuitBreakerState),
  sticky:          make(map[string]*stickyEntry),
  latencies:       make(map[string]*latencyRing),
 }
}

func (m *MemoryStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
 m.mu.Lock()
 defer m.mu.Unlock()

 b, ok := m.rateLimits[key]
 now := time.Now()

 if !ok || now.After(b.resetAt) {
  b = &rateLimitBucket{
   tokens:  tokens,
   window:  window,
   resetAt: now.Add(window),
  }
  m.rateLimits[key] = b
  return tokens, nil
 }

 b.tokens -= tokens
 return b.tokens, nil
}

func (m *MemoryStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
 m.mu.Lock()
 defer m.mu.Unlock()

 if b, ok := m.rateLimits[key]; ok {
  b.tokens += tokens
 }
 return nil
}

func (m *MemoryStateStore) CircuitBreakerRecord(ctx context.Context, key string, success bool) error {
 m.mu.Lock()
 defer m.mu.Unlock()

 cb, ok := m.circuitBreakers[key]
 now := time.Now()

 if !ok || now.After(cb.windowStart.Add(cb.window)) {
  cb = &circuitBreakerState{
   state:       gateway.CircuitClosed,
   windowStart: now,
   window:      60 * time.Second,
   halfOpenMax: 3,
  }
  m.circuitBreakers[key] = cb
 }

 cb.total++
 if success {
  cb.successes++
 } else {
  cb.failures++
 }

 return nil
}

func (m *MemoryStateStore) CircuitBreakerState(ctx context.Context, key string) (gateway.CircuitState, error) {
 m.mu.RLock()
 defer m.mu.RUnlock()

 cb, ok := m.circuitBreakers[key]
 if !ok {
  return gateway.CircuitClosed, nil
 }

 now := time.Now()
 if cb.state == gateway.CircuitOpen && now.After(cb.openUntil) {
  return gateway.CircuitHalfOpen, nil
 }

 return cb.state, nil
}

func (m *MemoryStateStore) CircuitBreakerReset(ctx context.Context, key string) error {
 m.mu.Lock()
 defer m.mu.Unlock()

 delete(m.circuitBreakers, key)
 return nil
}

func (m *MemoryStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
 m.mu.RLock()
 defer m.mu.RUnlock()

 entry, ok := m.sticky[sessionKey]
 if !ok || time.Now().After(entry.expireAt) {
  return "", nil
 }
 return entry.endpointID, nil
}

func (m *MemoryStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
 m.mu.Lock()
 defer m.mu.Unlock()

 m.sticky[sessionKey] = &stickyEntry{
  endpointID: endpointID,
  expireAt:   time.Now().Add(ttl),
 }
 return nil
}

func (m *MemoryStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
 m.mu.Lock()
 defer m.mu.Unlock()

 ring, ok := m.latencies[endpointID]
 if !ok {
  ring = &latencyRing{
   data:   make([]time.Duration, 100),
   size:   100,
   window: 5 * time.Minute,
   timestamps: make([]time.Time, 100),
  }
  m.latencies[endpointID] = ring
 }

 ring.data[ring.pos] = latency
 ring.timestamps[ring.pos] = time.Now()
 ring.pos = (ring.pos + 1) % ring.size
 return nil
}

func (m *MemoryStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
 m.mu.RLock()
 defer m.mu.RUnlock()

 ring, ok := m.latencies[endpointID]
 if !ok {
  return 0, nil
 }

 var sum time.Duration
 var count int
 cutoff := time.Now().Add(-window)

 for i := 0; i < ring.size; i++ {
  if ring.timestamps[i].After(cutoff) {
   sum += ring.data[i]
   count++
  }
 }

 if count == 0 {
  return 0, nil
 }
 return sum / time.Duration(count), nil
}

func (m *MemoryStateStore) Close() error {
 return nil
}
```

- [ ] **Step 3: 编写 MemoryStateStore 测试**

```go
// pkg/store/memory_test.go
package store

import (
 "context"
 "testing"
 "time"
)

func TestMemoryStateStore_RateLimit(t *testing.T) {
 s := NewMemoryStateStore()
 ctx := context.Background()

 // 首次请求
 remaining, err := s.RateLimitIncr(ctx, "test", 10, time.Minute)
 if err != nil {
  t.Fatal(err)
 }
 if remaining != 10 {
  t.Errorf("expected 10, got %d", remaining)
 }

 // 消耗 3 个
 remaining, _ = s.RateLimitIncr(ctx, "test", 3, time.Minute)
 if remaining != 7 {
  t.Errorf("expected 7, got %d", remaining)
 }

 // 退款 1 个
 s.RateLimitRefund(ctx, "test", 1)
 remaining, _ = s.RateLimitIncr(ctx, "test", 0, time.Minute)
 if remaining != 8 {
  t.Errorf("expected 8, got %d", remaining)
 }
}

func TestMemoryStateStore_Sticky(t *testing.T) {
 s := NewMemoryStateStore()
 ctx := context.Background()

 // 设置
 s.StickySet(ctx, "sess1", "ep1", time.Hour)

 // 获取
 epID, _ := s.StickyGet(ctx, "sess1")
 if epID != "ep1" {
  t.Errorf("expected ep1, got %s", epID)
 }

 // 不存在
 epID, _ = s.StickyGet(ctx, "sess2")
 if epID != "" {
  t.Errorf("expected empty, got %s", epID)
 }
}

func TestMemoryStateStore_Latency(t *testing.T) {
 s := NewMemoryStateStore()
 ctx := context.Background()

 s.RecordLatency(ctx, "ep1", 100*time.Millisecond)
 s.RecordLatency(ctx, "ep1", 200*time.Millisecond)

 avg, _ := s.GetAvgLatency(ctx, "ep1", time.Hour)
 if avg != 150*time.Millisecond {
  t.Errorf("expected 150ms, got %v", avg)
 }
}
```

- [ ] **Step 4: 运行测试**

Run: `go test ./pkg/store/... -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add pkg/store/
git commit -m "feat(store): 添加 StateStore 接口和 MemoryStateStore 实现"
```

---

### Task 4: RedisStateStore 实现

**Files:**

- Create: `pkg/store/redis.go`
- Create: `pkg/store/redis_test.go`
- Create: `pkg/store/lua/` (Lua 脚本)

- [ ] **Step 1: 创建 Redis StateStore**

```go
// pkg/store/redis.go
package store

import (
 "context"
 "fmt"
 "time"

 "github.com/redis/go-redis/v9"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// RedisStateStore Redis 实现
type RedisStateStore struct {
 client     redis.Cmdable
 keyPrefix  string
 luaScripts map[string]*redis.Script
}

// NewRedisStateStore 创建 Redis StateStore
func NewRedisStateStore(client redis.Cmdable, keyPrefix string) *RedisStateStore {
 s := &RedisStateStore{
  client:    client,
  keyPrefix: keyPrefix,
  luaScripts: map[string]*redis.Script{
   "rate_limit_incr":  redis.NewScript(rateLimitIncrLua),
   "rate_limit_refund": redis.NewScript(rateLimitRefundLua),
   "cb_record":        redis.NewScript(cbRecordLua),
  },
 }

 // 预加载脚本
 ctx := context.Background()
 for _, script := range s.luaScripts {
  script.Load(ctx, client)
 }

 return s
}

func (r *RedisStateStore) key(parts ...string) string {
 key := r.keyPrefix
 for _, p := range parts {
  key += ":" + p
 }
 return key
}

func (r *RedisStateStore) RateLimitIncr(ctx context.Context, key string, tokens int64, window time.Duration) (int64, error) {
 k := r.key("rl", key)
 result, err := r.luaScripts["rate_limit_incr"].Run(ctx, r.client, []string{k}, tokens, window.Milliseconds()).Result()
 if err != nil {
  return 0, err
 }
 return result.(int64), nil
}

func (r *RedisStateStore) RateLimitRefund(ctx context.Context, key string, tokens int64) error {
 k := r.key("rl", key)
 return r.luaScripts["rate_limit_refund"].Run(ctx, r.client, []string{k}, tokens).Err()
}

func (r *RedisStateStore) CircuitBreakerRecord(ctx context.Context, key string, success bool) error {
 k := r.key("cb", key)
 successInt := 0
 if success {
  successInt = 1
 }
 return r.luaScripts["cb_record"].Run(ctx, r.client, []string{k}, successInt, 60000).Err()
}

func (r *RedisStateStore) CircuitBreakerState(ctx context.Context, key string) (gateway.CircuitState, error) {
 k := r.key("cb", key)
 val, err := r.client.Get(ctx, k+":state").Int()
 if err == redis.Nil {
  return gateway.CircuitClosed, nil
 }
 if err != nil {
  return gateway.CircuitClosed, err
 }
 return gateway.CircuitState(val), nil
}

func (r *RedisStateStore) CircuitBreakerReset(ctx context.Context, key string) error {
 k := r.key("cb", key)
 return r.client.Del(ctx, k+":state", k+":failures", k+":total").Err()
}

func (r *RedisStateStore) StickyGet(ctx context.Context, sessionKey string) (string, error) {
 k := r.key("sticky", sessionKey)
 return r.client.Get(ctx, k).Result()
}

func (r *RedisStateStore) StickySet(ctx context.Context, sessionKey string, endpointID string, ttl time.Duration) error {
 k := r.key("sticky", sessionKey)
 return r.client.Set(ctx, k, endpointID, ttl).Err()
}

func (r *RedisStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
 k := r.key("latency", endpointID)
 now := time.Now().UnixMilli()
 return r.client.ZAdd(ctx, k, redis.Z{
  Score:  float64(now),
  Member: latency.Milliseconds(),
 }).Err()
}

func (r *RedisStateStore) GetAvgLatency(ctx context.Context, endpointID string, window time.Duration) (time.Duration, error) {
 k := r.key("latency", endpointID)
 cutoff := time.Now().Add(-window).UnixMilli()

 vals, err := r.client.ZRangeByScore(ctx, k, &redis.ZRangeBy{
  Min: fmt.Sprintf("%d", cutoff),
  Max: "+inf",
 }).Result()
 if err != nil {
  return 0, err
 }

 if len(vals) == 0 {
  return 0, nil
 }

 var sum int64
 for _, v := range vals {
  var ms int64
  fmt.Sscanf(v, "%d", &ms)
  sum += ms
 }

 return time.Duration(sum/int64(len(vals))) * time.Millisecond, nil
}

func (r *RedisStateStore) Close() error {
 if closer, ok := r.client.(interface{ Close() error }); ok {
  return closer.Close()
 }
 return nil
}

// ===== Lua 脚本 =====

const rateLimitIncrLua = `
local key = KEYS[1]
local tokens = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call('GET', key)
if current == false then
    redis.call('SET', key, tokens, 'PX', window)
    return tokens
end

current = tonumber(current) - tokens
redis.call('SET', key, current, 'KEEPTTL')
return current
`

const rateLimitRefundLua = `
local key = KEYS[1]
local tokens = tonumber(ARGV[1])
redis.call('INCRBY', key, tokens)
return 1
`

const cbRecordLua = `
local key = KEYS[1]
local success = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local stateKey = key .. ':state'
local failuresKey = key .. ':failures'
local totalKey = key .. ':total'

local state = redis.call('GET', stateKey)
if state == false then
    state = 0
end

redis.call('INCR', totalKey)
if success == 0 then
    redis.call('INCR', failuresKey)
end

return 1
`
```

- [ ] **Step 2: 编写 Redis 测试（使用 miniredis）**

```go
// pkg/store/redis_test.go
package store

import (
 "context"
 "testing"
 "time"

 "github.com/alicebob/miniredis/v2"
 "github.com/redis/go-redis/v9"
)

func setupRedis(t *testing.T) (*miniredis.Miniredis, *RedisStateStore) {
 mr, err := miniredis.Run()
 if err != nil {
  t.Fatal(err)
 }

 client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
 store := NewRedisStateStore(client, "test")

 return mr, store
}

func TestRedisStateStore_RateLimit(t *testing.T) {
 mr, store := setupRedis(t)
 defer mr.Close()

 ctx := context.Background()

 remaining, err := store.RateLimitIncr(ctx, "test", 10, time.Minute)
 if err != nil {
  t.Fatal(err)
 }
 if remaining != 10 {
  t.Errorf("expected 10, got %d", remaining)
 }

 remaining, _ = store.RateLimitIncr(ctx, "test", 3, time.Minute)
 if remaining != 7 {
  t.Errorf("expected 7, got %d", remaining)
 }
}

func TestRedisStateStore_Sticky(t *testing.T) {
 mr, store := setupRedis(t)
 defer mr.Close()

 ctx := context.Background()

 store.StickySet(ctx, "sess1", "ep1", time.Hour)

 epID, err := store.StickyGet(ctx, "sess1")
 if err != nil {
  t.Fatal(err)
 }
 if epID != "ep1" {
  t.Errorf("expected ep1, got %s", epID)
 }
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/store/... -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add pkg/store/
git commit -m "feat(store): 添加 RedisStateStore 实现和 Lua 脚本"
```

---

## Phase 3: Discovery 适配 + Router

### Task 5: Discovery 适配层

**Files:**

- Create: `pkg/gateway/discovery.go`
- Create: `pkg/gateway/discovery_test.go`

- [ ] **Step 1: 定义 Discovery 接口和适配器**

```go
// pkg/gateway/discovery.go
package gateway

import (
 "context"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/discovery"
)

// Discovery 按 model 提供可用 Endpoint 列表
type Discovery interface {
 List(ctx context.Context, model string) ([]*Endpoint, error)
 Watch(ctx context.Context, model string) (<-chan []*Endpoint, error)
 Close() error
}

// DiscoveryAdapter 将现有 pkg/discovery 适配为 gateway.Discovery
type DiscoveryAdapter struct {
 inner      discovery.ServiceDiscovery
 providers  map[string]*ProviderConfig // provider name -> config
}

// ProviderConfig Provider 配置（用于构建 Endpoint）
type ProviderConfig struct {
 Name         string
 Type         string
 Models       []string
 RequestTypes []RequestType
}

// NewDiscoveryAdapter 创建适配器
func NewDiscoveryAdapter(inner discovery.ServiceDiscovery, providers map[string]*ProviderConfig) *DiscoveryAdapter {
 return &DiscoveryAdapter{
  inner:     inner,
  providers: providers,
 }
}

func (d *DiscoveryAdapter) List(ctx context.Context, model string) ([]*Endpoint, error) {
 instances, err := d.inner.List(ctx)
 if err != nil {
  return nil, err
 }

 var endpoints []*Endpoint
 for _, inst := range instances {
  // 找到支持该 model 的 provider
  for _, prov := range d.providers {
   for _, m := range prov.Models {
    if m == model {
     ep := ServiceInstanceToEndpoint(inst, prov.Name, model, prov.RequestTypes)
     endpoints = append(endpoints, ep)
     break
    }
   }
  }
 }

 return endpoints, nil
}

func (d *DiscoveryAdapter) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
 // 简化实现：返回一个定期刷新的 channel
 ch := make(chan []*Endpoint, 1)
 go func() {
  for {
   select {
   case <-ctx.Done():
    close(ch)
    return
   default:
    eps, _ := d.List(ctx, model)
    ch <- eps
   }
  }
 }()
 return ch, nil
}

func (d *DiscoveryAdapter) Close() error {
 return d.inner.Close()
}

// ServiceInstanceToEndpoint 转换
func ServiceInstanceToEndpoint(si *discovery.ServiceInstance, providerName, model string, caps []RequestType) *Endpoint {
 return &Endpoint{
  ID:           si.ID,
  URL:          si.GetURL(),
  Provider:     providerName,
  Model:        model,
  Metadata:     si.Metadata,
  Weight:       si.Weight,
  RequestTypes: caps,
 }
}
```

- [ ] **Step 2: 编写测试**

```go
// pkg/gateway/discovery_test.go
package gateway

import (
 "context"
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/discovery"
)

// mockServiceDiscovery 模拟 ServiceDiscovery
type mockServiceDiscovery struct {
 instances []*discovery.ServiceInstance
}

func (m *mockServiceDiscovery) List(ctx context.Context) ([]*discovery.ServiceInstance, error) {
 return m.instances, nil
}

func (m *mockServiceDiscovery) Watch(ctx context.Context) (<-chan []*discovery.ServiceInstance, error) {
 ch := make(chan []*discovery.ServiceInstance, 1)
 ch <- m.instances
 return ch, nil
}

func (m *mockServiceDiscovery) Close() error {
 return nil
}

func TestDiscoveryAdapter_List(t *testing.T) {
 mock := &mockServiceDiscovery{
  instances: []*discovery.ServiceInstance{
   {ID: "ep1", Host: "localhost", Port: 8080},
   {ID: "ep2", Host: "localhost", Port: 8081},
  },
 }

 providers := map[string]*ProviderConfig{
  "openai": {
   Name:         "openai",
   Models:       []string{"gpt-4", "gpt-3.5-turbo"},
   RequestTypes: []RequestType{RequestTypeChatCompletion},
  },
 }

 adapter := NewDiscoveryAdapter(mock, providers)
 eps, err := adapter.List(context.Background(), "gpt-4")
 if err != nil {
  t.Fatal(err)
 }

 if len(eps) != 2 {
  t.Errorf("expected 2 endpoints, got %d", len(eps))
 }
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/gateway/... -v -run TestDiscovery`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add pkg/gateway/discovery.go pkg/gateway/discovery_test.go
git commit -m "feat(gateway): 添加 Discovery 接口和适配层"
```

---

### Task 6: Router 链实现

**Files:**

- Create: `pkg/gateway/router.go`
- Create: `pkg/gateway/routers/API.go`
- Create: `pkg/gateway/routers/tag.go`
- Create: `pkg/gateway/routers/circuit_breaker.go`
- Create: `pkg/gateway/routers/router_test.go`

- [ ] **Step 1: 定义 Router 接口**

```go
// pkg/gateway/router.go
package gateway

// Router Endpoint 列表的硬约束过滤器
type Router interface {
 Name() string
 Route(gctx *GatewayContext, endpoints []*Endpoint) []*Endpoint
}
```

- [ ] **Step 2: 实现 APIRouter**

```go
// pkg/gateway/routers/API.go
package routers

import "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"

// APIRouter 过滤不支持 RequestType 的 endpoint
type APIRouter struct{}

func (r *APIRouter) Name() string { return "API" }

func (r *APIRouter) Route(gctx *gateway.GatewayContext, endpoints []*gateway.Endpoint) []*gateway.Endpoint {
 var result []*gateway.Endpoint
 for _, ep := range endpoints {
  if ep.SupportsRequestType(gctx.RequestType) {
   result = append(result, ep)
  }
 }
 return result
}
```

- [ ] **Step 3: 实现 TagRouter**

```go
// pkg/gateway/routers/tag.go
package routers

import (
 "strings"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "go.uber.org/zap"
)

// TagRouter 标签过滤（zone/region/tenant）
type TagRouter struct {
 tags   map[string]string // 期望的标签
 logger *zap.Logger
}

func NewTagRouter(tags map[string]string, logger *zap.Logger) *TagRouter {
 return &TagRouter{tags: tags, logger: logger}
}

func (r *TagRouter) Name() string { return "tag" }

func (r *TagRouter) Route(gctx *gateway.GatewayContext, endpoints []*gateway.Endpoint) []*gateway.Endpoint {
 if len(r.tags) == 0 {
  return endpoints
 }

 var result []*gateway.Endpoint
 for _, ep := range endpoints {
  if r.matchTags(ep) {
   result = append(result, ep)
  }
 }

 // 全不匹配时放行兜底（防误杀）
 if len(result) == 0 {
  r.logger.Warn("tag router: no endpoints match tags, falling back to all",
   zap.Any("tags", r.tags))
  return endpoints
 }

 return result
}

func (r *TagRouter) matchTags(ep *gateway.Endpoint) bool {
 for k, v := range r.tags {
  epVal, ok := ep.Metadata[k]
  if !ok || !strings.EqualFold(epVal, v) {
   return false
  }
 }
 return true
}
```

- [ ] **Step 4: 实现 CircuitBreakerRouter**

```go
// pkg/gateway/routers/circuit_breaker.go
package routers

import (
 "context"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "go.uber.org/zap"
)

// CircuitBreakerRouter 过滤已熔断的 endpoint
type CircuitBreakerRouter struct {
 stateStore store.StateStore
 logger     *zap.Logger
}

func NewCircuitBreakerRouter(stateStore store.StateStore, logger *zap.Logger) *CircuitBreakerRouter {
 return &CircuitBreakerRouter{stateStore: stateStore, logger: logger}
}

func (r *CircuitBreakerRouter) Name() string { return "circuit_breaker" }

func (r *CircuitBreakerRouter) Route(gctx *gateway.GatewayContext, endpoints []*gateway.Endpoint) []*gateway.Endpoint {
 ctx := context.Background()
 var result []*gateway.Endpoint

 for _, ep := range endpoints {
  // 检查 service-level 熔断
  serviceKey := ep.Provider + ":" + ep.Model
  serviceState, _ := r.stateStore.CircuitBreakerState(ctx, serviceKey)
  if serviceState == gateway.CircuitOpen {
   r.logger.Debug("circuit breaker: service open, skipping",
    zap.String("key", serviceKey))
   continue
  }

  // 检查 instance-level 熔断
  instanceState, _ := r.stateStore.CircuitBreakerState(ctx, ep.ID)
  if instanceState == gateway.CircuitOpen {
   r.logger.Debug("circuit breaker: instance open, skipping",
    zap.String("endpoint", ep.ID))
   continue
  }

  result = append(result, ep)
 }

 return result
}
```

- [ ] **Step 5: 编写 Router 测试**

```go
// pkg/gateway/routers/router_test.go
package routers

import (
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "go.uber.org/zap"
)

func TestAPIRouter(t *testing.T) {
 r := &APIRouter{}
 gctx := &gateway.GatewayContext{RequestType: gateway.RequestTypeChatCompletion}

 eps := []*gateway.Endpoint{
  {ID: "ep1", RequestTypes: []gateway.RequestType{gateway.RequestTypeChatCompletion}},
  {ID: "ep2", RequestTypes: []gateway.RequestType{gateway.RequestTypeEmbedding}},
  {ID: "ep3", RequestTypes: []gateway.RequestType{gateway.RequestTypeChatCompletion, gateway.RequestTypeEmbedding}},
 }

 result := r.Route(gctx, eps)
 if len(result) != 2 {
  t.Errorf("expected 2, got %d", len(result))
 }
}

func TestTagRouter(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 r := NewTagRouter(map[string]string{"zone": "us-east"}, logger)
 gctx := &gateway.GatewayContext{}

 eps := []*gateway.Endpoint{
  {ID: "ep1", Metadata: map[string]string{"zone": "us-east"}},
  {ID: "ep2", Metadata: map[string]string{"zone": "us-west"}},
 }

 result := r.Route(gctx, eps)
 if len(result) != 1 {
  t.Errorf("expected 1, got %d", len(result))
 }
}

func TestTagRouter_Fallback(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 r := NewTagRouter(map[string]string{"zone": "eu-west"}, logger)
 gctx := &gateway.GatewayContext{}

 eps := []*gateway.Endpoint{
  {ID: "ep1", Metadata: map[string]string{"zone": "us-east"}},
  {ID: "ep2", Metadata: map[string]string{"zone": "us-west"}},
 }

 // 全不匹配时放行全部
 result := r.Route(gctx, eps)
 if len(result) != 2 {
  t.Errorf("expected fallback to all 2, got %d", len(result))
 }
}

func TestCircuitBreakerRouter(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 ss := store.NewMemoryStateStore()
 r := NewCircuitBreakerRouter(ss, logger)
 gctx := &gateway.GatewayContext{}

 eps := []*gateway.Endpoint{
  {ID: "ep1", Provider: "openai", Model: "gpt-4"},
  {ID: "ep2", Provider: "openai", Model: "gpt-4"},
 }

 result := r.Route(gctx, eps)
 if len(result) != 2 {
  t.Errorf("expected 2, got %d", len(result))
 }
}
```

- [ ] **Step 6: 运行测试**

Run: `go test ./pkg/gateway/routers/... -v`
Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add pkg/gateway/router.go pkg/gateway/routers/
git commit -m "feat(gateway): 添加 Router 接口和三种实现（API/Tag/CircuitBreaker）"
```

---

## Phase 4: LoadBalancer 适配

### Task 7: LoadBalancer 接口适配

**Files:**

- Create: `pkg/gateway/lb.go`
- Create: `pkg/gateway/lbs/round_robin.go`
- Create: `pkg/gateway/lbs/sticky.go`
- Create: `pkg/gateway/lbs/cost.go`
- Create: `pkg/gateway/lbs/composite.go`
- Create: `pkg/gateway/lbs/lb_test.go`

- [ ] **Step 1: 定义 LoadBalancer 接口**

```go
// pkg/gateway/lb.go
package gateway

// LoadBalancer 从候选列表中选一个 ProviderInvoker
type LoadBalancer interface {
 Select(gctx *GatewayContext, endpoints []*Endpoint) *ProviderInvoker
}
```

- [ ] **Step 2: 实现 RoundRobin**

```go
// pkg/gateway/lbs/round_robin.go
package lbs

import (
 "sync/atomic"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// RoundRobin 轮询
type RoundRobin struct {
 counter uint64
}

func NewRoundRobin() *RoundRobin {
 return &RoundRobin{}
}

func (lb *RoundRobin) Select(gctx *gateway.GatewayContext, endpoints []*gateway.Endpoint) *gateway.ProviderInvoker {
 if len(endpoints) == 0 {
  return nil
 }

 idx := atomic.AddUint64(&lb.counter, 1)
 ep := endpoints[idx%uint64(len(endpoints))]

 return &gateway.ProviderInvoker{Endpoint: ep}
}
```

- [ ] **Step 3: 实现 Sticky**

```go
// pkg/gateway/lbs/sticky.go
package lbs

import (
 "context"
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
)

// StickyLoadBalancer Session 粘性 LB
type StickyLoadBalancer struct {
 stateStore store.StateStore
 fallback   gateway.LoadBalancer
 keyFunc    func(gctx *gateway.GatewayContext) string
 ttl        time.Duration
}

func NewStickyLoadBalancer(ss store.StateStore, fallback gateway.LoadBalancer, keyFunc func(*gateway.GatewayContext) string, ttl time.Duration) *StickyLoadBalancer {
 return &StickyLoadBalancer{
  stateStore: ss,
  fallback:   fallback,
  keyFunc:    keyFunc,
  ttl:        ttl,
 }
}

func (lb *StickyLoadBalancer) Select(gctx *gateway.GatewayContext, endpoints []*gateway.Endpoint) *gateway.ProviderInvoker {
 key := lb.keyFunc(gctx)
 if key != "" {
  epID, _ := lb.stateStore.StickyGet(context.Background(), key)
  if epID != "" {
   for _, ep := range endpoints {
    if ep.ID == epID {
     return &gateway.ProviderInvoker{Endpoint: ep}
    }
   }
  }
 }

 // miss 时落到 fallback LB
 return lb.fallback.Select(gctx, endpoints)
}
```

- [ ] **Step 4: 实现 Cost**

```go
// pkg/gateway/lbs/cost.go
package lbs

import (
 "math"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// CostLoadBalancer 最低成本
type CostLoadBalancer struct{}

func NewCostLoadBalancer() *CostLoadBalancer {
 return &CostLoadBalancer{}
}

func (lb *CostLoadBalancer) Select(gctx *gateway.GatewayContext, endpoints []*gateway.Endpoint) *gateway.ProviderInvoker {
 if len(endpoints) == 0 {
  return nil
 }

 var best *gateway.Endpoint
 bestCost := math.MaxFloat64

 for _, ep := range endpoints {
  cost := ep.CostPerToken()
  if cost < bestCost {
   bestCost = cost
   best = ep
  }
 }

 return &gateway.ProviderInvoker{Endpoint: best}
}
```

- [ ] **Step 5: 实现 Composite**

```go
// pkg/gateway/lbs/composite.go
package lbs

import (
 "math"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "context"
 "time"
)

// CompositeLoadBalancer 多维归一化加权
type CompositeLoadBalancer struct {
 stateStore   store.StateStore
 costWeight   float64
 latencyWeight float64
}

func NewCompositeLoadBalancer(ss store.StateStore, costWeight, latencyWeight float64) *CompositeLoadBalancer {
 return &CompositeLoadBalancer{
  stateStore:    ss,
  costWeight:    costWeight,
  latencyWeight: latencyWeight,
 }
}

func (lb *CompositeLoadBalancer) Select(gctx *gateway.GatewayContext, endpoints []*gateway.Endpoint) *gateway.ProviderInvoker {
 if len(endpoints) == 0 {
  return nil
 }

 type scored struct {
  ep    *gateway.Endpoint
  score float64
 }

 scores := make([]scored, len(endpoints))
 maxCost := 0.0
 maxLatency := time.Duration(0)

 // 收集数据
 for i, ep := range endpoints {
  cost := ep.CostPerToken()
  if cost > maxCost {
   maxCost = cost
  }

  latency, _ := lb.stateStore.GetAvgLatency(context.Background(), ep.ID, 5*time.Minute)
  if latency > maxLatency {
   maxLatency = latency
  }

  scores[i] = scored{ep: ep}
 }

 // 归一化评分（越低越好）
 for i, s := range scores {
  costScore := 0.0
  if maxCost > 0 {
   costScore = s.ep.CostPerToken() / maxCost
  }

  latencyScore := 0.0
  if maxLatency > 0 {
   latency, _ := lb.stateStore.GetAvgLatency(context.Background(), s.ep.ID, 5*time.Minute)
   latencyScore = float64(latency) / float64(maxLatency)
  }

  scores[i].score = costScore*lb.costWeight + latencyScore*lb.latencyWeight
 }

 // 选最低分
 best := scores[0]
 for _, s := range scores[1:] {
  if s.score < best.score {
   best = s
  }
 }

 return &gateway.ProviderInvoker{Endpoint: best.ep}
}
```

- [ ] **Step 6: 编写测试**

```go
// pkg/gateway/lbs/lb_test.go
package lbs

import (
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
)

func TestRoundRobin(t *testing.T) {
 lb := NewRoundRobin()
 gctx := &gateway.GatewayContext{}

 eps := []*gateway.Endpoint{
  {ID: "ep1"},
  {ID: "ep2"},
  {ID: "ep3"},
 }

 // 应该轮询
 seen := make(map[string]int)
 for i := 0; i < 6; i++ {
  invoker := lb.Select(gctx, eps)
  seen[invoker.Endpoint.ID]++
 }

 if seen["ep1"] != 2 || seen["ep2"] != 2 || seen["ep3"] != 2 {
  t.Errorf("expected even distribution, got %v", seen)
 }
}

func TestCostLoadBalancer(t *testing.T) {
 lb := NewCostLoadBalancer()
 gctx := &gateway.GatewayContext{}

 eps := []*gateway.Endpoint{
  {ID: "ep1", Metadata: map[string]string{"cost_per_token": "0.00003"}},
  {ID: "ep2", Metadata: map[string]string{"cost_per_token": "0.00001"}},
  {ID: "ep3", Metadata: map[string]string{"cost_per_token": "0.00002"}},
 }

 invoker := lb.Select(gctx, eps)
 if invoker.Endpoint.ID != "ep2" {
  t.Errorf("expected ep2 (cheapest), got %s", invoker.Endpoint.ID)
 }
}

func TestStickyLoadBalancer(t *testing.T) {
 ss := store.NewMemoryStateStore()
 fallback := NewRoundRobin()
 lb := NewStickyLoadBalancer(ss, fallback, func(gctx *gateway.GatewayContext) string {
  return gctx.SessionID
 }, 3600)

 gctx := &gateway.GatewayContext{SessionID: "sess1"}
 eps := []*gateway.Endpoint{
  {ID: "ep1"},
  {ID: "ep2"},
 }

 // 首次请求，fallback 到 round-robin
 invoker := lb.Select(gctx, eps)
 firstEndpoint := invoker.Endpoint.ID

 // 设置 sticky
 ss.StickySet(nil, "sess1", firstEndpoint, 3600)

 // 后续请求应该 sticky
 invoker = lb.Select(gctx, eps)
 if invoker.Endpoint.ID != firstEndpoint {
  t.Errorf("expected sticky to %s, got %s", firstEndpoint, invoker.Endpoint.ID)
 }
}
```

- [ ] **Step 7: 运行测试**

Run: `go test ./pkg/gateway/lbs/... -v`
Expected: PASS

- [ ] **Step 8: 提交**

```bash
git add pkg/gateway/lb.go pkg/gateway/lbs/
git commit -m "feat(gateway): 添加 LoadBalancer 接口和实现（RR/Sticky/Cost/Composite）"
```

---

## Phase 5: Invoker 层级

### Task 8: ProviderInvoker

**Files:**

- Create: `pkg/gateway/provider_invoker.go`
- Create: `pkg/gateway/provider_invoker_test.go`

- [ ] **Step 1: 定义 Provider 接口和 ProviderInvoker**

```go
// pkg/gateway/provider_invoker.go
package gateway

import (
 "context"
)

// ProviderType Provider 类型
type ProviderType string

const (
 ProviderOpenAI    ProviderType = "openai"
 ProviderAnthropic ProviderType = "anthropic"
)

// Provider 协议适配层（API-based）
type Provider interface {
 Name() string
 Type() ProviderType
 RequestTypes() []RequestType
 Invoke(gctx *GatewayContext) error
 HealthCheck(ctx context.Context) error
 ValidateConfig() error
}

// ProviderInvoker 叶子节点：封装一个 Provider + Endpoint
type ProviderInvoker struct {
 Provider Provider
 Endpoint *Endpoint
}

// Invoke 执行上游调用
func (pi *ProviderInvoker) Invoke(gctx *GatewayContext) error {
 gctx.SelectedInvoker = pi
 gctx.SelectedEndpoint = pi.Endpoint
 gctx.UpstreamConnect = time.Now()

 return pi.Provider.Invoke(gctx)
}
```

- [ ] **Step 2: 编写测试**

```go
// pkg/gateway/provider_invoker_test.go
package gateway

import (
 "context"
 "testing"
 "time"
)

// mockProvider 模拟 Provider
type mockProvider struct {
 name    string
 pType   ProviderType
 caps    []RequestType
.invokeFn func(gctx *GatewayContext) error
}

func (m *mockProvider) Name() string                    { return m.name }
func (m *mockProvider) Type() ProviderType              { return mType }
func (m *mockProvider) RequestTypes() []RequestType     { return m.caps }
func (m *mockProvider) Invoke(gctx *GatewayContext) error { return m.invokeFn(gctx) }
func (m *mockProvider) HealthCheck(ctx context.Context) error { return nil }
func (m *mockProvider) ValidateConfig() error            { return nil }

func TestProviderInvoker_Invoke(t *testing.T) {
 provider := &mockProvider{
  name:  "openai",
  pType: ProviderOpenAI,
  caps:  []RequestType{RequestTypeChatCompletion},
  invokeFn: func(gctx *GatewayContext) error {
   gctx.Response = map[string]string{"status": "ok"}
   return nil
  },
 }

 ep := &Endpoint{
  ID:       "ep1",
  URL:      "http://localhost:8080",
  Provider: "openai",
  Model:    "gpt-4",
 }

 invoker := &ProviderInvoker{Provider: provider, Endpoint: ep}
 gctx := &GatewayContext{
  Ctx:           context.Background(),
  RequestType:   RequestTypeChatCompletion,
  ResponseWriter: httptest.NewRecorder(),
 }

 err := invoker.Invoke(gctx)
 if err != nil {
  t.Fatal(err)
 }

 if gctx.SelectedEndpoint != ep {
  t.Error("expected SelectedEndpoint to be set")
 }
 if gctx.UpstreamConnect.IsZero() {
  t.Error("expected UpstreamConnect to be set")
 }
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/gateway/... -v -run TestProviderInvoker`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add pkg/gateway/provider_invoker.go pkg/gateway/provider_invoker_test.go
git commit -m "feat(gateway): 添加 Provider 接口和 ProviderInvoker"
```

---

### Task 9: ClusterInvoker

**Files:**

- Create: `pkg/gateway/cluster_invoker.go`
- Create: `pkg/gateway/cluster_invoker_test.go`

- [ ] **Step 1: 定义 RetryStrategy 和 ClusterInvoker**

```go
// pkg/gateway/cluster_invoker.go
package gateway

import (
 "context"
 "math"
 "math/rand"
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "go.uber.org/zap"
)

// RetryStrategy 重试策略
type RetryStrategy struct {
 MaxRetries int
 Backoff    BackoffConfig
 ErrorRules []RetryRule
}

// BackoffConfig 退避配置
type BackoffConfig struct {
 Type    string // "exponential_jitter"
 BaseMs  int
 MaxMs   int
}

// ShouldRetry 判断是否应该重试
func (rs *RetryStrategy) ShouldRetry(err error, statusCode int) bool {
 for _, rule := range rs.ErrorRules {
  if rule.Matcher.Match(statusCode, "", err.Error()) {
   return rule.Retry
  }
 }
 return false
}

// CalcBackoff 计算退避时间
func (rs *RetryStrategy) CalcBackoff(attempt int) time.Duration {
 base := float64(rs.Backoff.BaseMs)
 max := float64(rs.Backoff.MaxMs)

 // exponential
 delay := base * math.Pow(2, float64(attempt))
 if delay > max {
  delay = max
 }

 // jitter
 jitter := delay * 0.2 * (rand.Float64()*2 - 1)
 delay += jitter

 return time.Duration(delay) * time.Millisecond
}

// ClusterInvoker 编排器：Discovery + Router + LB + retry
type ClusterInvoker struct {
 discovery     Discovery
 routerChain   []Router
 loadBalancer  LoadBalancer
 retryStrategy *RetryStrategy
 cbManager     *CircuitBreakerManager
 stateStore    store.StateStore
 logger        *zap.Logger
}

// NewClusterInvoker 创建 ClusterInvoker
func NewClusterInvoker(
 discovery Discovery,
 routers []Router,
 lb LoadBalancer,
 retry *RetryStrategy,
 cbManager *CircuitBreakerManager,
 stateStore store.StateStore,
 logger *zap.Logger,
) *ClusterInvoker {
 return &ClusterInvoker{
  discovery:     discovery,
  routerChain:   routers,
  loadBalancer:  lb,
  retryStrategy: retry,
  cbManager:     cbManager,
  stateStore:    stateStore,
  logger:        logger,
 }
}

// Invoke 执行集群调用（带重试）
func (ci *ClusterInvoker) Invoke(gctx *GatewayContext) error {
 excluded := make(map[string]bool)
 var lastErr error

 for attempt := 0; attempt <= ci.retryStrategy.MaxRetries; attempt++ {
  if attempt > 0 {
   // 退避
   backoff := ci.retryStrategy.CalcBackoff(attempt - 1)
   time.Sleep(backoff)
  }

  gctx.ResetAttempt()

  // Discovery
  endpoints, err := ci.discovery.List(gctx.Ctx, gctx.Model)
  if err != nil {
   ci.logger.Error("discovery failed", zap.Error(err))
   lastErr = err
   continue
  }

  if len(endpoints) == 0 {
   lastErr = ErrNoAvailableEndpoint
   continue
  }

  // Router chain
  for _, router := range ci.routerChain {
   endpoints = router.Route(gctx, endpoints)
  }

  if len(endpoints) == 0 {
   lastErr = ErrNoAvailableEndpoint
   continue
  }

  // 过滤已排除的
  var filtered []*Endpoint
  for _, ep := range endpoints {
   if !excluded[ep.ID] {
    filtered = append(filtered, ep)
   }
  }

  if len(filtered) == 0 {
   lastErr = ErrNoAvailableEndpoint
   continue
  }

  // LoadBalancer 选择
  invoker := ci.loadBalancer.Select(gctx, filtered)
  if invoker == nil {
   lastErr = ErrNoAvailableEndpoint
   continue
  }

  // 执行调用
  err = invoker.Invoke(gctx)
  gctx.RecordAttempt(err == nil)

  if err == nil {
   // 成功
   ci.cbManager.RecordSuccess(gctx.SelectedEndpoint)
   ci.stateStore.RecordLatency(gctx.Ctx, gctx.SelectedEndpoint.ID, time.Since(gctx.UpstreamConnect))
   return nil
  }

  lastErr = err

  // 流式已发首字节，不能重试
  if gctx.TTFT > 0 {
   return err
  }

  // 检查是否应该重试
  if !ci.retryStrategy.ShouldRetry(err, getStatusCode(gctx.UpstreamResponse)) {
   return err
  }

  // 记录熔断失败
  ci.cbManager.RecordFailure(gctx.SelectedEndpoint, err)

  // 排除该 endpoint
  excluded[gctx.SelectedEndpoint.ID] = true
 }

 return lastErr
}

// 错误定义
var (
 ErrNoAvailableEndpoint = fmt.Errorf("no available endpoint")
)
```

- [ ] **Step 2: 实现 CircuitBreakerManager**

```go
// pkg/gateway/circuit_breaker.go
package gateway

import (
 "context"
 "sync"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
)

// CircuitBreakerManager 管理双层熔断器
type CircuitBreakerManager struct {
 stateStore store.StateStore
 mu         sync.RWMutex
}

// NewCircuitBreakerManager 创建熔断器管理器
func NewCircuitBreakerManager(stateStore store.StateStore) *CircuitBreakerManager {
 return &CircuitBreakerManager{stateStore: stateStore}
}

// RecordSuccess 记录成功
func (cbm *CircuitBreakerManager) RecordSuccess(ep *Endpoint) {
 ctx := context.Background()
 cbm.stateStore.CircuitBreakerRecord(ctx, ep.Provider+":"+ep.Model, true)
 cbm.stateStore.CircuitBreakerRecord(ctx, ep.ID, true)
}

// RecordFailure 记录失败
func (cbm *CircuitBreakerManager) RecordFailure(ep *Endpoint, err error) {
 ctx := context.Background()
 cbm.stateStore.CircuitBreakerRecord(ctx, ep.Provider+":"+ep.Model, false)
 cbm.stateStore.CircuitBreakerRecord(ctx, ep.ID, false)
}

// IsServiceOpen 检查 service-level 熔断
func (cbm *CircuitBreakerManager) IsServiceOpen(key string) bool {
 ctx := context.Background()
 state, _ := cbm.stateStore.CircuitBreakerState(ctx, key)
 return state == CircuitOpen
}

// IsInstanceOpen 检查 instance-level 熔断
func (cbm *CircuitBreakerManager) IsInstanceOpen(endpointID string) bool {
 ctx := context.Background()
 state, _ := cbm.stateStore.CircuitBreakerState(ctx, endpointID)
 return state == CircuitOpen
}
```

- [ ] **Step 3: 编写 ClusterInvoker 测试**

```go
// pkg/gateway/cluster_invoker_test.go
package gateway

import (
 "context"
 "errors"
 "net/http/httptest"
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "go.uber.org/zap"
)

func TestClusterInvoker_Success(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 ss := store.NewMemoryStateStore()

 // Mock discovery
 discovery := &mockDiscovery{
  endpoints: []*Endpoint{
   {ID: "ep1", Provider: "openai", Model: "gpt-4", URL: "http://localhost:8080"},
  },
 }

 // Mock provider
 provider := &mockProvider{
  name:  "openai",
  pType: ProviderOpenAI,
  caps:  []RequestType{RequestTypeChatCompletion},
  invokeFn: func(gctx *GatewayContext) error {
   return nil
  },
 }

 // 创建 invoker
 ci := NewClusterInvoker(
  discovery,
  []Router{}, // 空 router
  &mockLoadBalancer{provider: provider},
  &RetryStrategy{MaxRetries: 1, Backoff: BackoffConfig{BaseMs: 10, MaxMs: 100}},
  NewCircuitBreakerManager(ss),
  ss,
  logger,
 )

 gctx := &GatewayContext{
  Ctx:            context.Background(),
  RequestType:    RequestTypeChatCompletion,
  Model:          "gpt-4",
  ResponseWriter: httptest.NewRecorder(),
 }

 err := ci.Invoke(gctx)
 if err != nil {
  t.Fatal(err)
 }

 if gctx.AttemptCount != 1 {
  t.Errorf("expected 1 attempt, got %d", gctx.AttemptCount)
 }
}

func TestClusterInvoker_Retry(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 ss := store.NewMemoryStateStore()

 attempt := 0
 provider := &mockProvider{
  name:  "openai",
  pType: ProviderOpenAI,
  caps:  []RequestType{RequestTypeChatCompletion},
  invokeFn: func(gctx *GatewayContext) error {
   attempt++
   if attempt == 1 {
    return errors.New("500 internal server error")
   }
   return nil
  },
 }

 discovery := &mockDiscovery{
  endpoints: []*Endpoint{
   {ID: "ep1", Provider: "openai", Model: "gpt-4"},
   {ID: "ep2", Provider: "openai", Model: "gpt-4"},
  },
 }

 ci := NewClusterInvoker(
  discovery,
  []Router{},
  &mockLoadBalancer{provider: provider},
  &RetryStrategy{
   MaxRetries: 2,
   Backoff:    BackoffConfig{BaseMs: 10, MaxMs: 100},
   ErrorRules: []RetryRule{
    {Matcher: ErrorMatcher{StatusCodes: []int{500}}, Retry: true},
   },
  },
  NewCircuitBreakerManager(ss),
  ss,
  logger,
 )

 gctx := &GatewayContext{
  Ctx:            context.Background(),
  RequestType:    RequestTypeChatCompletion,
  Model:          "gpt-4",
  ResponseWriter: httptest.NewRecorder(),
  UpstreamResponse: &http.Response{StatusCode: 500},
 }

 err := ci.Invoke(gctx)
 if err != nil {
  t.Fatal(err)
 }

 if gctx.AttemptCount != 2 {
  t.Errorf("expected 2 attempts, got %d", gctx.AttemptCount)
 }
}

// Mock 类型
type mockDiscovery struct {
 endpoints []*Endpoint
}

func (m *mockDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
 return m.endpoints, nil
}

func (m *mockDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
 ch := make(chan []*Endpoint, 1)
 ch <- m.endpoints
 return ch, nil
}

func (m *mockDiscovery) Close() error { return nil }

type mockLoadBalancer struct {
 provider *mockProvider
}

func (m *mockLoadBalancer) Select(gctx *GatewayContext, endpoints []*Endpoint) *ProviderInvoker {
 if len(endpoints) == 0 {
  return nil
 }
 return &ProviderInvoker{Provider: m.provider, Endpoint: endpoints[0]}
}
```

- [ ] **Step 4: 运行测试**

Run: `go test ./pkg/gateway/... -v -run TestClusterInvoker`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add pkg/gateway/cluster_invoker.go pkg/gateway/circuit_breaker.go pkg/gateway/cluster_invoker_test.go
git commit -m "feat(gateway): 添加 ClusterInvoker（编排 Discovery + Router + LB + retry）"
```

---

### Task 10: FallbackInvoker

**Files:**

- Create: `pkg/gateway/fallback_invoker.go`
- Create: `pkg/gateway/fallback_invoker_test.go`

- [ ] **Step 1: 实现 FallbackInvoker**

```go
// pkg/gateway/fallback_invoker.go
package gateway

import "fmt"

// FallbackEntry 降级条目
type FallbackEntry struct {
 Model          string
 ClusterInvoker *ClusterInvoker
}

// FallbackInvoker 模型降级编排器
type FallbackInvoker struct {
 chain      []FallbackEntry
 errorRules []FallbackRule
}

// NewFallbackInvoker 创建 FallbackInvoker
func NewFallbackInvoker(chain []FallbackEntry, errorRules []FallbackRule) *FallbackInvoker {
 return &FallbackInvoker{
  chain:      chain,
  errorRules: errorRules,
 }
}

// Invoke 执行降级调用
func (fi *FallbackInvoker) Invoke(gctx *GatewayContext) error {
 for i, entry := range fi.chain {
  if i > 0 {
   // 降级：重写 Model
   gctx.Model = entry.Model
   gctx.FallbackChain = append(gctx.FallbackChain, entry.Model)
  }

  err := entry.ClusterInvoker.Invoke(gctx)
  if err == nil {
   return nil
  }

  // 流式已发首字节，不能降级
  if gctx.TTFT > 0 {
   return err
  }

  // 检查是否应该降级
  if !fi.shouldFallback(err) {
   return err
  }
 }

 return ErrAllFallbackExhausted
}

// shouldFallback 判断是否应该降级
func (fi *FallbackInvoker) shouldFallback(err error) bool {
 if len(fi.errorRules) == 0 {
  return true // 默认降级
 }

 for _, rule := range fi.errorRules {
  if rule.Matcher.Match(0, "", err.Error()) {
   return rule.Fallback
  }
 }

 return false
}

// BuildInvoker 构建 Invoker（无降级时退化为单 ClusterInvoker）
func BuildInvoker(fallbacks []FallbackEntry, errorRules []FallbackRule) Invoker {
 if len(fallbacks) <= 1 {
  return fallbacks[0].ClusterInvoker
 }
 return NewFallbackInvoker(fallbacks, errorRules)
}

var ErrAllFallbackExhausted = fmt.Errorf("all fallback models exhausted")
```

- [ ] **Step 2: 编写测试**

```go
// pkg/gateway/fallback_invoker_test.go
package gateway

import (
 "context"
 "errors"
 "net/http/httptest"
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "go.uber.org/zap"
)

func TestFallbackInvoker_SimplePassthrough(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 ss := store.NewMemoryStateStore()

 provider := &mockProvider{
  name:  "openai",
  pType: ProviderOpenAI,
  caps:  []RequestType{RequestTypeChatCompletion},
  invokeFn: func(gctx *GatewayContext) error {
   return nil
  },
 }

 ci := NewClusterInvoker(
  &mockDiscovery{endpoints: []*Endpoint{{ID: "ep1"}}},
  []Router{},
  &mockLoadBalancer{provider: provider},
  &RetryStrategy{MaxRetries: 0},
  NewCircuitBreakerManager(ss),
  ss,
  logger,
 )

 // 单模型，退化为透传
 fi := BuildInvoker([]FallbackEntry{{Model: "gpt-4", ClusterInvoker: ci}}, nil)

 gctx := &GatewayContext{
  Ctx:            context.Background(),
  Model:          "gpt-4",
  ResponseWriter: httptest.NewRecorder(),
 }

 err := fi.Invoke(gctx)
 if err != nil {
  t.Fatal(err)
 }
}

func TestFallbackInvoker_Degradation(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 ss := store.NewMemoryStateStore()

 attempt := 0
 provider1 := &mockProvider{
  name:  "openai",
  pType: ProviderOpenAI,
  caps:  []RequestType{RequestTypeChatCompletion},
  invokeFn: func(gctx *GatewayContext) error {
   attempt++
   return errors.New("500 error")
  },
 }

 provider2 := &mockProvider{
  name:  "anthropic",
  pType: ProviderAnthropic,
  caps:  []RequestType{RequestTypeChatCompletion},
  invokeFn: func(gctx *GatewayContext) error {
   return nil
  },
 }

 ci1 := NewClusterInvoker(
  &mockDiscovery{endpoints: []*Endpoint{{ID: "ep1"}}},
  []Router{},
  &mockLoadBalancer{provider: provider1},
  &RetryStrategy{MaxRetries: 0},
  NewCircuitBreakerManager(ss),
  ss,
  logger,
 )

 ci2 := NewClusterInvoker(
  &mockDiscovery{endpoints: []*Endpoint{{ID: "ep2"}}},
  []Router{},
  &mockLoadBalancer{provider: provider2},
  &RetryStrategy{MaxRetries: 0},
  NewCircuitBreakerManager(ss),
  ss,
  logger,
 )

 fi := NewFallbackInvoker(
  []FallbackEntry{
   {Model: "gpt-4", ClusterInvoker: ci1},
   {Model: "claude-sonnet-4-20250514", ClusterInvoker: ci2},
  },
  []FallbackRule{
   {Matcher: ErrorMatcher{StatusCodes: []int{500}}, Fallback: true},
  },
 )

 gctx := &GatewayContext{
  Ctx:            context.Background(),
  Model:          "gpt-4",
  ResponseWriter: httptest.NewRecorder(),
 }

 err := fi.Invoke(gctx)
 if err != nil {
  t.Fatal(err)
 }

 if gctx.Model != "claude-sonnet-4-20250514" {
  t.Errorf("expected model to be claude-sonnet-4-20250514, got %s", gctx.Model)
 }
 if len(gctx.FallbackChain) != 1 {
  t.Errorf("expected 1 fallback, got %d", len(gctx.FallbackChain))
 }
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/gateway/... -v -run TestFallback`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add pkg/gateway/fallback_invoker.go pkg/gateway/fallback_invoker_test.go
git commit -m "feat(gateway): 添加 FallbackInvoker（模型降级编排）"
```

---

## Phase 6: Filter 体系

### Task 11: InboundFilter 接口与实现

**Files:**

- Create: `pkg/gateway/filter.go`
- Create: `pkg/gateway/filters/auth.go`
- Create: `pkg/gateway/filters/rate_limit.go`
- Create: `pkg/gateway/filters/validate.go`
- Create: `pkg/gateway/filters/filter_test.go`

- [ ] **Step 1: 定义 Filter 接口**

```go
// pkg/gateway/filter.go
package gateway

// FilterCriticality Filter 关键性
type FilterCriticality int

const (
 BestEffort FilterCriticality = iota
 Critical
)

// InboundFilter 请求进入 ClusterInvoker 前执行
type InboundFilter interface {
 Name() string
 Order() int
 OnRequest(gctx *GatewayContext) error
}

// OutboundFilter 响应离开后执行
type OutboundFilter interface {
 Name() string
 Order() int
 Criticality() FilterCriticality
 OnResponse(gctx *GatewayContext) error
}
```

- [ ] **Step 2: 实现 AuthFilter**

```go
// pkg/gateway/filters/auth.go
package filters

import (
 "net/http"
 "strings"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// AuthFilter API Key 验证
type AuthFilter struct {
 validKeys map[string]string // apikey -> userID
}

func NewAuthFilter(validKeys map[string]string) *AuthFilter {
 return &AuthFilter{validKeys: validKeys}
}

func (f *AuthFilter) Name() string  { return "auth" }
func (f *AuthFilter) Order() int    { return 10 }
func (f *AuthFilter) OnRequest(gctx *gateway.GatewayContext) error {
 apiKey := extractAPIKey(gctx.Request)
 if apiKey == "" {
  return &HTTPError{Code: http.StatusUnauthorized, Message: "missing API key"}
 }

 userID, ok := f.validKeys[apiKey]
 if !ok {
  return &HTTPError{Code: http.StatusUnauthorized, Message: "invalid API key"}
 }

 gctx.APIKey = apiKey
 gctx.UserID = userID
 return nil
}

func extractAPIKey(r *http.Request) string {
 // Authorization: Bearer sk-xxx
 auth := r.Header.Get("Authorization")
 if strings.HasPrefix(auth, "Bearer ") {
  return strings.TrimPrefix(auth, "Bearer ")
 }
 // X-API-Key header
 return r.Header.Get("X-API-Key")
}

// HTTPError HTTP 错误
type HTTPError struct {
 Code    int
 Message string
}

func (e *HTTPError) Error() string {
 return e.Message
}
```

- [ ] **Step 3: 实现 RateLimitFilter**

```go
// pkg/gateway/filters/rate_limit.go
package filters

import (
 "context"
 "net/http"
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
)

// RateLimitFilter 限流（投机预扣）
type RateLimitFilter struct {
 stateStore store.StateStore
 matcher    *gateway.PolicyMatcher
}

func NewRateLimitFilter(ss store.StateStore, matcher *gateway.PolicyMatcher) *RateLimitFilter {
 return &RateLimitFilter{stateStore: ss, matcher: matcher}
}

func (f *RateLimitFilter) Name() string  { return "rate_limit" }
func (f *RateLimitFilter) Order() int    { return 20 }
func (f *RateLimitFilter) OnRequest(gctx *gateway.GatewayContext) error {
 policy := f.matcher.Match(gctx)
 if policy == nil {
  return nil // 无限流策略
 }

 gctx.Policy = policy

 // 估算 token
 estimate := estimatePromptTokens(gctx)

 // 投机预扣
 remaining, err := f.stateStore.RateLimitIncr(context.Background(), policy.ID, estimate, time.Minute)
 if err != nil {
  return err
 }

 if remaining < 0 {
  // 超限，退款
  f.stateStore.RateLimitRefund(context.Background(), policy.ID, estimate)
  return &HTTPError{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"}
 }

 return nil
}

func estimatePromptTokens(gctx *gateway.GatewayContext) int64 {
 // Content-Length / 4 粗估
 return int64(len(gctx.RawBody)) / 4
}
```

- [ ] **Step 4: 实现 ValidateFilter**

```go
// pkg/gateway/filters/validate.go
package filters

import (
 "net/http"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// ValidateFilter 校验请求
type ValidateFilter struct {
 knownModels map[string]bool
}

func NewValidateFilter(knownModels map[string]bool) *ValidateFilter {
 return &ValidateFilter{knownModels: knownModels}
}

func (f *ValidateFilter) Name() string  { return "validate" }
func (f *ValidateFilter) Order() int    { return 30 }
func (f *ValidateFilter) OnRequest(gctx *gateway.GatewayContext) error {
 if gctx.Model == "" {
  return &HTTPError{Code: http.StatusBadRequest, Message: "model is required"}
 }

 if !f.knownModels[gctx.Model] {
  return &HTTPError{Code: http.StatusBadRequest, Message: "unknown model: " + gctx.Model}
 }

 return nil
}
```

- [ ] **Step 5: 编写测试**

```go
// pkg/gateway/filters/filter_test.go
package filters

import (
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
)

func TestAuthFilter(t *testing.T) {
 f := NewAuthFilter(map[string]string{"sk-test": "user1"})

 // 有效 key
 r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
 r.Header.Set("Authorization", "Bearer sk-test")
 gctx := &gateway.GatewayContext{Request: r}

 err := f.OnRequest(gctx)
 if err != nil {
  t.Fatal(err)
 }
 if gctx.APIKey != "sk-test" {
  t.Errorf("expected sk-test, got %s", gctx.APIKey)
 }

 // 无效 key
 r = httptest.NewRequest("POST", "/v1/chat/completions", nil)
 r.Header.Set("Authorization", "Bearer invalid")
 gctx = &gateway.GatewayContext{Request: r}

 err = f.OnRequest(gctx)
 if err == nil {
  t.Error("expected error for invalid key")
 }
}

func TestRateLimitFilter(t *testing.T) {
 ss := store.NewMemoryStateStore()
 matcher := gateway.NewPolicyMatcher([]*gateway.Policy{
  {ID: "default", RPM: 60, TPM: 100000},
 })
 f := NewRateLimitFilter(ss, matcher)

 body := strings.NewReader(`{"model":"gpt-4","messages":[]}`)
 r := httptest.NewRequest("POST", "/v1/chat/completions", body)
 gctx := &gateway.GatewayContext{
  Request: r,
  RawBody: []byte(`{"model":"gpt-4","messages":[]}`),
  Model:   "gpt-4",
 }

 err := f.OnRequest(gctx)
 if err != nil {
  t.Fatal(err)
 }
 if gctx.Policy == nil {
  t.Error("expected policy to be set")
 }
}

func TestValidateFilter(t *testing.T) {
 f := NewValidateFilter(map[string]bool{"gpt-4": true, "gpt-3.5-turbo": true})

 // 有效 model
 gctx := &gateway.GatewayContext{Model: "gpt-4"}
 err := f.OnRequest(gctx)
 if err != nil {
  t.Fatal(err)
 }

 // 无效 model
 gctx = &gateway.GatewayContext{Model: "unknown"}
 err = f.OnRequest(gctx)
 if err == nil {
  t.Error("expected error for unknown model")
 }
}
```

- [ ] **Step 6: 运行测试**

Run: `go test ./pkg/gateway/filters/... -v`
Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add pkg/gateway/filter.go pkg/gateway/filters/
git commit -m "feat(gateway): 添加 InboundFilter 接口和实现（Auth/RateLimit/Validate）"
```

---

### Task 12: OutboundFilter 实现

**Files:**

- Create: `pkg/gateway/filters/token_settlement.go`
- Create: `pkg/gateway/filters/sticky_session.go`
- Create: `pkg/gateway/filters/metrics.go`
- Create: `pkg/gateway/filters/access_log.go`

- [ ] **Step 1: 实现 TokenSettlementFilter**

```go
// pkg/gateway/filters/token_settlement.go
package filters

import (
 "context"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
)

// TokenSettlementFilter 实际 token 与预扣差额结算
type TokenSettlementFilter struct {
 stateStore store.StateStore
}

func NewTokenSettlementFilter(ss store.StateStore) *TokenSettlementFilter {
 return &TokenSettlementFilter{stateStore: ss}
}

func (f *TokenSettlementFilter) Name() string              { return "token_settlement" }
func (f *TokenSettlementFilter) Order() int                { return 10 }
func (f *TokenSettlementFilter) Criticality() gateway.FilterCriticality { return gateway.Critical }
func (f *TokenSettlementFilter) OnResponse(gctx *gateway.GatewayContext) error {
 if gctx.Policy == nil {
  return nil
 }

 // 计算实际 token
 actual := int64(gctx.PromptTokens + gctx.CompletionTokens)
 estimated := estimatePromptTokens(gctx)

 if actual < estimated {
  // 退款
  return f.stateStore.RateLimitRefund(context.Background(), gctx.Policy.ID, estimated-actual)
 } else if actual > estimated {
  // 追加扣款
  _, err := f.stateStore.RateLimitIncr(context.Background(), gctx.Policy.ID, actual-estimated, 0)
  return err
 }

 return nil
}
```

- [ ] **Step 2: 实现 StickySessionFilter**

```go
// pkg/gateway/filters/sticky_session.go
package filters

import (
 "context"
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
)

// StickySessionFilter 保存 SessionID -> EndpointID
type StickySessionFilter struct {
 stateStore store.StateStore
 ttl        time.Duration
}

func NewStickySessionFilter(ss store.StateStore, ttl time.Duration) *StickySessionFilter {
 return &StickySessionFilter{stateStore: ss, ttl: ttl}
}

func (f *StickySessionFilter) Name() string              { return "sticky_session" }
func (f *StickySessionFilter) Order() int                { return 20 }
func (f *StickySessionFilter) Criticality() gateway.FilterCriticality { return gateway.Critical }
func (f *StickySessionFilter) OnResponse(gctx *gateway.GatewayContext) error {
 if gctx.SessionID == "" || gctx.SelectedEndpoint == nil {
  return nil
 }

 return f.stateStore.StickySet(
  context.Background(),
  gctx.SessionID,
  gctx.SelectedEndpoint.ID,
  f.ttl,
 )
}
```

- [ ] **Step 3: 实现 MetricsFilter**

```go
// pkg/gateway/filters/metrics.go
package filters

import (
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/prometheus/client_golang/prometheus"
)

// MetricsFilter Prometheus 指标
type MetricsFilter struct {
 requestDuration *prometheus.HistogramVec
 requestTotal    *prometheus.CounterVec
 tokensTotal     *prometheus.CounterVec
}

func NewMetricsFilter(reg prometheus.Registerer) *MetricsFilter {
 f := &MetricsFilter{
  requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
   Name:    "gateway_request_duration_seconds",
   Help:    "Request duration in seconds",
   Buckets: prometheus.DefBuckets,
  }, []string{"model", "provider", "status", "stream"}),
  requestTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
   Name: "gateway_request_total",
   Help: "Total requests",
  }, []string{"model", "provider", "status", "stream"}),
  tokensTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
   Name: "gateway_tokens_total",
   Help: "Total tokens",
  }, []string{"model", "provider", "type"}),
 }

 reg.MustRegister(f.requestDuration, f.requestTotal, f.tokensTotal)
 return f
}

func (f *MetricsFilter) Name() string              { return "metrics" }
func (f *MetricsFilter) Order() int                { return 30 }
func (f *MetricsFilter) Criticality() gateway.FilterCriticality { return gateway.BestEffort }
func (f *MetricsFilter) OnResponse(gctx *gateway.GatewayContext) error {
 status := "success"
 if gctx.Err != nil {
  status = "error"
 }

 stream := "false"
 if gctx.IsStream {
  stream = "true"
 }

 provider := ""
 if gctx.SelectedEndpoint != nil {
  provider = gctx.SelectedEndpoint.Provider
 }

 duration := time.Since(gctx.StartTime).Seconds()

 f.requestDuration.WithLabelValues(gctx.Model, provider, status, stream).Observe(duration)
 f.requestTotal.WithLabelValues(gctx.Model, provider, status, stream).Inc()
 f.tokensTotal.WithLabelValues(gctx.Model, provider, "prompt").Add(float64(gctx.PromptTokens))
 f.tokensTotal.WithLabelValues(gctx.Model, provider, "completion").Add(float64(gctx.CompletionTokens))

 return nil
}
```

- [ ] **Step 4: 实现 AccessLogFilter**

```go
// pkg/gateway/filters/access_log.go
package filters

import (
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "go.uber.org/zap"
)

// AccessLogFilter 结构化访问日志
type AccessLogFilter struct {
 logger *zap.Logger
}

func NewAccessLogFilter(logger *zap.Logger) *AccessLogFilter {
 return &AccessLogFilter{logger: logger}
}

func (f *AccessLogFilter) Name() string              { return "access_log" }
func (f *AccessLogFilter) Order() int                { return 40 }
func (f *AccessLogFilter) Criticality() gateway.FilterCriticality { return gateway.BestEffort }
func (f *AccessLogFilter) OnResponse(gctx *gateway.GatewayContext) error {
 provider := ""
 endpointID := ""
 if gctx.SelectedEndpoint != nil {
  provider = gctx.SelectedEndpoint.Provider
  endpointID = gctx.SelectedEndpoint.ID
 }

 fields := []zap.Field{
  zap.String("original_model", gctx.OriginalModel),
  zap.String("model", gctx.Model),
  zap.String("provider", provider),
  zap.String("endpoint", endpointID),
  zap.Bool("stream", gctx.IsStream),
  zap.Duration("latency", time.Since(gctx.StartTime)),
  zap.Duration("ttft", gctx.TTFT),
  zap.Int("prompt_tokens", gctx.PromptTokens),
  zap.Int("completion_tokens", gctx.CompletionTokens),
  zap.Float64("cost", gctx.Cost),
  zap.Int("attempts", gctx.AttemptCount),
  zap.Strings("fallback_chain", gctx.FallbackChain),
  zap.String("api_key", gctx.APIKey),
  zap.String("user_id", gctx.UserID),
  zap.String("session_id", gctx.SessionID),
 }

 if gctx.Err != nil {
  fields = append(fields, zap.Error(gctx.Err))
  f.logger.Error("request completed with error", fields...)
 } else {
  f.logger.Info("request completed", fields...)
 }

 return nil
}
```

- [ ] **Step 5: 提交**

```bash
git add pkg/gateway/filters/
git commit -m "feat(gateway): 添加 OutboundFilter 实现（TokenSettlement/Sticky/Metrics/AccessLog）"
```

---

## Phase 7: SSE 拦截 + Engine 组装

### Task 13: SSEInterceptWriter

**Files:**

- Create: `pkg/util/sse/intercept_writer.go`
- Create: `pkg/util/sse/parser.go`
- Create: `pkg/util/sse/sse_test.go`

- [ ] **Step 1: 实现 SSEInterceptWriter**

```go
// pkg/util/sse/intercept_writer.go
package sse

import (
 "net/http"
 "time"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

// InterceptWriter 透明包装 ResponseWriter，拦截 SSE 流
type InterceptWriter struct {
 http.ResponseWriter
 gctx      *gateway.GatewayContext
 parser    *Parser
 firstByte bool
 startTime time.Time
 flusher   http.Flusher
}

// NewInterceptWriter 创建 SSE 拦截 Writer
func NewInterceptWriter(gctx *gateway.GatewayContext) *InterceptWriter {
 flusher, _ := gctx.ResponseWriter.(http.Flusher)

 return &InterceptWriter{
  ResponseWriter: gctx.ResponseWriter,
  gctx:           gctx,
  parser:         NewParser(),
  startTime:      time.Now(),
  flusher:        flusher,
 }
}

func (w *InterceptWriter) Write(p []byte) (int, error) {
 if !w.firstByte {
  w.firstByte = true
  w.gctx.TTFT = time.Since(w.startTime)
 }

 // 解析 SSE 帧，累计 token
 w.parser.Feed(p)

 return w.ResponseWriter.Write(p)
}

func (w *InterceptWriter) Flush() {
 if w.flusher != nil {
  w.flusher.Flush()
 }
}

// Hijack 支持 WebSocket 等
func (w *InterceptWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
 if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
  return hj.Hijack()
 }
 return nil, nil, fmt.Errorf("ResponseWriter does not support Hijack")
}
```

- [ ] **Step 2: 实现 SSE Parser**

```go
// pkg/util/sse/parser.go
package sse

import (
 "bytes"
 "encoding/json"
)

// Parser SSE 帧解析器
type Parser struct {
 buffer    []byte
 totalTokens int
}

// NewParser 创建 Parser
func NewParser() *Parser {
 return &Parser{}
}

// Feed 喂入数据
func (p *Parser) Feed(data []byte) {
 p.buffer = append(p.buffer, data...)

 // 解析 SSE 帧
 for {
  idx := bytes.Index(p.buffer, []byte("\n\n"))
  if idx < 0 {
   break
  }

  frame := p.buffer[:idx]
  p.buffer = p.buffer[idx+2:]

  p.parseFrame(frame)
 }
}

// parseFrame 解析单个 SSE 帧
func (p *Parser) parseFrame(frame []byte) {
 // 查找 data: 行
 lines := bytes.Split(frame, []byte("\n"))
 for _, line := range lines {
  if bytes.HasPrefix(line, []byte("data: ")) {
   data := bytes.TrimPrefix(line, []byte("data: "))
   if string(data) == "[DONE]" {
    return
   }

   // 尝试解析 JSON 获取 usage
   var chunk struct {
    Usage *struct {
     PromptTokens     int `json:"prompt_tokens"`
     CompletionTokens int `json:"completion_tokens"`
    } `json:"usage"`
   }

   if err := json.Unmarshal(data, &chunk); err == nil && chunk.Usage != nil {
    p.totalTokens = chunk.Usage.PromptTokens + chunk.Usage.CompletionTokens
   }
  }
 }
}

// GetTotalTokens 获取解析到的总 token 数
func (p *Parser) GetTotalTokens() int {
 return p.totalTokens
}
```

- [ ] **Step 3: 编写测试**

```go
// pkg/util/sse/sse_test.go
package sse

import (
 "net/http/httptest"
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
)

func TestInterceptWriter_FirstByte(t *testing.T) {
 w := httptest.NewRecorder()
 gctx := &gateway.GatewayContext{ResponseWriter: w}

 iw := NewInterceptWriter(gctx)

 // 首次写入
 iw.Write([]byte("data: test\n\n"))

 if gctx.TTFT == 0 {
  t.Error("expected TTFT to be set after first write")
 }
}

func TestParser_Feed(t *testing.T) {
 p := NewParser()

 // 带 usage 的帧
 frame := `data: {"id":"1","usage":{"prompt_tokens":10,"completion_tokens":20}}

`
 p.Feed([]byte(frame))

 if p.GetTotalTokens() != 30 {
  t.Errorf("expected 30 tokens, got %d", p.GetTotalTokens())
 }
}
```

- [ ] **Step 4: 运行测试**

Run: `go test ./pkg/util/sse/... -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add pkg/util/sse/
git commit -m "feat(sse): 添加 SSEInterceptWriter 和 Parser"
```

---

### Task 14: Engine 核心

**Files:**

- Create: `pkg/gateway/engine.go`
- Create: `pkg/gateway/pipeline.go`
- Create: `pkg/gateway/engine_test.go`

- [ ] **Step 1: 定义 Pipeline 和 Engine**

```go
// pkg/gateway/pipeline.go
package gateway

// Pipeline 一组 Filter + Invoker 配置的有序组合
type Pipeline struct {
 Name            string
 RequestTypes    []RequestType
 InboundFilters  []InboundFilter
 OutboundFilters []OutboundFilter
 Invoker         Invoker
}

// PipelineConfig Pipeline 配置
type PipelineConfig struct {
 Name            string            `yaml:"name"`
 RequestTypes    []RequestType     `yaml:"request_types"`
 InboundFilters  []string          `yaml:"inbound_filters"`
 OutboundFilters []string          `yaml:"outbound_filters"`
 Invoker         InvokerConfig     `yaml:"invoker"`
}

// InvokerConfig Invoker 配置
type InvokerConfig struct {
 Type     string           `yaml:"type"` // cluster, fallback
 Retry    RetryConfig      `yaml:"retry"`
 Fallback []FallbackConfig `yaml:"fallback"`
}

// RetryConfig 重试配置
type RetryConfig struct {
 MaxRetries int         `yaml:"max_retries"`
 Backoff    BackoffConfig `yaml:"backoff"`
 ErrorRules []RetryRule `yaml:"error_rules"`
}

// FallbackConfig 降级配置
type FallbackConfig struct {
 Model      string `yaml:"model"`
 Provider   string `yaml:"provider"`
 ErrorRules []FallbackRule `yaml:"error_rules"`
}
```

```go
// pkg/gateway/engine.go
package gateway

import (
 "context"
 "fmt"
 "net/http"
 "sync"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "go.uber.org/zap"
)

// Engine 网关引擎
type Engine struct {
 config       *EngineConfig
 discovery    Discovery
 pipelines    map[string]*Pipeline
 stateStore   store.StateStore
 logger       *zap.Logger
 mu           sync.RWMutex
}

// EngineConfig 引擎配置
type EngineConfig struct {
 Pipelines map[string]*PipelineConfig `yaml:"pipelines"`
 Providers map[string]*ProviderConfig `yaml:"providers"`
}

// NewEngine 创建 Engine
func NewEngine(config *EngineConfig, discovery Discovery, stateStore store.StateStore, logger *zap.Logger) *Engine {
 return &Engine{
  config:     config,
  discovery:  discovery,
  pipelines:  make(map[string]*Pipeline),
  stateStore: stateStore,
  logger:     logger,
 }
}

// Init 初始化 Engine
func (e *Engine) Init() error {
 // 构建 pipelines
 for name, cfg := range e.config.Pipelines {
  pipeline, err := e.buildPipeline(cfg)
  if err != nil {
   return fmt.Errorf("build pipeline %s: %w", name, err)
  }
  e.pipelines[name] = pipeline
 }

 return nil
}

// HandleRequest 处理 HTTP 请求
func (e *Engine) HandleRequest(w http.ResponseWriter, r *http.Request) {
 // Acquire context
 gctx := AcquireContext(w, r)
 defer ReleaseContext(gctx)

 // 解析请求
 if err := e.parseRequest(gctx); err != nil {
  e.writeError(w, http.StatusBadRequest, err)
  return
 }

 // 匹配 pipeline
 pipeline := e.matchPipeline(gctx)
 if pipeline == nil {
  e.writeError(w, http.StatusInternalServerError, fmt.Errorf("no pipeline matched"))
  return
 }

 // 执行 InboundFilters
 for _, f := range pipeline.InboundFilters {
  if err := f.OnRequest(gctx); err != nil {
   e.writeError(w, e.getErrorCode(err), err)
   return
  }
 }

 // 执行 Invoker
 gctx.Err = pipeline.Invoker.Invoke(gctx)

 // 执行 OutboundFilters（始终执行）
 for _, f := range pipeline.OutboundFilters {
  if err := f.OnResponse(gctx); err != nil {
   e.logger.Error("outbound filter failed",
    zap.String("filter", f.Name()),
    zap.Error(err))

   if f.Criticality() == Critical {
    // 入补偿队列
    e.enqueueCompensation(f, gctx)
   }
  }
 }

 // 写响应（非流式）
 if !gctx.IsStream && gctx.Err == nil {
  e.writeJSON(w, gctx.Response)
 }
}

// parseRequest 解析请求
func (e *Engine) parseRequest(gctx *GatewayContext) error {
 // 解析 RequestType
 gctx.RequestType = resolveRequestType(gctx.Request.URL.Path)

 // 解析 body
 if err := e.readBody(gctx); err != nil {
  return err
 }

 // 提取 model 和 stream
 gctx.Model = extractModel(gctx.RawBody)
 gctx.OriginalModel = gctx.Model
 gctx.IsStream = extractStream(gctx.RawBody)

 return nil
}

// matchPipeline 匹配 Pipeline
func (e *Engine) matchPipeline(gctx *GatewayContext) *Pipeline {
 // 按 RequestType 匹配
 for _, p := range e.pipelines {
  for _, rt := range p.RequestTypes {
   if rt == gctx.RequestType {
    return p
   }
  }
 }

 // 默认 pipeline
 return e.pipelines["default"]
}

// buildPipeline 构建 Pipeline
func (e *Engine) buildPipeline(cfg *PipelineConfig) (*Pipeline, error) {
 pipeline := &Pipeline{
  Name:         cfg.Name,
  RequestTypes: cfg.RequestTypes,
 }

 // 构建 InboundFilters
 for _, name := range cfg.InboundFilters {
  f, err := e.getFilter(name)
  if err != nil {
   return nil, err
  }
  if inf, ok := f.(InboundFilter); ok {
   pipeline.InboundFilters = append(pipeline.InboundFilters, inf)
  }
 }

 // 构建 OutboundFilters
 for _, name := range cfg.OutboundFilters {
  f, err := e.getFilter(name)
  if err != nil {
   return nil, err
  }
  if outf, ok := f.(OutboundFilter); ok {
   pipeline.OutboundFilters = append(pipeline.OutboundFilters, outf)
  }
 }

 // 构建 Invoker
 invoker, err := e.buildInvoker(cfg.Invoker)
 if err != nil {
  return nil, err
 }
 pipeline.Invoker = invoker

 return pipeline, nil
}

// buildInvoker 构建 Invoker
func (e *Engine) buildInvoker(cfg InvokerConfig) (Invoker, error) {
 switch cfg.Type {
 case "cluster":
  return e.buildClusterInvoker(cfg.Retry)
 case "fallback":
  return e.buildFallbackInvoker(cfg.Fallback, cfg.Retry)
 default:
  return nil, fmt.Errorf("unknown invoker type: %s", cfg.Type)
 }
}

// buildClusterInvoker 构建 ClusterInvoker
func (e *Engine) buildClusterInvoker(retryCfg RetryConfig) (*ClusterInvoker, error) {
 routers := []Router{
  &APIRouter{},
  NewCircuitBreakerRouter(e.stateStore, e.logger),
 }

 lb := NewRoundRobin()

 retry := &RetryStrategy{
  MaxRetries: retryCfg.MaxRetries,
  Backoff:    retryCfg.Backoff,
  ErrorRules: retryCfg.ErrorRules,
 }

 return NewClusterInvoker(
  e.discovery,
  routers,
  lb,
  retry,
  NewCircuitBreakerManager(e.stateStore),
  e.stateStore,
  e.logger,
 ), nil
}

// buildFallbackInvoker 构建 FallbackInvoker
func (e *Engine) buildFallbackInvoker(fallbacks []FallbackConfig, retryCfg RetryConfig) (*FallbackInvoker, error) {
 var entries []FallbackEntry

 for _, fb := range fallbacks {
  ci, err := e.buildClusterInvoker(retryCfg)
  if err != nil {
   return nil, err
  }
  entries = append(entries, FallbackEntry{
   Model:          fb.Model,
   ClusterInvoker: ci,
  })
 }

 return NewFallbackInvoker(entries, fallbacks[0].ErrorRules), nil
}

// getFilter 获取 Filter
func (e *Engine) getFilter(name string) (interface{}, error) {
 // 这里应该从注册表获取
 return nil, fmt.Errorf("filter not found: %s", name)
}

// resolveRequestType 解析请求类型
func resolveRequestType(path string) RequestType {
 switch path {
 case "/v1/chat/completions":
  return RequestTypeChatCompletion
 case "/v1/embeddings":
  return RequestTypeEmbedding
 case "/v1/images/generations":
  return RequestTypeImageGeneration
 case "/v1/responses":
  return RequestTypeResponses
 default:
  return RequestTypeChatCompletion
 }
}

func (e *Engine) readBody(gctx *GatewayContext) error {
 body, err := io.ReadAll(gctx.Request.Body)
 if err != nil {
  return err
 }
 gctx.RawBody = body
 gctx.Request.Body.Close()
 return nil
}

func extractModel(body []byte) string {
 // 使用 gjson 快速提取
 result := gjson.GetBytes(body, "model")
 return result.String()
}

func extractStream(body []byte) bool {
 result := gjson.GetBytes(body, "stream")
 return result.Bool()
}

func (e *Engine) writeError(w http.ResponseWriter, code int, err error) {
 w.Header().Set("Content-Type", "application/json")
 w.WriteHeader(code)
 json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func (e *Engine) writeJSON(w http.ResponseWriter, v interface{}) {
 w.Header().Set("Content-Type", "application/json")
 json.NewEncoder(w).Encode(v)
}

func (e *Engine) getErrorCode(err error) int {
 if httpErr, ok := err.(*HTTPError); ok {
  return httpErr.Code
 }
 return http.StatusInternalServerError
}

func (e *Engine) enqueueCompensation(f OutboundFilter, gctx *GatewayContext) {
 // TODO: 实现补偿队列
 e.logger.Warn("compensation not implemented yet")
}
```

- [ ] **Step 2: 编写 Engine 测试**

```go
// pkg/gateway/engine_test.go
package gateway

import (
 "bytes"
 "context"
 "net/http"
 "net/http/httptest"
 "testing"

 "github.com/anthropic-ai/tokenlive-gateway/pkg/store"
 "go.uber.org/zap"
)

func TestEngine_HandleRequest(t *testing.T) {
 logger, _ := zap.NewDevelopment()
 ss := store.NewMemoryStateStore()

 // Mock provider
 provider := &mockProvider{
  name:  "openai",
  pType: ProviderOpenAI,
  caps:  []RequestType{RequestTypeChatCompletion},
  invokeFn: func(gctx *GatewayContext) error {
   gctx.Response = map[string]interface{}{
    "choices": []map[string]interface{}{
     {"message": map[string]string{"content": "Hello"}},
    },
   }
   return nil
  },
 }

 // Mock discovery
 discovery := &mockDiscovery{
  endpoints: []*Endpoint{
   {ID: "ep1", Provider: "openai", Model: "gpt-4", URL: "http://localhost:8080"},
  },
 }

 // 创建 Engine
 engine := &Engine{
  config: &EngineConfig{},
  discovery: discovery,
  pipelines: map[string]*Pipeline{
   "default": {
    Name:         "default",
    RequestTypes: []RequestType{RequestTypeChatCompletion},
    Invoker: NewClusterInvoker(
     discovery,
     []Router{},
     &mockLoadBalancer{provider: provider},
     &RetryStrategy{MaxRetries: 0},
     NewCircuitBreakerManager(ss),
     ss,
     logger,
    ),
   },
  },
  stateStore: ss,
  logger:     logger,
 }

 // 测试请求
 body := bytes.NewBufferString(`{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}`)
 r := httptest.NewRequest("POST", "/v1/chat/completions", body)
 w := httptest.NewRecorder()

 engine.HandleRequest(w, r)

 if w.Code != http.StatusOK {
  t.Errorf("expected 200, got %d", w.Code)
 }
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/gateway/... -v -run TestEngine`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add pkg/gateway/engine.go pkg/gateway/pipeline.go pkg/gateway/engine_test.go
git commit -m "feat(gateway): 添加 Engine 核心和 Pipeline 组装"
```

---

## Phase 8: Handler 迁移 + 配置热加载

### Task 15: Gin Handler 迁移

**Files:**

- Modify: `internal/handler/llm_handler.go`

- [ ] **Step 1: 改写 LLM Handler 为薄适配器**

```go
// internal/handler/llm_handler.go
package handler

import (
 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/gin-gonic/gin"
)

// LLMHandler LLM 请求处理器（薄 Gin 适配器）
type LLMHandler struct {
 engine *gateway.Engine
}

// NewLLMHandler 创建 LLM Handler
func NewLLMHandler(engine *gateway.Engine) *LLMHandler {
 return &LLMHandler{engine: engine}
}

// ChatCompletion 处理聊天完成请求
func (h *LLMHandler) ChatCompletion(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}

// CreateEmbedding 处理嵌入请求
func (h *LLMHandler) CreateEmbedding(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}

// ListModels 处理模型列表请求
func (h *LLMHandler) ListModels(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}
```

- [ ] **Step 2: 更新路由注册**

```go
// internal/router/router.go（修改）
package router

import (
 "github.com/anthropic-ai/tokenlive-gateway/internal/handler"
 "github.com/anthropic-ai/tokenlive-gateway/pkg/gateway"
 "github.com/gin-gonic/gin"
)

func SetupRoutes(r *gin.Engine, engine *gateway.Engine) {
 llmHandler := handler.NewLLMHandler(engine)

 v1 := r.Group("/v1")
 {
  v1.POST("/chat/completions", llmHandler.ChatCompletion)
  v1.POST("/embeddings", llmHandler.CreateEmbedding)
  v1.GET("/models", llmHandler.ListModels)
 }
}
```

- [ ] **Step 3: 提交**

```bash
git add internal/handler/ internal/router/
git commit -m "refactor(handler): 将 LLM Handler 改写为 Engine 薄适配器"
```

---

### Task 16: 配置热加载

**Files:**

- Create: `pkg/gateway/config_watcher.go`
- Create: `pkg/gateway/config_watcher_test.go`

- [ ] **Step 1: 实现配置热加载**

```go
// pkg/gateway/config_watcher.go
package gateway

import (
 "sync/atomic"
 "time"

 "github.com/fsnotify/fsnotify"
 "go.uber.org/zap"
)

// ConfigWatcher 配置热加载器
type ConfigWatcher struct {
 configPath string
 engine     *Engine
 logger     *zap.Logger
 watcher    *fsnotify.Watcher
 done       chan struct{}
}

// NewConfigWatcher 创建配置热加载器
func NewConfigWatcher(configPath string, engine *Engine, logger *zap.Logger) *ConfigWatcher {
 return &ConfigWatcher{
  configPath: configPath,
  engine:     engine,
  logger:     logger,
  done:       make(chan struct{}),
 }
}

// Start 启动配置监听
func (cw *ConfigWatcher) Start() error {
 watcher, err := fsnotify.NewWatcher()
 if err != nil {
  return err
 }

 cw.watcher = watcher

 if err := watcher.Add(cw.configPath); err != nil {
  return err
 }

 go cw.watch()

 return nil
}

// Stop 停止配置监听
func (cw *ConfigWatcher) Stop() {
 close(cw.done)
 if cw.watcher != nil {
  cw.watcher.Close()
 }
}

func (cw *ConfigWatcher) watch() {
 debounce := time.NewTimer(0)
 <-debounce.C // 消耗初始事件

 for {
  select {
  case <-cw.done:
   return
  case event, ok := <-cw.watcher.Events:
   if !ok {
    return
   }

   if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
    debounce.Reset(500 * time.Millisecond)
   }
  case <-debounce.C:
   cw.reload()
  case err, ok := <-cw.watcher.Errors:
   if !ok {
    return
   }
   cw.logger.Error("config watcher error", zap.Error(err))
  }
 }
}

func (cw *ConfigWatcher) reload() {
 cw.logger.Info("reloading configuration...")

 // 解析新配置
 newConfig, err := LoadConfig(cw.configPath)
 if err != nil {
  cw.logger.Error("failed to load config, keeping old config",
   zap.Error(err))
  return
 }

 // 校验配置
 if err := ValidateConfig(newConfig); err != nil {
  cw.logger.Error("invalid config, keeping old config",
   zap.Error(err))
  return
 }

 // 原子替换
 cw.engine.UpdateConfig(newConfig)

 cw.logger.Info("configuration reloaded successfully")
}

// LoadConfig 加载配置
func LoadConfig(path string) (*EngineConfig, error) {
 // TODO: 实现配置加载
 return nil, nil
}

// ValidateConfig 校验配置
func ValidateConfig(config *EngineConfig) error {
 // TODO: 实现配置校验
 return nil
}
```

- [ ] **Step 2: 添加 Engine 配置更新方法**

```go
// pkg/gateway/engine.go（添加方法）

// UpdateConfig 原子更新配置
func (e *Engine) UpdateConfig(config *EngineConfig) {
 e.mu.Lock()
 defer e.mu.Unlock()

 e.config = config

 // 重建 pipelines
 e.pipelines = make(map[string]*Pipeline)
 for name, cfg := range config.Pipelines {
  pipeline, err := e.buildPipeline(cfg)
  if err != nil {
   e.logger.Error("failed to build pipeline",
    zap.String("name", name),
    zap.Error(err))
   continue
  }
  e.pipelines[name] = pipeline
 }
}
```

- [ ] **Step 3: 提交**

```bash
git add pkg/gateway/config_watcher.go
git commit -m "feat(gateway): 添加配置热加载支持"
```

---

## 完成

计划已保存到 `docs/superpowers/plans/2026-05-25-engine-pipeline-migration.md`。

**两种执行方式：**

**1. Subagent-Driven（推荐）** - 每个任务派发独立 subagent，任务间 review，快速迭代

**2. Inline Execution** - 在当前会话中执行任务，批量执行带检查点

选择哪种方式？
