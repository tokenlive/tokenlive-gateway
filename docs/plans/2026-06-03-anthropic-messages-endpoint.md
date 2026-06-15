# Anthropic Messages 原生入口实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 tokenlive-gateway 上新增 `POST /v1/messages` 入口,原样透传 Anthropic Messages API 请求/响应,错误返回 Anthropic 原生格式。

**Architecture:** 独立 RequestType `messages` + 独立 Pipeline + 独立 `anthropicMessagesInvoker`(原样透传)。路由层 `ProtocolGuardRouter` 过滤掉非 anthropic 协议端点。无需跨协议翻译。错误响应按 `RequestType` 分发:messages 入口用 Anthropic 格式,其他入口用 OpenAI 格式。

**Tech Stack:** Go, Gin, `pkg/core` Engine Pipeline, `httptest`(单测)

**设计文档:** `docs/superpowers/specs/2026-06-03-anthropic-messages-endpoint-design.md`

**重要:** 根据 CLAUDE.md 规定,**所有改动禁止自动 git commit**。测试通过后,代码留在本地工作区,由用户手动提交。

---

### Task 1: 类型与枚举(3 个文件改,1 个文件新)

**文件:**

- Modify: `pkg/core/types.go` — 加 `RequestTypeMessages`,加 `Endpoint.Protocol()` helper
- Modify: `pkg/core/provider.go` — 加 `ProtocolFamily` 类型 + `ProtocolFamilyForRequestType()` 函数
- Modify: `pkg/core/engine_request.go` — `resolveRequestType` 加 `/messages` 分支
- Test: `pkg/core/engine_test.go` — 扩 `TestResolveRequestType` 加新 case

- [ ] **Step 1: 在 `pkg/core/types.go` 加 `RequestTypeMessages` 常量**

在 [第 16 行](pkg/core/types.go#L16) `RequestTypeModelList` 常量之后追加:

```go
RequestTypeMessages RequestType = "messages"
```

- [ ] **Step 2: 在 `pkg/core/types.go` 加 `Endpoint.Protocol()` helper**

在 [第 62 行](pkg/core/types.go#L62) `EffectiveModel()` 方法之后追加:

```go
// Protocol 返回类型化的协议簇(从 ProviderProtocol 字段读取)
func (ep *Endpoint) Protocol() ProtocolFamily {
 return ProtocolFamily(ep.ProviderProtocol)
}
```

- [ ] **Step 3: 在 `pkg/core/provider.go` 加 `ProtocolFamily` 类型**

在 [第 11 行](pkg/core/provider.go#L11) `ProviderAnthropic` 常量之后追加:

```go
// ProtocolFamily 端点协议簇,用于路由阶段匹配 RequestType
type ProtocolFamily string

const (
 ProtocolOpenAI    ProtocolFamily = "openai"
 ProtocolAnthropic ProtocolFamily = "anthropic"
)

// ProtocolFamilyForRequestType 返回 RequestType 要求的端点协议簇。
// 不在表里的 RequestType 返回空串,表示不做协议簇过滤(向后兼容)。
func ProtocolFamilyForRequestType(rt RequestType) ProtocolFamily {
 switch rt {
 case RequestTypeMessages:
  return ProtocolAnthropic
 }
 return ""
}
```

- [ ] **Step 4: 在 `pkg/core/engine_request.go` 的 `resolveRequestType` 加 `/messages` 分支**

在 [第 133 行](pkg/core/engine_request.go#L133) `/chat/completions` case 之后、`/embeddings` case 之前,插入:

```go
case strings.HasSuffix(path, "/messages"):
 return RequestTypeMessages
```

注意:`HasSuffix("/messages")` 与 `/chat/completions` 不冲突。

- [ ] **Step 5: 扩展 `TestResolveRequestType`**

在 `pkg/core/engine_test.go` 的 [第 166 行](pkg/core/engine_test.go#L166) (现有 test table 里 `"/v1/embeddings"` case)之前,插入一个新 case:

```go
{"/v1/messages", RequestTypeMessages},
```

- [ ] **Step 6: 运行单测验证编译 + 测试通过**

Run: `go test ./pkg/core/... -v -run TestResolveRequestType`
Expected: PASS,新 case `"/v1/messages"` → `RequestTypeMessages` 通过

---

### Task 2: 错误格式器(1 个文件新,1 个文件改)

**文件:**

- Create: `pkg/core/error_format.go` — 错误格式器实现
- Create: `pkg/core/error_format_test.go` — 格式器单测
- Modify: `pkg/core/engine_response.go` — `writeError` 改用 `ErrorFormatterForRequestType`

- [ ] **Step 1: 创建 `pkg/core/error_format.go`**

创建以下文件:

```go
package core

import "net/http"

// ErrorFormatter 把 (httpCode, error) 序列化为对应协议簇的错误响应体。
type ErrorFormatter interface {
 Format(code int, err error) map[string]interface{}
}

// ErrorFormatterForRequestType 根据 RequestType 返回对应协议簇的错误格式器。
func ErrorFormatterForRequestType(rt RequestType) ErrorFormatter {
 switch rt {
 case RequestTypeMessages:
  return anthropicErrorFormatter{}
 default:
  return openaiErrorFormatter{}
 }
}

// ===== OpenAI 风格(默认,保持向后兼容) =====

type openaiErrorFormatter struct{}

func (openaiErrorFormatter) Format(code int, err error) map[string]interface{} {
 return map[string]interface{}{
  "error": map[string]interface{}{
   "message": err.Error(),
   "type":    "gateway_error",
   "code":    code,
  },
 }
}

// ===== Anthropic 原生 =====

type anthropicErrorFormatter struct{}

func (anthropicErrorFormatter) Format(code int, err error) map[string]interface{} {
 return map[string]interface{}{
  "type": "error",
  "error": map[string]interface{}{
   "type":    anthropicErrorType(code),
   "message": err.Error(),
  },
 }
}

func anthropicErrorType(code int) string {
 switch code {
 case http.StatusBadRequest:
  return "invalid_request_error"
 case http.StatusUnauthorized:
  return "authentication_error"
 case http.StatusForbidden:
  return "permission_error"
 case http.StatusNotFound:
  return "not_found_error"
 case http.StatusRequestEntityTooLarge:
  return "request_too_large"
 case http.StatusTooManyRequests:
  return "rate_limit_error"
 case http.StatusServiceUnavailable:
  return "overloaded_error"
 default:
  if code >= 500 {
   return "api_error"
  }
  return "invalid_request_error"
 }
}
```

- [ ] **Step 2: 创建 `pkg/core/error_format_test.go`**

```go
package core

import (
 "errors"
 "net/http"
 "testing"
)

func TestAnthropicErrorType(t *testing.T) {
 tests := []struct {
  code int
  want string
 }{
  {http.StatusBadRequest, "invalid_request_error"},
  {http.StatusUnauthorized, "authentication_error"},
  {http.StatusForbidden, "permission_error"},
  {http.StatusNotFound, "not_found_error"},
  {http.StatusRequestEntityTooLarge, "request_too_large"},
  {http.StatusTooManyRequests, "rate_limit_error"},
  {http.StatusServiceUnavailable, "overloaded_error"},
  {http.StatusInternalServerError, "api_error"},
  {http.StatusBadGateway, "api_error"},
  {418, "invalid_request_error"},          // 4xx 默认
  {600, "api_error"},                      // 其他 5xx
 }
 for _, tt := range tests {
  t.Run(http.StatusText(tt.code), func(t *testing.T) {
   got := anthropicErrorType(tt.code)
   if got != tt.want {
    t.Errorf("anthropicErrorType(%d) = %q, want %q", tt.code, got, tt.want)
   }
  })
 }
}

func TestErrorFormatterForRequestType_Messages(t *testing.T) {
 f := ErrorFormatterForRequestType(RequestTypeMessages)
 if _, ok := f.(anthropicErrorFormatter); !ok {
  t.Errorf("expected anthropicErrorFormatter, got %T", f)
 }

 result := f.Format(http.StatusBadRequest, errors.New("bad request"))
 topType, ok := result["type"].(string)
 if !ok || topType != "error" {
  t.Errorf("top-level type = %q, want \"error\"", topType)
 }
 errObj, ok := result["error"].(map[string]interface{})
 if !ok {
  t.Fatalf("missing error object")
 }
 if errObj["type"] != "invalid_request_error" {
  t.Errorf("error.type = %q, want invalid_request_error", errObj["type"])
 }
 if errObj["message"] != "bad request" {
  t.Errorf("error.message = %q, want \"bad request\"", errObj["message"])
 }
}

func TestErrorFormatterForRequestType_Default(t *testing.T) {
 f := ErrorFormatterForRequestType(RequestTypeChatCompletion)
 if _, ok := f.(openaiErrorFormatter); !ok {
  t.Errorf("expected openaiErrorFormatter, got %T", f)
 }
 result := f.Format(http.StatusInternalServerError, errors.New("fail"))
 errObj, ok := result["error"].(map[string]interface{})
 if !ok {
  t.Fatalf("missing error object")
 }
 if errObj["type"] != "gateway_error" {
  t.Errorf("error.type = %q, want gateway_error", errObj["type"])
 }
 if errObj["code"] != http.StatusInternalServerError {
  t.Errorf("error.code = %v, want 500", errObj["code"])
 }
}

func TestErrorFormatterForRequestType_NilRequestType(t *testing.T) {
 f := ErrorFormatterForRequestType(RequestType("unknown"))
 result := f.Format(404, errors.New("not found"))
 if _, ok := result["error"].(map[string]interface{}); !ok {
  t.Error("expected openai format for unknown RequestType")
 }
}
```

- [ ] **Step 3: 运行新测试确认通过**

Run: `go test ./pkg/core/... -v -run "TestAnthropicErrorType|TestErrorFormatterForRequestType"`
Expected: PASS

- [ ] **Step 4: 修改 `pkg/core/engine_response.go` 的 `writeError`**

在 [第 39 行](pkg/core/engine_response.go#L39) 之前的旧逻辑:

```go
 w.Header().Set("Content-Type", "application/json")
 w.WriteHeader(code)
 resp := map[string]interface{}{
  "error": map[string]interface{}{
   "message": err.Error(),
   "type":    "gateway_error",
   "code":    code,
  },
 }
 _ = json.NewEncoder(w).Encode(resp)
```

替换为:

```go
 var rt RequestType
 if gctx != nil {
  rt = gctx.RequestType
 }
 formatter := ErrorFormatterForRequestType(rt)

 w.Header().Set("Content-Type", "application/json")
 w.WriteHeader(code)
 _ = json.NewEncoder(w).Encode(formatter.Format(code, err))
```

- [ ] **Step 5: 运行现有 `engine_response` 相关测试,确认 OpenAI 路径行为不变**

Run: `go test ./pkg/core/... -v -run "TestEngine|TestWriteError"`
Expected: PASS(现有测试走默认 openaiErrorFormatter)

---

### Task 3: ProtocolGuardRouter(2 个文件新)

**文件:**

- Create: `pkg/routers/protocol_guard.go`
- Create: `pkg/routers/protocol_guard_test.go`

- [ ] **Step 1: 创建 `pkg/routers/protocol_guard.go`**

```go
package routers

import "tokenlive-gateway/pkg/core"

// ProtocolGuardRouter 按 RequestType 要求的协议簇过滤端点。
// 例:RequestTypeMessages 只放行 ProviderProtocol="anthropic" 的端点。
type ProtocolGuardRouter struct{}

// NewProtocolGuardRouter 创建 ProtocolGuardRouter。
func NewProtocolGuardRouter() *ProtocolGuardRouter { return &ProtocolGuardRouter{} }

func (r *ProtocolGuardRouter) Name() string { return "protocol_guard" }

func (r *ProtocolGuardRouter) Route(
 gctx *core.GatewayContext,
 endpoints []*core.Endpoint,
) []*core.Endpoint {
 required := core.ProtocolFamilyForRequestType(gctx.RequestType)
 if required == "" {
  return endpoints
 }
 out := make([]*core.Endpoint, 0, len(endpoints))
 for _, ep := range endpoints {
  if ep.Protocol() == required {
   out = append(out, ep)
  }
 }
 return out
}
```

- [ ] **Step 2: 创建 `pkg/routers/protocol_guard_test.go`**

```go
package routers

import (
 "tokenlive-gateway/pkg/core"
 "testing"
)

func TestProtocolGuardRouter_Messages(t *testing.T) {
 r := NewProtocolGuardRouter()

 a1 := &core.Endpoint{ID: "a1", ProviderProtocol: "anthropic"}
 a2 := &core.Endpoint{ID: "a2", ProviderProtocol: "anthropic"}
 o1 := &core.Endpoint{ID: "o1", ProviderProtocol: "openai"}
 o2 := &core.Endpoint{ID: "o2", ProviderProtocol: "openai"}
 empty := &core.Endpoint{ID: "ep", ProviderProtocol: ""}

 tests := []struct {
  name       string
  endpoints  []*core.Endpoint
  reqType    core.RequestType
  wantIDs    []string
 }{
  {
   name:      "空列表",
   endpoints: []*core.Endpoint{},
   reqType:   core.RequestTypeMessages,
   wantIDs:   []string{},
  },
  {
   name:      "全 anthropic 保留",
   endpoints: []*core.Endpoint{a1, a2},
   reqType:   core.RequestTypeMessages,
   wantIDs:   []string{"a1", "a2"},
  },
  {
   name:      "全 openai 过滤空",
   endpoints: []*core.Endpoint{o1, o2},
   reqType:   core.RequestTypeMessages,
   wantIDs:   []string{},
  },
  {
   name:      "混合只留 anthropic",
   endpoints: []*core.Endpoint{a1, o1, a2},
   reqType:   core.RequestTypeMessages,
   wantIDs:   []string{"a1", "a2"},
  },
  {
   name:      "ChatCompletion 无约束放行全部",
   endpoints: []*core.Endpoint{a1, o1},
   reqType:   core.RequestTypeChatCompletion,
   wantIDs:   []string{"a1", "o1"},
  },
  {
   name:      "协议字段为空被排除",
   endpoints: []*core.Endpoint{empty, a1},
   reqType:   core.RequestTypeMessages,
   wantIDs:   []string{"a1"},
  },
 }

 for _, tt := range tests {
  t.Run(tt.name, func(t *testing.T) {
   gctx := &core.GatewayContext{RequestType: tt.reqType}
   result := r.Route(gctx, tt.endpoints)
   var gotIDs []string
   for _, ep := range result {
    gotIDs = append(gotIDs, ep.ID)
   }
   if len(gotIDs) != len(tt.wantIDs) {
    t.Fatalf("got %d endpoints, want %d: %v vs %v", len(gotIDs), len(tt.wantIDs), gotIDs, tt.wantIDs)
   }
   for i, id := range gotIDs {
    if id != tt.wantIDs[i] {
     t.Errorf("index %d: got %q, want %q", i, id, tt.wantIDs[i])
    }
   }
  })
 }
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/routers/... -v -run TestProtocolGuardRouter`
Expected: PASS,6 个 case 全过

---

### Task 4: Validate filter 扩展(1 个文件改,1 个文件改测试)

**文件:**

- Modify: `pkg/filters/validate.go` — 加 `RequestTypeMessages` 分支 + `validateAnthropicMessages` 方法
- Modify: `pkg/filters/validate_test.go` — 加 Anthropic 校验用例

- [ ] **Step 1: 在 `pkg/filters/validate.go` 的 `OnRequest` 末尾加 switch 分支**

在 [第 42 行](pkg/filters/validate.go#L42) 原有的 `if gctx.RequestType == core.RequestTypeChatCompletion && len(gctx.RawBody) > 0 {` 块,替换整个 `if` 块为:

```go
 if len(gctx.RawBody) > 0 {
  switch gctx.RequestType {
  case core.RequestTypeChatCompletion:
   var body struct {
    Messages []json.RawMessage `json:"messages"`
   }
   if err := json.Unmarshal(gctx.RawBody, &body); err != nil {
    return &HTTPError{Code: http.StatusBadRequest, Message: "invalid JSON body"}
   }
   if len(body.Messages) == 0 {
    return &HTTPError{Code: http.StatusBadRequest, Message: "messages is required and must not be empty"}
   }
  case core.RequestTypeMessages:
   return f.validateAnthropicMessages(gctx.RawBody)
  }
 }
 return nil
```

- [ ] **Step 2: 在 `pkg/filters/validate.go` 文件末尾追加 `validateAnthropicMessages` 方法**

在 `validate.go` 最后一个 `}` 之前,追加:

```go
func (f *ValidateFilter) validateAnthropicMessages(body []byte) error {
 var req struct {
  Messages  []json.RawMessage `json:"messages"`
  MaxTokens *int              `json:"max_tokens"` // 指针区分缺省 vs 显式 0
 }
 if err := json.Unmarshal(body, &req); err != nil {
  return &HTTPError{Code: http.StatusBadRequest, Message: "invalid JSON body"}
 }
 if len(req.Messages) == 0 {
  return &HTTPError{Code: http.StatusBadRequest, Message: "messages is required and must not be empty"}
 }
 if req.MaxTokens == nil {
  return &HTTPError{Code: http.StatusBadRequest, Message: "max_tokens is required"}
 }
 if *req.MaxTokens <= 0 {
  return &HTTPError{Code: http.StatusBadRequest, Message: "max_tokens must be positive"}
 }
 return nil
}
```

注意:`MaxTokens *int` 用指针 — Go 的 `int` 零值是 `0`,无法区分"用户传 0"与"用户没传"。`*int` 时 `nil` = 缺省,`&0` = 用户显式传 0。

- [ ] **Step 3: 在 `pkg/filters/validate_test.go` 末尾追加 Anthropic 用例**

在 `validate_test.go` 文件末尾追加:

```go
func TestValidateFilter_Messages_MissingMaxTokens(t *testing.T) {
 knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
 validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
  return knownModels[model], nil
 })
 f := NewValidateFilter(validator)

 gctx := &core.GatewayContext{
  Model:       "claude-sonnet-4-20250514",
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}]}`),
 }

 err := f.OnRequest(gctx)
 if err == nil {
  t.Fatal("expected error for missing max_tokens")
 }
 if getHTTPErrorCode(err) != http.StatusBadRequest {
  t.Errorf("expected 400, got %d", getHTTPErrorCode(err))
 }
 if !strings.Contains(err.Error(), "max_tokens is required") {
  t.Errorf("expected 'max_tokens is required', got '%s'", err.Error())
 }
}

func TestValidateFilter_Messages_MaxTokensZero(t *testing.T) {
 knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
 validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
  return knownModels[model], nil
 })
 f := NewValidateFilter(validator)

 gctx := &core.GatewayContext{
  Model:       "claude-sonnet-4-20250514",
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"max_tokens":0}`),
 }

 err := f.OnRequest(gctx)
 if err == nil {
  t.Fatal("expected error for max_tokens=0")
 }
 if getHTTPErrorCode(err) != http.StatusBadRequest {
  t.Errorf("expected 400, got %d", getHTTPErrorCode(err))
 }
 if !strings.Contains(err.Error(), "must be positive") {
  t.Errorf("expected 'must be positive', got '%s'", err.Error())
 }
}

func TestValidateFilter_Messages_MissingMessages(t *testing.T) {
 knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
 validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
  return knownModels[model], nil
 })
 f := NewValidateFilter(validator)

 gctx := &core.GatewayContext{
  Model:       "claude-sonnet-4-20250514",
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","max_tokens":100}`),
 }

 err := f.OnRequest(gctx)
 if err == nil {
  t.Fatal("expected error for missing messages")
 }
 if getHTTPErrorCode(err) != http.StatusBadRequest {
  t.Errorf("expected 400, got %d", getHTTPErrorCode(err))
 }
 if !strings.Contains(err.Error(), "messages") {
  t.Errorf("expected 'messages' in error, got '%s'", err.Error())
 }
}

func TestValidateFilter_Messages_Valid(t *testing.T) {
 knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
 validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
  return knownModels[model], nil
 })
 f := NewValidateFilter(validator)

 gctx := &core.GatewayContext{
  Model:       "claude-sonnet-4-20250514",
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"max_tokens":100}`),
 }

 err := f.OnRequest(gctx)
 if err != nil {
  t.Fatalf("expected no error for valid messages request, got: %v", err)
 }
}

func TestValidateFilter_Messages_ValidWithOptional(t *testing.T) {
 knownModels := map[string]bool{"claude-sonnet-4-20250514": true}
 validator := mockModelValidator(func(ctx context.Context, model string, tenant string, userID string) (bool, error) {
  return knownModels[model], nil
 })
 f := NewValidateFilter(validator)

 gctx := &core.GatewayContext{
  Model:       "claude-sonnet-4-20250514",
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","system":"You are helpful","messages":[{"role":"user","content":"hi"}],"max_tokens":100,"stream":true,"tools":[]}`),
 }

 err := f.OnRequest(gctx)
 if err != nil {
  t.Fatalf("expected no error, got: %v", err)
 }
}
```

- [ ] **Step 4: 运行 validate filter 测试**

Run: `go test ./pkg/filters/... -v -run TestValidateFilter`
Expected: PASS,老用例 + 新 Anthropic 用例全过

---

### Task 5: Anthropic Messages invoker + handler(3 个文件改/建,1 个文件删)

**文件:**

- Create: `pkg/llm/providers/anthropic_messages.go` — `anthropicMessagesInvoker` 实现
- Modify: `pkg/llm/providers/anthropic.go` — 改 init() 注册、删旧代码、加新 handler 方法
- Delete: `pkg/llm/providers/anthropic_chat.go` — 整文件删除(旧跨协议路径)
- Rewrite: `pkg/llm/providers/anthropic_test.go` — 测试对应新 invoker

- [ ] **Step 1: 创建 `pkg/llm/providers/anthropic_messages.go`**

```go
package providers

import (
 "bytes"
 "fmt"
 "io"
 "net/http"

 "tokenlive-gateway/pkg/core"
)

type anthropicMessagesInvoker struct{}

func (i *anthropicMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
 ap, ok := p.(*AnthropicProvider)
 if !ok {
  return fmt.Errorf("expected *AnthropicProvider, got %T", p)
 }

 endpoint := ap.baseURL + "/v1/messages"
 req, err := http.NewRequestWithContext(gctx.Ctx, http.MethodPost, endpoint, bytes.NewReader(gctx.RawBody))
 if err != nil {
  return fmt.Errorf("create request: %w", err)
 }
 req.Header.Set("Content-Type", "application/json")
 req.Header.Set("x-api-key", ap.apiKey)
 req.Header.Set("anthropic-version", "2023-06-01")

 if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
  for k, v := range gctx.SelectedEndpoint.Headers {
   req.Header.Set(k, v)
  }
 }

 resp, err := ap.client.Do(req)
 if err != nil {
  return fmt.Errorf("upstream request: %w", err)
 }
 defer resp.Body.Close()
 gctx.UpstreamResponse = resp

 if resp.StatusCode >= 400 {
  body, _ := io.ReadAll(resp.Body)
  return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
 }

 if gctx.IsStream {
  return ap.handleMessagesStream(gctx, resp)
 }
 return ap.handleMessagesNonStream(gctx, resp)
}
```

- [ ] **Step 2: 删除 `pkg/llm/providers/anthropic_chat.go` 整个文件**

Run: `rm pkg/llm/providers/anthropic_chat.go`

- [ ] **Step 3: 重写 `pkg/llm/providers/anthropic.go`**

当前文件 [anthropic.go](pkg/llm/providers/anthropic.go) 需要做以下改动:

**a) `init()` 改注册:** [第 20 行](pkg/llm/providers/anthropic.go#L20) 原来:

```go
core.RegisterRequestInvoker(core.ProviderAnthropic, core.RequestTypeChatCompletion, &anthropicChatInvoker{})
```

改为:

```go
core.RegisterRequestInvoker(core.ProviderAnthropic, core.RequestTypeMessages, &anthropicMessagesInvoker{})
```

**b) `RequestTypes()` 改返回值:** [第 47-49 行](pkg/llm/providers/anthropic.go#L47) 原来:

```go
func (p *AnthropicProvider) RequestTypes() []core.RequestType {
 return []core.RequestType{core.RequestTypeChatCompletion}
}
```

改为:

```go
func (p *AnthropicProvider) RequestTypes() []core.RequestType {
 return []core.RequestType{core.RequestTypeMessages}
}
```

**c) 删除旧的跨协议翻译方法** — 删掉以下方法(位于 [第 83-235 行](pkg/llm/providers/anthropic.go#L83)):

- `handleNonStream`
- `convertToOpenAIResponse`
- `convertUsage`
- `handleStream`
- `convertStreamEvent`

**d) 追加两个新 handler 方法** — 在文件末尾 `var _ core.Provider = ...` 之前追加:

```go
// handleMessagesNonStream 原生透传非流式响应,只提取 token 用于计费。
// 不填 gctx.Response:仅填充 gctx.UpstreamBody,Engine 主流程兜底逻辑自动写出。
func (p *AnthropicProvider) handleMessagesNonStream(gctx *core.GatewayContext, resp *http.Response) error {
 body, err := io.ReadAll(resp.Body)
 if err != nil {
  return fmt.Errorf("read response: %w", err)
 }
 gctx.UpstreamBody = body

 var anthropicResp struct {
  Usage struct {
   InputTokens  int `json:"input_tokens"`
   OutputTokens int `json:"output_tokens"`
  } `json:"usage"`
 }
 if err := json.Unmarshal(body, &anthropicResp); err == nil {
  gctx.PromptTokens = anthropicResp.Usage.InputTokens
  gctx.CompletionTokens = anthropicResp.Usage.OutputTokens
 }
 return nil
}

// handleMessagesStream 原生透传 SSE 流,InterceptWriter 用 AnthropicTokenExtractor 提取 token。
func (p *AnthropicProvider) handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
 writer := llm.NewSSEInterceptWriter(gctx, llm.WithTokenExtractor(llm.AnthropicTokenExtractor))
 writer.Header().Set("Content-Type", "text/event-stream")
 writer.Header().Set("Cache-Control", "no-cache")
 writer.Header().Set("Connection", "keep-alive")
 writer.WriteHeader(http.StatusOK)

 buf := make([]byte, 4096)
 for {
  n, err := resp.Body.Read(buf)
  if n > 0 {
   if _, werr := writer.Write(buf[:n]); werr != nil {
    return werr
   }
   writer.Flush()
  }
  if err != nil {
   if err == io.EOF {
    break
   }
   return fmt.Errorf("read upstream stream: %w", err)
  }
 }
 return nil
}
```

**e) 确认 import 列表** — 文件需要的 import:

```go
import (
 "context"
 "encoding/json"
 "fmt"
 "io"
 "net/http"
 "strings"

 "tokenlive-gateway/pkg/core"
 "tokenlive-gateway/pkg/llm"
)
```

(删掉 `"bufio"`,新增 `"tokenlive-gateway/pkg/llm"` 和 `"encoding/json"`,保留其他)

- [ ] **Step 4: 重写 `pkg/llm/providers/anthropic_test.go`**

现在注册的是 `(Anthropic, Messages)` 而不是 `(Anthropic, ChatCompletion)`,所有 `RequestType: core.RequestTypeChatCompletion` 改成 `core.RequestTypeMessages`,旧断言 `resp["object"] == "chat.completion"` 改成验证原始字节透传。整个文件重写为:

```go
package providers

import (
 "context"
 "encoding/json"
 "io"
 "net/http"
 "net/http/httptest"
 "testing"
 "time"

 "tokenlive-gateway/pkg/core"
)

func TestAnthropicMessagesInvoker_NonStream(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  if r.URL.Path != "/v1/messages" {
   t.Errorf("unexpected path: %s", r.URL.Path)
  }
  if r.Header.Get("x-api-key") != "sk-ant-test" {
   t.Errorf("unexpected x-api-key: %s", r.Header.Get("x-api-key"))
  }
  if r.Header.Get("anthropic-version") != "2023-06-01" {
   t.Errorf("unexpected anthropic-version: %s", r.Header.Get("anthropic-version"))
  }

  // 验证请求体原样透传
  body, _ := io.ReadAll(r.Body)
  var req map[string]interface{}
  json.Unmarshal(body, &req)
  if _, ok := req["messages"]; !ok {
   t.Error("expected messages field")
  }

  w.Header().Set("Content-Type", "application/json")
  w.Write([]byte(`{"id":"msg_123","type":"message","role":"assistant","content":[{"type":"text","text":"Hello!"}],"usage":{"input_tokens":15,"output_tokens":8}}`))
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
 gctx := &core.GatewayContext{
  Ctx:         context.Background(),
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"max_tokens":100}`),
  IsStream:    false,
 }

 err := p.Invoke(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if gctx.PromptTokens != 15 {
  t.Errorf("expected prompt_tokens=15, got %d", gctx.PromptTokens)
 }
 if gctx.CompletionTokens != 8 {
  t.Errorf("expected completion_tokens=8, got %d", gctx.CompletionTokens)
 }
 // 关键:UpstreamBody 应包含原始 Anthropic JSON(不要填 Response)
 if gctx.UpstreamBody == nil {
  t.Fatal("expected UpstreamBody to be set")
 }
 if gctx.Response != nil {
  t.Fatal("Response should NOT be set (Engine uses UpstreamBody for passthrough)")
 }
}

func TestAnthropicMessagesInvoker_Stream(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "text/event-stream")
  w.WriteHeader(http.StatusOK)
  flusher := w.(http.Flusher)

  events := []string{
   "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n",
   "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hi\"}}\n\n",
   "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" there\"}}\n\n",
   "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
   "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
  }
  for _, ev := range events {
   w.Write([]byte(ev))
   flusher.Flush()
  }
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)

 rec := httptest.NewRecorder()
 gctx := &core.GatewayContext{
  Ctx:            context.Background(),
  RequestType:    core.RequestTypeMessages,
  RawBody:        []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"stream":true}`),
  IsStream:       true,
  ResponseWriter: rec,
  StartTime:      time.Now().Add(-100 * time.Millisecond),
 }

 err := p.Invoke(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
 if gctx.PromptTokens != 10 {
  t.Errorf("expected prompt_tokens=10, got %d", gctx.PromptTokens)
 }
 if gctx.CompletionTokens != 5 {
  t.Errorf("expected completion_tokens=5, got %d", gctx.CompletionTokens)
 }
 if gctx.TTFT <= 0 {
  t.Error("expected TTFT > 0")
 }
 // 流式时 ResponseWriter 已收到原始 Anthropic SSE 帧
 if rec.Body.Len() == 0 {
  t.Error("expected body to be written to ResponseWriter")
 }
}

func TestAnthropicMessagesInvoker_UpstreamError(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  w.WriteHeader(http.StatusBadRequest)
  w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"bad request"}}`))
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
 gctx := &core.GatewayContext{
  Ctx:         context.Background(),
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[],"max_tokens":10}`),
  IsStream:    false,
 }

 err := p.Invoke(gctx)
 if err == nil {
  t.Fatal("expected error for upstream 400")
 }
}

func TestAnthropicMessagesInvoker_CustomHeaders(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  if r.Header.Get("X-Custom-Auth") != "custom-value" {
   t.Errorf("expected X-Custom-Auth: custom-value, got %s", r.Header.Get("X-Custom-Auth"))
  }
  if r.Header.Get("x-api-key") != "override-key" {
   t.Errorf("expected x-api-key: override-key, got %s", r.Header.Get("x-api-key"))
  }
  w.Header().Set("Content-Type", "application/json")
  w.Write([]byte(`{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1}}`))
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
 gctx := &core.GatewayContext{
  Ctx:         context.Background(),
  RequestType: core.RequestTypeMessages,
  RawBody:     []byte(`{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"test"}],"max_tokens":100}`),
  IsStream:    false,
  SelectedEndpoint: &core.Endpoint{
   Headers: map[string]string{
    "X-Custom-Auth": "custom-value",
    "x-api-key":     "override-key",
   },
  },
 }

 err := p.Invoke(gctx)
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
}

func TestAnthropicProvider_RequestTypes(t *testing.T) {
 p := NewAnthropicProvider("anthropic", "", "", nil)
 caps := p.RequestTypes()
 if len(caps) != 1 || caps[0] != core.RequestTypeMessages {
  t.Errorf("expected [messages], got %v", caps)
 }
}

func TestAnthropicProvider_HealthCheck(t *testing.T) {
 server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
  w.WriteHeader(http.StatusOK)
 }))
 defer server.Close()

 p := NewAnthropicProvider("anthropic", server.URL, "sk-ant-test", nil)
 err := p.HealthCheck(context.Background())
 if err != nil {
  t.Fatalf("expected no error, got %v", err)
 }
}
```

- [ ] **Step 5: 运行所有 anthropic provider 测试**

Run: `go test ./pkg/llm/providers/... -v -run TestAnthropic`
Expected: PASS

- [ ] **Step 6: 运行完整编译确认无断裂**

Run: `go build ./...`
Expected: PASS(删除 anthropic_chat.go 不会导致编译错误 — 若报 `undefined: anthropicChatInvoker`,说明 `anthropic.go` 还有残留引用,需清理)

---

### Task 6: Wire 装配 + Gin 路由(3 个文件改)

**文件:**

- Modify: `cmd/server/wire/engine.go` — 注册 protocol_guard factory,加 messages pipeline,ProviderConfig RequestTypes 按协议分支
- Modify: `pkg/core/engine_builder.go` — 默认 RouterChain 加 `protocol_guard`
- Modify: `internal/router/llm.go` — 加 `POST /v1/messages` 路由
- Modify: `internal/handler/llm_handler.go` — 加 `Messages` 方法

- [ ] **Step 1: 在 `cmd/server/wire/engine.go` 注册 `protocol_guard` Router factory**

在 [第 178 行](cmd/server/wire/engine.go#L178) `tag` factory 注册之后追加:

```go
engine.RegisterRouterFactory("protocol_guard", func(cfg core.RouterConfig, _ core.StateStore, _ *zap.Logger) core.Router {
 return routers.NewProtocolGuardRouter()
})
```

- [ ] **Step 2: 在 `cmd/server/wire/engine.go` 的 `buildFromRelationalConfig` 加 messages pipeline**

在 [第 358 行](cmd/server/wire/engine.go#L358) 附近,embedding pipeline 注册块之后,追加:

```go
// 4. 创建通用的 messages pipeline (Anthropic 原生协议)
if _, exists := engineConfig.Pipelines["messages"]; !exists {
 engineConfig.Pipelines["messages"] = &core.PipelineConfig{
  Name:         "messages",
  RequestTypes: []core.RequestType{core.RequestTypeMessages},
  Invoker: core.InvokerConfig{
   Type: "cluster",
  },
  InboundFilters:          inboundFilters,
  OutboundFilters:         []string{"token_settlement", "sticky_session", "metrics", "access_log"},
  CriticalOutboundFilters: []string{"token_settlement", "sticky_session"},
 }
}
```

- [ ] **Step 3: 在 `cmd/server/wire/engine.go` 按协议簇分支 ProviderConfig RequestTypes**

在 [第 267-271 行](cmd/server/wire/engine.go#L267),原代码:

```go
providerConfigMap[re.ProviderName] = &core.ProviderConfig{
 Name:   re.ProviderName,
 Type:   re.ProviderProtocol,
 Models: providerModels[re.ProviderName],
 RequestTypes: []core.RequestType{
  core.RequestTypeChatCompletion,
  core.RequestTypeEmbedding,
  core.RequestTypeModelList,
 },
}
```

替换为:

```go
caps := []core.RequestType{
 core.RequestTypeChatCompletion,
 core.RequestTypeEmbedding,
 core.RequestTypeModelList,
}
if re.ProviderProtocol == "anthropic" {
 caps = []core.RequestType{core.RequestTypeMessages}
}
providerConfigMap[re.ProviderName] = &core.ProviderConfig{
 Name:         re.ProviderName,
 Type:         re.ProviderProtocol,
 Models:       providerModels[re.ProviderName],
 RequestTypes: caps,
}
```

- [ ] **Step 4: 在 `pkg/core/engine_builder.go` 默认 RouterChain 加 `protocol_guard`**

在 [第 118 行](pkg/core/engine_builder.go#L118),原代码:

```go
names = []string{"API", "circuit_breaker"}
```

改为:

```go
names = []string{"protocol_guard", "API", "circuit_breaker"}
```

- [ ] **Step 5: 在 `internal/handler/llm_handler.go` 加 `Messages` 方法**

在 `CreateEmbedding` 方法之后、`ListModels` 方法之前,追加:

```go
// Messages 处理 Anthropic 原生 Messages 协议请求
func (h *LLMHandler) Messages(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}
```

- [ ] **Step 6: 在 `internal/router/llm.go` 注册 `/messages` 路由**

在 [第 57 行](internal/router/llm.go#L57) `POST("/chat/completions", ...)` 之后、`POST("/embeddings", ...)` 之前,追加:

```go
llmGroup.POST("/messages", deps.LLMHandler.Messages)
```

- [ ] **Step 7: 运行全量编译**

Run: `go build ./...`
Expected: PASS

- [ ] **Step 8: 运行全量测试**

Run: `go test ./...`
Expected: PASS — 所有现有测试 + 新增测试全过

---

### Task 7: 全量回归验证

- [ ] **Step 1: 运行全量测试(含 verbose 输出)**

Run: `go test ./... -v 2>&1 | grep -E "FAIL|PASS|ok|panic" | tail -30`
Expected: 无 FAIL,无 panic

- [ ] **Step 2: 验证删除的文件没有残留引用**

Run: `grep -rn "anthropicChatInvoker\|convertToOpenAIResponse\|convertStreamEvent\|convertUsage\|chat/completions.*Anthropic\|chat_completion.*Anthropic" pkg/ cmd/ internal/ 2>&1 | grep -v "_test.go"`
Expected: 无输出(旧代码引用已全部清理)

- [ ] **Step 3: 验证新 RequestType 在 Engine 中可正确解析**

Run: `grep -n "RequestTypeMessages\|\"messages\"\|/messages" pkg/core/types.go pkg/core/engine_request.go cmd/server/wire/engine.go internal/router/llm.go internal/handler/llm_handler.go 2>&1`
Expected: 所有文件都包含新枚举/路由

- [ ] **Step 4: 验证现有 OpenAI 测试未被破坏**

Run: `go test ./pkg/llm/providers/... -v -run TestOpenAI`
Expected: PASS

Run: `go test ./pkg/filters/... -v -run TestValidateFilter`
Expected: PASS

Run: `go test ./pkg/core/... -v -run TestResolveRequestType`
Expected: PASS

- [ ] **Step 5: (可选)本地启动服务验证**

Run: `go run ./cmd/server &`
然后:

```bash
# 正常 Anthropic 入口(需要配置真实的 anthropic endpoint)
curl -X POST http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-test-key" \
  -d '{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}],"max_tokens":10}'

# 验证 schema 校验:
# 缺 max_tokens
curl -X POST http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-test-key" \
  -d '{"model":"claude-sonnet-4-20250514","messages":[{"role":"user","content":"hi"}]}'
# 期望: 400 + {"type":"error","error":{"type":"invalid_request_error","message":"max_tokens is required"}}
```

验证完成后 stop 服务: `kill %1`
