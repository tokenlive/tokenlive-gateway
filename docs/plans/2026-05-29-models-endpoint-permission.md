# `/v1/models` 权限化改造实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **本仓库不允许自动 commit。** 每个任务末尾的"提交"步骤是**列出待提交文件清单交还给用户**，由用户手动 `git add` / `git commit`。Agent 严禁直接执行 `git commit`。

**Goal:** 将 `GET /v1/models` 从"广播聚合上游"改造为"返回当前 API Key 授权的模型列表"，并保持 OpenAI 标准响应格式。

**Architecture:** Gin Handler 层直接处理（不再走 Engine pipeline）。数据源沿用 `ModelService` 现有的 Redis SET `aigw:user:{userID}:models`。`ConfigManager` 新增 `OwnerOf` 用于填充 `owned_by` 字段。`BroadcastInvoker` 与 `model_list` Pipeline 保留以备 Engine 内部任务复用。

**Tech Stack:** Go 1.21+、Gin、Redis (go-redis v9)、google/wire、testify、alicebob/miniredis、zap

**关联文档：** `docs/specs/2026-05-29-models-endpoint-permission-design.md`

---

## 文件结构与职责

| 路径 | 操作 | 职责 |
|---|---|---|
| `pkg/config/config_manager.go` | 修改（追加方法） | 新增 `OwnerOf(ctx, model) string` |
| `pkg/config/config_manager_test.go` | 修改（追加用例） | `TestOwnerOf_*` |
| `internal/service/model.go` | 修改（追加方法） | 新增 `ListUserModels(ctx, userID) ([]string, error)` |
| `internal/service/model_test.go` | **新建** | `TestListUserModels_*`（基于 miniredis） |
| `internal/handler/llm_handler.go` | 修改（重写 `ListModels`、扩展构造函数） | 引入 `modelLister` / `modelOwner` 接口；权限化 `ListModels` |
| `internal/handler/llm_handler_test.go` | 修改（删旧用例 + 加新用例） | 删除 `TestLLMHandler_ListModels_DelegatesToEngine`；加 5 条新 ListModels 用例 |
| `cmd/server/wire/provider.go` | 修改 | `NewGatewayEngine` 多返回一个 `*config.ConfigManager` |
| `cmd/server/wire/wire.go` | 不动 | wireSet 已含 `NewLLMHandler` 与 `NewGatewayEngine`，签名变化由 wire 自动接住 |
| `cmd/server/wire/wire_gen.go` | 重生成 | `make wire` 后自动同步 |
| `docs/architecture.md` | 修改（文末小节） | `/v1/models` 段落 + `model_list` Pipeline 段落标注"内部能力保留" |
| `README.md` | 修改 | `/v1/models` 端点说明 |

---

## Task 1：ConfigManager 新增 `OwnerOf`

**Files:**

- Modify: `pkg/config/config_manager.go`
- Test: `pkg/config/config_manager_test.go`

- [ ] **Step 1：先看现有 `GetModelProviders` 与测试，理解返回结构**

执行：

```bash
sed -n '23,40p' pkg/config/config_manager.go
sed -n '1,40p' pkg/config/config_manager_test.go
```

`GetModelProviders` 返回 `[]ResolvedModelProvider`，元素带 `ProviderName string` 字段。

- [ ] **Step 2：写失败测试 `TestOwnerOf_YAMLHit`、`TestOwnerOf_RedisHit`、`TestOwnerOf_Miss`**

在 `pkg/config/config_manager_test.go` 末尾追加：

```go
func TestConfigManager_OwnerOf_YAMLHit(t *testing.T) {
 mgr := NewConfigManager(newTestYAMLConfig(), nil, zap.NewNop())
 owner := mgr.OwnerOf(context.Background(), "gpt-4")
 assert.Equal(t, "openai", owner)
}

func TestConfigManager_OwnerOf_RedisHit(t *testing.T) {
 redisSrc := newMockRedisSrc(map[string][]ResolvedModelProvider{
  "claude-3-opus": {{ModelName: "claude-3-opus", ProviderName: "anthropic"}},
 })
 mgr := NewConfigManager(newTestYAMLConfig(), redisSrc, zap.NewNop())
 owner := mgr.OwnerOf(context.Background(), "claude-3-opus")
 assert.Equal(t, "anthropic", owner)
}

func TestConfigManager_OwnerOf_Miss(t *testing.T) {
 mgr := NewConfigManager(newTestYAMLConfig(), nil, zap.NewNop())
 owner := mgr.OwnerOf(context.Background(), "non-existent-model")
 assert.Equal(t, "", owner)
}
```

> 沿用现有测试中的 `newTestYAMLConfig` / `newMockRedisSrc` helper（已存在于 `config_manager_test.go`）。如发现 helper 名字不同，按文件实际命名替换即可（不要新建 helper）。

- [ ] **Step 3：运行测试，确认 FAIL（编译错误：未定义方法）**

执行：

```bash
go test ./pkg/config/ -run TestConfigManager_OwnerOf -v
```

预期：`undefined: (*ConfigManager).OwnerOf` 或测试 FAIL。

- [ ] **Step 4：在 `pkg/config/config_manager.go` 追加 `OwnerOf` 方法**

在 `GetFallbacks` 方法上方插入：

```go
// OwnerOf 返回某个 model 在配置中的归属 provider 名称，未命中返回空字符串。
// 多 provider 时取首个（与 GetModelProviders 顺序一致）。
func (m *ConfigManager) OwnerOf(ctx context.Context, model string) string {
 rps := m.GetModelProviders(ctx, model)
 if len(rps) == 0 {
  return ""
 }
 return rps[0].ProviderName
}
```

- [ ] **Step 5：跑测试确认 PASS**

执行：

```bash
go test ./pkg/config/ -run TestConfigManager_OwnerOf -v
```

预期：`--- PASS: TestConfigManager_OwnerOf_YAMLHit`、`_RedisHit`、`_Miss` 全部通过。

- [ ] **Step 6：跑该包全量测试，确认无回归**

执行：

```bash
go test ./pkg/config/...
```

预期：`ok  tokenlive-gateway/pkg/config`。

- [ ] **Step 7：列出待提交文件，交还用户**

打印：

```
变更文件（请用户手动 git add / git commit）：
  M  pkg/config/config_manager.go
  M  pkg/config/config_manager_test.go

建议 commit 信息：
  feat(config): add ConfigManager.OwnerOf to resolve model owner provider
```

**严禁** Agent 自行 `git commit`。

---

## Task 2：ModelService 新增 `ListUserModels`

**Files:**

- Modify: `internal/service/model.go`
- Test: `internal/service/model_test.go`（新建）

- [ ] **Step 1：检查 `internal/service/apikey_test.go` 的 miniredis 风格，作为本测试模板**

执行：

```bash
sed -n '1,80p' internal/service/apikey_test.go
```

确认 helper 怎么构造 `*service.ApiKeyService`（用 miniredis client 注入），同样的方式构造 `*service.ModelService`。

- [ ] **Step 2：新建 `internal/service/model_test.go` 写 5 条失败测试**

```go
package service

import (
 "context"
 "testing"

 "tokenlive-gateway/pkg/log"

 "github.com/alicebob/miniredis/v2"
 "github.com/redis/go-redis/v9"
 "github.com/spf13/viper"
 "github.com/stretchr/testify/assert"
 "github.com/stretchr/testify/require"
 "go.uber.org/zap"
)

func newTestLogger() *log.Logger {
 z, _ := zap.NewDevelopment()
 return &log.Logger{Logger: z}
}

func newTestModelService(t *testing.T, rdb *redis.Client) *ModelService {
 t.Helper()
 v := viper.New()
 return NewModelService(rdb, newTestLogger(), v)
}

func TestListUserModels_Success(t *testing.T) {
 mr, err := miniredis.Run()
 require.NoError(t, err)
 defer mr.Close()
 rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

 mr.SAdd("aigw:user:u1:models", "gpt-4", "claude-3-opus")

 svc := newTestModelService(t, rdb)
 ids, err := svc.ListUserModels(context.Background(), "u1")
 require.NoError(t, err)
 assert.ElementsMatch(t, []string{"gpt-4", "claude-3-opus"}, ids)
}

func TestListUserModels_KeyMissing(t *testing.T) {
 mr, err := miniredis.Run()
 require.NoError(t, err)
 defer mr.Close()
 rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

 svc := newTestModelService(t, rdb)
 ids, err := svc.ListUserModels(context.Background(), "u-missing")
 require.NoError(t, err)
 assert.Equal(t, []string{}, ids)
}

func TestListUserModels_EmptyUserID(t *testing.T) {
 svc := newTestModelService(t, nil)
 ids, err := svc.ListUserModels(context.Background(), "")
 assert.Nil(t, ids)
 assert.Error(t, err)
}

func TestListUserModels_RedisNil(t *testing.T) {
 svc := newTestModelService(t, nil)
 ids, err := svc.ListUserModels(context.Background(), "u1")
 require.NoError(t, err)
 assert.Equal(t, []string{}, ids)
}

func TestListUserModels_RedisError(t *testing.T) {
 mr, err := miniredis.Run()
 require.NoError(t, err)
 rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
 mr.Close() // 主动关闭模拟连接断

 svc := newTestModelService(t, rdb)
 ids, err := svc.ListUserModels(context.Background(), "u1")
 require.NoError(t, err) // 错误被吞掉，仅日志告警
 assert.Equal(t, []string{}, ids)
}
```

> 如果 `*log.Logger` 结构与上述不符，按 `apikey_test.go` 中 `NewApiKeyService` 同样的 logger 构造方式写。

- [ ] **Step 3：跑测试确认 FAIL（方法未定义）**

执行：

```bash
go test ./internal/service/ -run TestListUserModels -v
```

预期：编译错误 `undefined: (*ModelService).ListUserModels`。

- [ ] **Step 4：在 `internal/service/model.go` 追加 `ListUserModels` 方法**

文件顶部 import 区追加 `"errors"`（如已有则跳过）。在文件末尾插入：

```go
// ListUserModels 返回该用户在 Redis 中授权的模型 ID 列表。
// 严格语义：Key 不存在或 Redis 错误，统一视为 0 个授权模型，避免越权暴露。
func (s *ModelService) ListUserModels(ctx context.Context, userID string) ([]string, error) {
 if userID == "" {
  return nil, errors.New("userID is empty")
 }
 if s.rdb == nil {
  s.logger.Logger.Warn("redis client unavailable, return empty user models",
   zap.String("userID", userID))
  return []string{}, nil
 }
 redisKey := fmt.Sprintf("aigw:user:%s:models", userID)
 members, err := s.rdb.SMembers(ctx, redisKey).Result()
 if err != nil {
  s.logger.Logger.Error("redis SMEMBERS error, return empty list",
   zap.Error(err),
   zap.String("key", redisKey),
   zap.String("userID", userID))
  return []string{}, nil
 }
 return members, nil
}
```

> `errors`、`fmt`、`zap` 在文件中应已 import；若未 import，请补全。

- [ ] **Step 5：跑测试确认 PASS**

执行：

```bash
go test ./internal/service/ -run TestListUserModels -v
```

预期：5 条 PASS。

- [ ] **Step 6：跑该包全量测试**

执行：

```bash
go test ./internal/service/...
```

预期：`ok  tokenlive-gateway/internal/service`。

- [ ] **Step 7：列出待提交文件**

```
变更文件：
  M  internal/service/model.go
  A  internal/service/model_test.go

建议 commit 信息：
  feat(service): add ModelService.ListUserModels to read user-authorized models from Redis
```

---

## Task 3：LLMHandler 引入接口 + 扩展构造函数（不改 ListModels 体）

> 拆成两步：先扩签名+测试编译通过、保持旧逻辑；再换 ListModels 实现。

**Files:**

- Modify: `internal/handler/llm_handler.go`
- Modify: `internal/handler/llm_handler_test.go`
- Modify: `cmd/server/wire/provider.go`
- Modify: `cmd/server/wire/wire_gen.go`（自动）

- [ ] **Step 1：改 `llm_handler.go` 引入接口、扩展字段、扩展构造函数（保留旧 ListModels 体）**

替换 `internal/handler/llm_handler.go` 全文为：

```go
package handler

import (
 "context"

 "tokenlive-gateway/internal/service"
 "tokenlive-gateway/pkg/config"
 "tokenlive-gateway/pkg/core"

 "github.com/gin-gonic/gin"
)

// modelLister 抽象用户授权模型读取，用于测试注入。
type modelLister interface {
 ListUserModels(ctx context.Context, userID string) ([]string, error)
}

// modelOwner 抽象 model→provider 归属解析，用于测试注入。
type modelOwner interface {
 OwnerOf(ctx context.Context, model string) string
}

// LLMHandler LLM 请求处理器（薄 Gin 适配器）
type LLMHandler struct {
 engine        *core.Engine
 modelService  modelLister
 configManager modelOwner
}

// NewLLMHandler 创建 LLM Handler
func NewLLMHandler(
 engine *core.Engine,
 modelService *service.ModelService,
 configManager *config.ConfigManager,
) *LLMHandler {
 return &LLMHandler{
  engine:        engine,
  modelService:  modelService,
  configManager: configManager,
 }
}

// ChatCompletion 处理聊天完成请求
func (h *LLMHandler) ChatCompletion(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}

// CreateEmbedding 处理嵌入请求
func (h *LLMHandler) CreateEmbedding(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}

// ListModels 处理模型列表请求（本任务先保留旧逻辑，下个任务重写）
func (h *LLMHandler) ListModels(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}
```

- [ ] **Step 2：改 `wire/provider.go`：`NewGatewayEngine` 多返回 `*config.ConfigManager`**

定位 `func NewGatewayEngine(...) (*core.Engine, func(), error) {` 改成：

```go
func NewGatewayEngine(
 v *viper.Viper,
 logger *log.Logger,
 modelService *service.ModelService,
 rdb *redis.Client,
) (*core.Engine, *config.ConfigManager, func(), error) {
```

> 注意原函数返回三元组 `(engine, cleanup, err)`，现在变成四元组 `(engine, configMgr, cleanup, err)`。

修改函数体内**所有** `return nil, nil, ...` / `return nil, cleanup, ...` 等返回语句，让长度对应新签名。具体清单：

```bash
grep -n 'return ' cmd/server/wire/provider.go | head -20
```

将每个早返回都补上 `nil` 占位（在 `*core.Engine` 后多塞一个 `nil`），最终成功 `return engine, configMgr, cleanup, nil`。

> 如果 `configMgr` 在某分支可能为 nil（当前实现中只有 `v.IsSet("models")=true` 一条主路径会创建 configMgr），保持其可能为 nil；调用方需要做 nil 防御。但本任务里我们已确认 `v.IsSet("models")` 是必需的（else 分支已 `return ... no models config found`），所以走到末尾时 `configMgr` 必然非 nil。

- [ ] **Step 3：跑 `make wire` 重生成 `wire_gen.go`**

执行：

```bash
make wire
```

预期：`wire_gen.go` 自动更新，`llmHandler := handler.NewLLMHandler(engine, modelService, configManager)`，`NewGatewayEngine` 调用获得 4 个返回值。

> 若没有 `make wire` target，回退到：`cd cmd/server/wire && wire`。

- [ ] **Step 4：修复 `internal/handler/llm_handler_test.go` 的 `setupTestLLMHandler`**

`handler.NewLLMHandler(engine)` 改为 `handler.NewLLMHandler(engine, fakeModelService, fakeConfigManager)`。在 helper 中追加：

```go
// fakeModelService 为 setupTestLLMHandler 提供占位（不参与本任务测试）
type fakeModelService struct{}
func (fakeModelService) ListUserModels(ctx context.Context, userID string) ([]string, error) {
 return []string{}, nil
}

// fakeConfigManager 同上
type fakeConfigManager struct{}
func (fakeConfigManager) OwnerOf(ctx context.Context, model string) string { return "" }
```

但注意：`NewLLMHandler` 接受具体类型 `*service.ModelService` 与 `*config.ConfigManager`，无法用接口 mock。**改用真实构造**：

```go
import (
    "tokenlive-gateway/internal/service"
    "tokenlive-gateway/pkg/config"
)
// 在 setupTestLLMHandler 内：
modelSvc := service.NewModelService(nil, &log.Logger{Logger: logger}, viper.New())
cfgMgr := config.NewConfigManager(&config.GatewayConfig{}, nil, logger)
llmHandler := handler.NewLLMHandler(engine, modelSvc, cfgMgr)
```

> 这样保持 setupTestLLMHandler 编译通过；ListModels 测试用例下个 Task 单独改造。

- [ ] **Step 5：跑全量构建与测试**

执行：

```bash
go build ./...
go test ./internal/handler/... ./cmd/server/...
```

预期：编译通过、测试 PASS（`TestLLMHandler_ChatCompletion_DelegatesToEngine`、`TestLLMHandler_CreateEmbedding_DelegatesToEngine`、`TestLLMHandler_ListModels_DelegatesToEngine` 仍 PASS——本步还没动 ListModels 体）。

- [ ] **Step 6：列出待提交文件**

```
变更文件：
  M  internal/handler/llm_handler.go
  M  internal/handler/llm_handler_test.go
  M  cmd/server/wire/provider.go
  M  cmd/server/wire/wire_gen.go

建议 commit 信息：
  refactor(handler): extend LLMHandler constructor with ModelService and ConfigManager
```

---

## Task 4：重写 `LLMHandler.ListModels` 为权限化实现 + TDD 替换测试

**Files:**

- Modify: `internal/handler/llm_handler.go`
- Modify: `internal/handler/llm_handler_test.go`

- [ ] **Step 1：删除旧测试 `TestLLMHandler_ListModels_DelegatesToEngine`**

打开 `internal/handler/llm_handler_test.go`，删除 125–135 行的 `TestLLMHandler_ListModels_DelegatesToEngine` 整个函数。

- [ ] **Step 2：在 test 文件加入轻量 mock + 5 条新测试**

在文件顶部 import 区追加：

```go
"context"
"encoding/json"
```

在文件末尾追加：

```go
type stubModelLister struct {
 models []string
 err    error
}
func (s stubModelLister) ListUserModels(ctx context.Context, userID string) ([]string, error) {
 return s.models, s.err
}

type stubModelOwner struct {
 owners map[string]string
}
func (s stubModelOwner) OwnerOf(ctx context.Context, model string) string {
 return s.owners[model]
}

// newListModelsHandler 直接构造一个仅注入接口的 handler，绕开真 Engine。
func newListModelsHandler(lister modelLister, owner modelOwner) *handler.LLMHandler {
 // 通过反射或导出的测试构造函数注入。本项目无现成测试构造函数——
 // 改为：在 llm_handler.go 同包里 export 一个 NewLLMHandlerForTest 测试构造函数。
 return handler.NewLLMHandlerForTest(lister, owner)
}

func TestListModels_Authorized_ReturnsUserModels(t *testing.T) {
 gin.SetMode(gin.TestMode)
 lister := stubModelLister{models: []string{"gpt-4"}}
 owner := stubModelOwner{owners: map[string]string{"gpt-4": "openai"}}
 h := handler.NewLLMHandlerForTest(lister, owner)

 r := gin.New()
 r.GET("/v1/models", func(c *gin.Context) {
  c.Set("user_id", "u1")
  h.ListModels(c)
 })

 req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
 w := httptest.NewRecorder()
 r.ServeHTTP(w, req)

 assert.Equal(t, http.StatusOK, w.Code)
 var resp map[string]any
 require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
 assert.Equal(t, "list", resp["object"])
 data := resp["data"].([]any)
 require.Len(t, data, 1)
 m := data[0].(map[string]any)
 assert.Equal(t, "gpt-4", m["id"])
 assert.Equal(t, "model", m["object"])
 assert.Equal(t, "openai", m["owned_by"])
 assert.EqualValues(t, 0, m["created"])
}

func TestListModels_Unauthorized_NoUserID(t *testing.T) {
 gin.SetMode(gin.TestMode)
 h := handler.NewLLMHandlerForTest(stubModelLister{}, stubModelOwner{})

 r := gin.New()
 r.GET("/v1/models", h.ListModels)

 req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
 w := httptest.NewRecorder()
 r.ServeHTTP(w, req)

 assert.Equal(t, http.StatusUnauthorized, w.Code)
 var resp map[string]any
 require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
 errObj := resp["error"].(map[string]any)
 assert.Equal(t, "authentication_error", errObj["type"])
}

func TestListModels_EmptyList(t *testing.T) {
 gin.SetMode(gin.TestMode)
 h := handler.NewLLMHandlerForTest(stubModelLister{models: []string{}}, stubModelOwner{})

 r := gin.New()
 r.GET("/v1/models", func(c *gin.Context) {
  c.Set("user_id", "u-empty")
  h.ListModels(c)
 })

 req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
 w := httptest.NewRecorder()
 r.ServeHTTP(w, req)

 assert.Equal(t, http.StatusOK, w.Code)
 var resp map[string]any
 require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
 assert.Equal(t, "list", resp["object"])
 assert.Empty(t, resp["data"])
}

func TestListModels_OwnerFallback(t *testing.T) {
 gin.SetMode(gin.TestMode)
 lister := stubModelLister{models: []string{"unknown-model"}}
 owner := stubModelOwner{owners: map[string]string{}} // 不命中
 h := handler.NewLLMHandlerForTest(lister, owner)

 r := gin.New()
 r.GET("/v1/models", func(c *gin.Context) {
  c.Set("user_id", "u1")
  h.ListModels(c)
 })

 req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
 w := httptest.NewRecorder()
 r.ServeHTTP(w, req)

 assert.Equal(t, http.StatusOK, w.Code)
 var resp map[string]any
 require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
 data := resp["data"].([]any)
 require.Len(t, data, 1)
 assert.Equal(t, "tokenlive-gateway", data[0].(map[string]any)["owned_by"])
}

// 计数 spy
type engineSpy struct{ called int }
func (s *engineSpy) HandleRequest(http.ResponseWriter, *http.Request) { s.called++ }

func TestListModels_DoesNotCallEngine(t *testing.T) {
 // ListModels 实现里不持有 spy 接口，本用例转为：
 // 验证 Authorized 用例的响应不来自任何 Engine 路径——通过响应体不包含 broadcast 痕迹断言。
 // 直接复用 TestListModels_Authorized_ReturnsUserModels：响应是 handler 直接构造的 JSON。
 gin.SetMode(gin.TestMode)
 lister := stubModelLister{models: []string{"gpt-4"}}
 owner := stubModelOwner{owners: map[string]string{"gpt-4": "openai"}}
 h := handler.NewLLMHandlerForTest(lister, owner)

 r := gin.New()
 r.GET("/v1/models", func(c *gin.Context) {
  c.Set("user_id", "u1")
  h.ListModels(c)
 })

 req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
 w := httptest.NewRecorder()
 r.ServeHTTP(w, req)

 // ListModels 走 handler 直接路径：响应必然由 c.JSON 输出
 // （Engine 走 SSE/InterceptWriter 路径，响应头与此不同）
 assert.Equal(t, http.StatusOK, w.Code)
 assert.Contains(t, w.Header().Get("Content-Type"), "application/json")
}
```

> `NewLLMHandlerForTest` 是测试用导出构造函数，下一步会加。

- [ ] **Step 3：在 `llm_handler.go` 添加测试构造函数 + 重写 `ListModels` 实现**

替换 `internal/handler/llm_handler.go` 全文为：

```go
package handler

import (
 "context"
 "net/http"

 "tokenlive-gateway/internal/service"
 "tokenlive-gateway/pkg/config"
 "tokenlive-gateway/pkg/core"

 "github.com/gin-gonic/gin"
)

// modelLister 抽象用户授权模型读取，用于测试注入。
type modelLister interface {
 ListUserModels(ctx context.Context, userID string) ([]string, error)
}

// modelOwner 抽象 model→provider 归属解析，用于测试注入。
type modelOwner interface {
 OwnerOf(ctx context.Context, model string) string
}

// LLMHandler LLM 请求处理器（薄 Gin 适配器）
type LLMHandler struct {
 engine        *core.Engine
 modelService  modelLister
 configManager modelOwner
}

// NewLLMHandler 创建 LLM Handler（生产用，注入具体类型）。
func NewLLMHandler(
 engine *core.Engine,
 modelService *service.ModelService,
 configManager *config.ConfigManager,
) *LLMHandler {
 return &LLMHandler{
  engine:        engine,
  modelService:  modelService,
  configManager: configManager,
 }
}

// NewLLMHandlerForTest 仅供测试，注入接口；不需要 Engine。
func NewLLMHandlerForTest(modelService modelLister, configManager modelOwner) *LLMHandler {
 return &LLMHandler{
  engine:        nil,
  modelService:  modelService,
  configManager: configManager,
 }
}

// ChatCompletion 处理聊天完成请求
func (h *LLMHandler) ChatCompletion(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}

// CreateEmbedding 处理嵌入请求
func (h *LLMHandler) CreateEmbedding(c *gin.Context) {
 h.engine.HandleRequest(c.Writer, c.Request)
}

// ListModels 返回当前 API Key 授权的模型列表（不再调用 Engine）。
func (h *LLMHandler) ListModels(c *gin.Context) {
 userID := c.GetString("user_id")
 if userID == "" {
  c.JSON(http.StatusUnauthorized, gin.H{
   "error": gin.H{
    "message": "Missing or invalid API key",
    "type":    "authentication_error",
   },
  })
  return
 }

 ids, _ := h.modelService.ListUserModels(c.Request.Context(), userID)

 data := make([]gin.H, 0, len(ids))
 for _, id := range ids {
  owner := h.configManager.OwnerOf(c.Request.Context(), id)
  if owner == "" {
   owner = "tokenlive-gateway"
  }
  data = append(data, gin.H{
   "id":       id,
   "object":   "model",
   "created":  0,
   "owned_by": owner,
  })
 }

 c.JSON(http.StatusOK, gin.H{
  "object": "list",
  "data":   data,
 })
}
```

- [ ] **Step 4：跑测试确认 5 条新用例 PASS、其余用例不回归**

执行：

```bash
go test ./internal/handler/... -v
```

预期：

- `TestListModels_Authorized_ReturnsUserModels` PASS
- `TestListModels_Unauthorized_NoUserID` PASS
- `TestListModels_EmptyList` PASS
- `TestListModels_OwnerFallback` PASS
- `TestListModels_DoesNotCallEngine` PASS
- `TestLLMHandler_ChatCompletion_DelegatesToEngine` PASS
- `TestLLMHandler_CreateEmbedding_DelegatesToEngine` PASS
- 旧 `TestLLMHandler_ListModels_DelegatesToEngine` 已删除

- [ ] **Step 5：跑仓库全量测试，确认无回归**

执行：

```bash
go build ./...
go test ./... -short
```

预期：全部通过。

- [ ] **Step 6：列出待提交文件**

```
变更文件：
  M  internal/handler/llm_handler.go
  M  internal/handler/llm_handler_test.go

建议 commit 信息：
  feat(handler): rewrite /v1/models to return user-authorized models, drop Engine delegation
```

---

## Task 5：文档同步（architecture.md / README.md）

**Files:**

- Modify: `docs/architecture.md`
- Modify: `README.md`

> 不写 ADR/CONTEXT.md（规模小、决策已记录在 spec 文档），仅同步面向用户的文档。

- [ ] **Step 1：更新 README 中 `/v1/models` 描述**

定位 `README.md` 中 `/v1/models` 端点说明。如果原文是"列出可用模型"或"聚合所有上游模型"，改为：

```
- `GET /v1/models` —— 返回当前 API Key 授权的模型列表（OpenAI 标准格式）。
  - 数据源：Redis SET `aigw:user:{userID}:models`。
  - 未鉴权返回 401；用户未授权任何模型返回 `{object:"list", data:[]}`。
```

- [ ] **Step 2：更新 architecture.md 中 `/v1/models` 章节**

定位 architecture.md 中描述 `model_list` Pipeline / `BroadcastInvoker` / `/v1/models` 路由的小节。在该小节末尾追加一段说明：

```markdown
> **接入方式**（v2.6 起）：`GET /v1/models` HTTP 路由由 `LLMHandler.ListModels` 直接处理，
> 数据源为 Redis SET `aigw:user:{userID}:models`，**不再调用 `engine.HandleRequest`**。
>
> `model_list` Pipeline 与 `BroadcastInvoker` 作为 Engine 内部能力保留，
> 可用于"网关侧聚合上游 /models 配置同步"等内部任务。
```

- [ ] **Step 3：列出待提交文件**

```
变更文件：
  M  README.md
  M  docs/architecture.md

建议 commit 信息：
  docs: update /v1/models description to reflect permission-based listing
```

---

## 完成验证

执行最后一次完整验证：

```bash
make build
make test
```

预期：

- 二进制生成成功
- 全量测试通过
- 覆盖率报告中 `internal/service/model.go` 与 `internal/handler/llm_handler.go` 覆盖率不下降

手工冒烟（可选）：

```bash
# 1. 启动服务
make bootstrap

# 2. 用未鉴权请求测 401
curl -i http://localhost:8080/v1/models
# 期望：401 authentication_error

# 3. 写入测试用户授权
redis-cli SADD aigw:user:test-user:models gpt-4 claude-3-opus
redis-cli HSET aigw:apikey:sk-test user_id test-user status 1 quota -1 expires_at 0

# 4. 用合法 API Key 请求
curl -s http://localhost:8080/v1/models -H 'Authorization: Bearer sk-test' | jq
# 期望：{object:"list", data:[{id:"gpt-4",...,owned_by:"openai"}, {id:"claude-3-opus",...,owned_by:"anthropic"}]}
```

---

## 自检（writing-plans skill 要求）

| 项 | 结果 |
|---|---|
| **Spec 覆盖** | D1（数据源 Redis SET）→ Task 2；D2（401）→ Task 4 Step 3；D3（Key 不存在 = 空）→ Task 2 Step 2；D4（Redis 错误 = 空+日志）→ Task 2 Step 2；D5（OpenAI 格式 + owned_by=provider）→ Task 1 + Task 4；D6（Handler 直处理）→ Task 4 Step 3 |
| **Placeholder 扫描** | 无 TBD/TODO；每步均含可执行命令或完整代码 |
| **类型一致性** | `modelLister` / `modelOwner` 在 Task 3、4 与测试中一致；`ListUserModels` 签名 `(ctx, userID) ([]string, error)` 全文一致；`OwnerOf` 签名 `(ctx, model) string` 全文一致 |
| **删除旧代码/测试** | Task 4 Step 1 显式删除 `TestLLMHandler_ListModels_DelegatesToEngine`；BroadcastInvoker / model_list Pipeline 按设计 §5.5 保留（非"无用"） |
| **不自动 commit** | 每个 Task 末尾仅"列出文件清单交还用户" |
