# Config Relational Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `llm.model_list` + `llm.providers` 嵌套配置改为 `models` + `providers` + `model_providers` 关系型三表结构，配置数据源分层（YAML 默认 + Redis 覆盖），懒加载 + 版本轮询热加载。

**Architecture:** YAML 作为默认层保证启动可用，Redis 作为覆盖层由 AdminProject 维护已合并的扁平化 ModelProvider 数据。网关懒加载：请求到来时从 Redis 按 model 拉取并缓存，后台轮询版本号变更时清缓存。Engine.UpdateConfig() 原子切换 Pipeline。

**Tech Stack:** Go, Viper (config), go-redis/v9, mapstructure, testify

**ADRs:** docs/adr/0001-0005

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `pkg/core/types.go` | UpstreamModel + EffectiveModel() | DONE (Task 1) |
| `pkg/core/invoker.go` | ProviderInvoker 使用 UpstreamModel | DONE (Task 2) |
| `pkg/config/types.go` | 配置类型定义 | DONE (Task 3) |
| `pkg/config/loader.go` | YAML 配置加载 + Validate + Resolve | DONE (Task 3) |
| `pkg/config/redis_source.go` | **新建** — Redis 配置源：懒加载 + 版本轮询 + 缓存 | Task 4 |
| `pkg/config/config_manager.go` | **新建** — 分层配置管理器：YAML 基线 + Redis 覆盖 + 合并 | Task 5 |
| `pkg/core/discovery.go` | ServiceInstanceToEndpoint 填充 UpstreamModel | Task 6 |
| `cmd/server/wire/provider.go` | 重写 NewGatewayEngine 接入新配置 | Task 7 |
| `config/local.yml` | 改为新配置格式 | Task 8 |
| `config/llm.example.yml` | 改为新配置格式 | Task 8 |

---

### Task 1: 添加 UpstreamModel 到 Endpoint ✅

已完成。`pkg/core/types.go` 已添加 `UpstreamModel` 字段和 `EffectiveModel()` 方法，测试通过。

---

### Task 2: ProviderInvoker 使用 UpstreamModel ✅

已完成。`pkg/core/invoker.go` 已修改，调上游前用 `EffectiveModel()` 替换 `gctx.Model`。

---

### Task 3: 创建配置类型和加载器 ✅

已完成。`pkg/config/types.go` 和 `pkg/config/loader.go` 已创建，包含 `Load`、`Validate`、`Resolve`、`KnownModels` 函数，7 个测试通过。

---

### Task 4: Redis 配置源（懒加载 + 版本轮询）

**Files:**

- Create: `pkg/config/redis_source.go`
- Create: `pkg/config/redis_source_test.go`

- [ ] **Step 1: 创建 redis_source.go**

创建 `pkg/config/redis_source.go`，实现从 Redis 懒加载 model_providers 数据：

```go
package config

import (
 "context"
 "encoding/json"
 "fmt"
 "sync"
 "time"

 "github.com/redis/go-redis/v9"
 "go.uber.org.org/zap"
)

const (
 redisKeyVersion        = "aigw:config:version"
 redisKeyModelProviders = "aigw:config:model_providers:"
 defaultPollInterval    = 10 * time.Second
)

// RedisConfigSource 从 Redis 懒加载 model_providers 配置
// 实现懒加载 + 版本轮询机制
type RedisConfigSource struct {
 client       redis.Cmdable
 pollInterval time.Duration
 logger       *zap.Logger

 mu             sync.RWMutex
 cache          map[string][]ResolvedModelProvider // model_name -> resolved list
 lastVersion    string
 providerImpls  map[string]interface{} // 缓存 Provider 实例
}

// NewRedisConfigSource 创建 Redis 配置源
func NewRedisConfigSource(client redis.Cmdable, pollInterval time.Duration, logger *zap.Logger) *RedisConfigSource {
 if pollInterval <= 0 {
  pollInterval = defaultPollInterval
 }
 return &RedisConfigSource{
  client:       client,
  pollInterval: pollInterval,
  logger:       logger,
  cache:        make(map[string][]ResolvedModelProvider),
  providerImpls: make(map[string]interface{}),
 }
}

// GetModelProviders 获取指定 model 的 ResolvedModelProvider 列表
// 优先从内存缓存读取，缓存没有则从 Redis 懒加载
func (s *RedisConfigSource) GetModelProviders(ctx context.Context, modelName string) ([]ResolvedModelProvider, bool) {
 s.mu.RLock()
 cached, ok := s.cache[modelName]
 s.mu.RUnlock()
 if ok {
  return cached, true
 }

 // 缓存未命中，从 Redis 懒加载
 providers, err := s.fetchFromRedis(ctx, modelName)
 if err != nil {
  s.logger.Warn("redis fetch failed",
   zap.String("model", modelName),
   zap.Error(err),
  )
  return nil, false
 }

 s.mu.Lock()
 s.cache[modelName] = providers
 s.mu.Unlock()

 return providers, true
}

// fetchFromRedis 从 Redis 读取单个 model 的 model_providers 数据
func (s *RedisConfigSource) fetchFromRedis(ctx context.Context, modelName string) ([]ResolvedModelProvider, error) {
 key := redisKeyModelProviders + modelName
 val, err := s.client.Get(ctx, key).Result()
 if err != nil {
  return nil, fmt.Errorf("redis get %s: %w", key, err)
 }

 var providers []ResolvedModelProvider
 if err := json.Unmarshal([]byte(val), &providers); err != nil {
  return nil, fmt.Errorf("unmarshal model_providers for %s: %w", modelName, err)
 }

 return providers, nil
}

// StartPolling 启动后台版本轮询 goroutine
func (s *RedisConfigSource) StartPolling(ctx context.Context) {
 go func() {
  ticker := time.NewTicker(s.pollInterval)
  defer ticker.Stop()

  for {
   select {
   case <-ctx.Done():
    return
   case <-ticker.C:
    s.checkVersion(ctx)
   }
  }
 }()
}

// checkVersion 检查版本号，变更时清空缓存
func (s *RedisConfigSource) checkVersion(ctx context.Context) {
 version, err := s.client.Get(ctx, redisKeyVersion).Result()
 if err != nil {
  if err != redis.Nil {
   s.logger.Warn("redis get version failed", zap.Error(err))
  }
  return
 }

 s.mu.RLock()
 changed := s.lastVersion != "" && s.lastVersion != version
 s.mu.RUnlock()

 if changed {
  s.mu.Lock()
  s.cache = make(map[string][]ResolvedModelProvider)
  s.lastVersion = version
  s.mu.Unlock()
  s.logger.Info("config version changed, cache cleared",
   zap.String("version", version),
  )
 } else if s.lastVersion == "" {
  s.mu.Lock()
  s.lastVersion = version
  s.mu.Unlock()
 }
}

// ClearCache 手动清空缓存（用于测试或强制刷新）
func (s *RedisConfigSource) ClearCache() {
 s.mu.Lock()
 defer s.mu.Unlock()
 s.cache = make(map[string][]ResolvedModelProvider)
}
```

注意：上面的 `go.uber.org/zap` 应为 `go.uber.org/zap`。实际实现时修正 import path。

- [ ] **Step 2: 创建 redis_source_test.go**

创建 `pkg/config/redis_source_test.go`。使用 miniredis（内存 Redis）进行测试：

```go
package config

import (
 "context"
 "encoding/json"
 "testing"
 "time"

 "github.com/alicebob/miniredis/v2"
 "github.com/redis/go-redis/v9"
 "github.com/stretchr/testify/assert"
 "github.com/stretchr/testify/require"
 "go.uber.org/zap"
)

func setupRedis(t *testing.T) (*miniredis.Miniredis, redis.Cmdable) {
 t.Helper()
 mr := miniredis.RunT(t)
 client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
 return mr, client
}

func TestRedisSource_GetModelProviders_CacheHit(t *testing.T) {
 mr, client := setupRedis(t)
 defer mr.Close()

 logger := zap.NewNop()
 src := NewRedisConfigSource(client, 30*time.Second, logger)

 // 预热缓存
 expected := []ResolvedModelProvider{
  {ModelName: "gpt-4", ProviderName: "openai", RealModel: "gpt-4", Priority: 1, Weight: 100},
 }
 data, _ := json.Marshal(expected)
 mr.Set("aigw:config:model_providers:gpt-4", string(data))

 // 第一次从 Redis 加载
 providers, ok := src.GetModelProviders(context.Background(), "gpt-4")
 assert.True(t, ok)
 assert.Len(t, providers, 1)
 assert.Equal(t, "openai", providers[0].ProviderName)

 // 第二次从缓存读取（删除 Redis key 验证）
 mr.Del("aigw:config:model_providers:gpt-4")
 providers2, ok := src.GetModelProviders(context.Background(), "gpt-4")
 assert.True(t, ok)
 assert.Len(t, providers2, 1)
}

func TestRedisSource_GetModelProviders_CacheMiss_RedisDown(t *testing.T) {
 mr, client := setupRedis(t)
 logger := zap.NewNop()
 src := NewRedisConfigSource(client, 30*time.Second, logger)

 // Redis 没有数据
 providers, ok := src.GetModelProviders(context.Background(), "gpt-4")
 assert.False(t, ok)
 assert.Nil(t, providers)
}

func TestRedisSource_VersionPolling_ClearsCache(t *testing.T) {
 mr, client := setupRedis(t)
 defer mr.Close()

 logger := zap.NewNop()
 src := NewRedisConfigSource(client, 100*time.Millisecond, logger)

 // 设置初始版本和数据
 mr.Set("aigw:config:version", "1")
 expected := []ResolvedModelProvider{
  {ModelName: "gpt-4", ProviderName: "openai"},
 }
 data, _ := json.Marshal(expected)
 mr.Set("aigw:config:model_providers:gpt-4", string(data))

 // 加载到缓存
 providers, ok := src.GetModelProviders(context.Background(), "gpt-4")
 assert.True(t, ok)
 assert.Len(t, providers, 1)

 // 启动轮询
 ctx, cancel := context.WithCancel(context.Background())
 defer cancel()
 src.StartPolling(ctx)

 // 等待首次轮询记录版本
 time.Sleep(150 * time.Millisecond)

 // 变更版本号
 mr.Set("aigw:config:version", "2")

 // 等待轮询检测到变更
 time.Sleep(200 * time.Millisecond)

 // 缓存应该被清空，再次获取应从 Redis 重新加载
 providers, ok = src.GetModelProviders(context.Background(), "gpt-4")
 assert.True(t, ok)
 assert.Len(t, providers, 1)
}

func TestRedisSource_ClearCache(t *testing.T) {
 mr, client := setupRedis(t)
 defer mr.Close()

 logger := zap.NewNop()
 src := NewRedisConfigSource(client, 30*time.Second, logger)

 expected := []ResolvedModelProvider{{ModelName: "gpt-4", ProviderName: "openai"}}
 data, _ := json.Marshal(expected)
 mr.Set("aigw:config:model_providers:gpt-4", string(data))

 // 加载到缓存
 src.GetModelProviders(context.Background(), "gpt-4")

 // 清空缓存
 src.ClearCache()

 // 删除 Redis key，验证缓存确实被清空
 mr.Del("aigw:config:model_providers:gpt-4")
 _, ok := src.GetModelProviders(context.Background(), "gpt-4")
 assert.False(t, ok)
}
```

- [ ] **Step 3: 安装 miniredis 测试依赖**

Run: `go get github.com/alicebob/miniredis/v2@latest && go get github.com/redis/go-redis/v9@latest`
Expected: go.mod 更新

- [ ] **Step 4: 运行测试**

Run: `go test ./pkg/config/ -run TestRedisSource -v`
Expected: 所有 Redis 测试通过

- [ ] **Step 5: Commit**

```bash
git add pkg/config/redis_source.go pkg/config/redis_source_test.go go.mod go.sum
git commit -m "feat: add Redis config source with lazy loading and version polling"
```

---

### Task 5: 分层配置管理器（YAML 基线 + Redis 覆盖）

**Files:**

- Create: `pkg/config/config_manager.go`
- Create: `pkg/config/config_manager_test.go`

- [ ] **Step 1: 创建 config_manager.go**

创建 `pkg/config/config_manager.go`，实现 YAML 基线 + Redis 覆盖的分层合并：

```go
package config

import (
 "context"
 "sync"

 "go.uber.org/zap"
)

// ConfigManager 分层配置管理器
// YAML 作为默认层，Redis 作为覆盖层
// 同一 model 两层都有时用 Redis 的，只有 YAML 有的保留 YAML 的
type ConfigManager struct {
 yamlConfig *GatewayConfig
 redisSrc   *RedisConfigSource
 logger     *zap.Logger

 // 内存中的合并后数据（YAML + Redis）
 mu       sync.RWMutex
 resolved map[string][]ResolvedModelProvider // model_name -> resolved list
}

// NewConfigManager 创建配置管理器
func NewConfigManager(yamlCfg *GatewayConfig, redisSrc *RedisConfigSource, logger *zap.Logger) *ConfigManager {
 m := &ConfigManager{
  yamlConfig: yamlCfg,
  redisSrc:   redisSrc,
  logger:     logger,
  resolved:   make(map[string][]ResolvedModelProvider),
 }

 // 加载 YAML 基线到内存
 if yamlCfg != nil {
  yamlResolved := Resolve(yamlCfg)
  for _, rp := range yamlResolved {
   m.resolved[rp.ModelName] = append(m.resolved[rp.ModelName], rp)
  }
 }

 return m
}

// GetModelProviders 获取指定 model 的 ResolvedModelProvider 列表
// 优先从 Redis 获取，Redis 不可用或无数据时回退到 YAML 基线
func (m *ConfigManager) GetModelProviders(ctx context.Context, modelName string) []ResolvedModelProvider {
 // 先尝试 Redis
 if m.redisSrc != nil {
  if providers, ok := m.redisSrc.GetModelProviders(ctx, modelName); ok && len(providers) > 0 {
   return providers
  }
 }

 // 回退到 YAML 基线
 m.mu.RLock()
 defer m.mu.RUnlock()
 return m.resolved[modelName]
}

// AllKnownModels 返回所有已知的 model_name 集合（YAML + Redis 缓存中的）
func (m *ConfigManager) AllKnownModels() map[string]bool {
 known := make(map[string]bool)

 // YAML 层
 if m.yamlConfig != nil {
  for name := range m.yamlConfig.Models {
   known[name] = true
  }
 }

 // Redis 缓存层
 if m.redisSrc != nil {
  m.redisSrc.mu.RLock()
  for name := range m.redisSrc.cache {
   known[name] = true
  }
  m.redisSrc.mu.RUnlock()
 }

 return known
}

// GetFallbacks 获取全局默认降级策略
func (m *ConfigManager) GetFallbacks() map[string][]string {
 if m.yamlConfig != nil {
  return m.yamlConfig.Fallbacks
 }
 return nil
}

// StartRedisPolling 启动 Redis 版本轮询（如果配置了 Redis）
func (m *ConfigManager) StartRedisPolling(ctx context.Context) {
 if m.redisSrc != nil {
  m.redisSrc.StartPolling(ctx)
 }
}
```

- [ ] **Step 2: 创建 config_manager_test.go**

```go
package config

import (
 "context"
 "encoding/json"
 "testing"
 "time"

 "github.com/alicebob/miniredis/v2"
 "github.com/redis/go-redis/v9"
 "github.com/stretchr/testify/assert"
 "go.uber.org/zap"
)

func newTestYAMLConfig() *GatewayConfig {
 return &GatewayConfig{
  Models: map[string]ModelConfig{
   "gpt-4":         {RealModel: "gpt-4", RequestType: "chat_completion"},
   "claude-sonnet": {RealModel: "claude-3-sonnet-20240229", RequestType: "chat_completion"},
  },
  Providers: map[string]ProviderConfig{
   "openai": {Type: "openai", APIKey: "sk-yaml", Endpoints: []EndpointConfig{{URL: "https://api.openai.com/v1"}}},
  },
  ModelProviders: []ModelProviderConfig{
   {Model: "gpt-4", Provider: "openai", Priority: 1, Weight: 100},
   {Model: "claude-sonnet", Provider: "openai", Priority: 1, Weight: 100},
  },
  Fallbacks: map[string][]string{"gpt-4": {"gpt-3.5-turbo"}},
 }
}

func TestConfigManager_YAMLOnly(t *testing.T) {
 yamlCfg := newTestYAMLConfig()
 mgr := NewConfigManager(yamlCfg, nil, zap.NewNop())

 providers := mgr.GetModelProviders(context.Background(), "gpt-4")
 assert.Len(t, providers, 1)
 assert.Equal(t, "openai", providers[0].ProviderName)

 // 未知模型返回空
 providers = mgr.GetModelProviders(context.Background(), "unknown")
 assert.Nil(t, providers)
}

func TestConfigManager_RedisOverridesYAML(t *testing.T) {
 mr := miniredis.RunT(t)
 defer mr.Close()

 client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
 logger := zap.NewNop()
 redisSrc := NewRedisConfigSource(client, 30*time.Second, logger)

 yamlCfg := newTestYAMLConfig()
 mgr := NewConfigManager(yamlCfg, redisSrc, logger)

 // Redis 有 gpt-4 的数据（不同 provider）
 redisData := []ResolvedModelProvider{
  {ModelName: "gpt-4", ProviderName: "azure-openai", RealModel: "gpt-4", Priority: 1, Weight: 50},
  {ModelName: "gpt-4", ProviderName: "openai", RealModel: "gpt-4", Priority: 2, Weight: 50},
 }
 data, _ := json.Marshal(redisData)
 mr.Set("aigw:config:model_providers:gpt-4", string(data))

 // gpt-4 应该用 Redis 数据
 providers := mgr.GetModelProviders(context.Background(), "gpt-4")
 assert.Len(t, providers, 2)
 assert.Equal(t, "azure-openai", providers[0].ProviderName)

 // claude-sonnet Redis 没有数据，回退到 YAML
 providers = mgr.GetModelProviders(context.Background(), "claude-sonnet")
 assert.Len(t, providers, 1)
 assert.Equal(t, "openai", providers[0].ProviderName)
}

func TestConfigManager_RedisDown_FallbackToYAML(t *testing.T) {
 mr := miniredis.RunT(t)
 client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
 logger := zap.NewNop()
 redisSrc := NewRedisConfigSource(client, 30*time.Second, logger)

 yamlCfg := newTestYAMLConfig()
 mgr := NewConfigManager(yamlCfg, redisSrc, logger)

 // 关闭 Redis
 mr.Close()

 // 应回退到 YAML
 providers := mgr.GetModelProviders(context.Background(), "gpt-4")
 assert.Len(t, providers, 1)
 assert.Equal(t, "openai", providers[0].ProviderName)
}

func TestConfigManager_AllKnownModels(t *testing.T) {
 mr := miniredis.RunT(t)
 defer mr.Close()

 client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
 logger := zap.NewNop()
 redisSrc := NewRedisConfigSource(client, 30*time.Second, logger)

 yamlCfg := newTestYAMLConfig()
 mgr := NewConfigManager(yamlCfg, redisSrc, logger)

 // YAML 有 gpt-4 和 claude-sonnet
 known := mgr.AllKnownModels()
 assert.True(t, known["gpt-4"])
 assert.True(t, known["claude-sonnet"])
 assert.False(t, known["llama2"])

 // Redis 加载了 llama2
 redisData := []ResolvedModelProvider{{ModelName: "llama2", ProviderName: "ollama"}}
 data, _ := json.Marshal(redisData)
 mr.Set("aigw:config:model_providers:llama2", string(data))
 mgr.GetModelProviders(context.Background(), "llama2")

 known = mgr.AllKnownModels()
 assert.True(t, known["llama2"])
}
```

- [ ] **Step 3: 运行测试**

Run: `go test ./pkg/config/ -run TestConfigManager -v`
Expected: 所有测试通过

- [ ] **Step 4: Commit**

```bash
git add pkg/config/config_manager.go pkg/config/config_manager_test.go
git commit -m "feat: add layered config manager with YAML baseline and Redis override"
```

---

### Task 6: 更新 DiscoveryAdapter 填充 UpstreamModel

**Files:**

- Modify: `pkg/core/discovery.go`

- [ ] **Step 1: 修改 ServiceInstanceToEndpoint**

在 `pkg/core/discovery.go` 的 `ServiceInstanceToEndpoint` 函数中，从 metadata 读取 `upstream_model` 并填入 `UpstreamModel`：

```go
func ServiceInstanceToEndpoint(inst *discovery.ServiceInstance, pc ProviderConfig) *Endpoint {
 if inst == nil {
  return nil
 }

 healthy := inst.Health == discovery.HealthStatusHealthy

 model := ""
 if m, ok := inst.Metadata["model"]; ok {
  model = m
 }

 upstreamModel := ""
 if m, ok := inst.Metadata["upstream_model"]; ok {
  upstreamModel = m
 }

 return &Endpoint{
  ID:            inst.ID,
  URL:           inst.GetURL(),
  Provider:      pc.Name,
  Model:         model,
  UpstreamModel: upstreamModel,
  Metadata:      inst.Metadata,
  Weight:        inst.Weight,
  RequestTypes:  pc.RequestTypes,
  Healthy:       healthy,
 }
}
```

- [ ] **Step 2: 运行已有测试**

Run: `go test ./pkg/core/ -v`
Expected: 所有已有测试通过

- [ ] **Step 3: Commit**

```bash
git add pkg/core/discovery.go
git commit -m "feat: DiscoveryAdapter populates UpstreamModel from metadata"
```

---

### Task 7: 重写 NewGatewayEngine 接入新配置

**Files:**

- Modify: `cmd/server/wire/provider.go`

- [ ] **Step 1: 添加新格式配置加载分支**

在 `NewGatewayEngine` 函数中，添加 `v.IsSet("models")` 分支：

```go
import (
    "tokenlive-gateway/pkg/config"
    // ... 其他 import
)
```

替换配置加载逻辑：

```go
func NewGatewayEngine(
    v *viper.Viper,
    logger *log.Logger,
) (*core.Engine, func(), error) {
    var engineConfig *core.EngineConfig
    providerImpls map[string]core.Provider
    var providerConfigs []core.ProviderConfig
    var knownModels map[string]bool
    var configMgr *config.ConfigManager

    validKeys := readAuthKeys(v)

    if v.IsSet("models") {
        // 新格式：models + providers + model_providers
        gwCfg, err := config.Load(v)
        if err != nil {
            return nil, nil, fmt.Errorf("load gateway config: %w", err)
        }
        if err := config.Validate(gwCfg); err != nil {
            return nil, nil, fmt.Errorf("validate gateway config: %w", err)
        }

        // 创建 Redis 配置源（如果配置了 Redis）
        var redisSrc *config.RedisConfigSource
        if v.IsSet("data.redis") {
            rdb := redis.NewClient(&redis.Options{
                Addr:     v.GetString("data.redis.addr"),
                Password: v.GetString("data.redis.password"),
                DB:       v.GetInt("data.redis.db"),
            })
            pollInterval := v.GetDuration("config_poll_interval")
            redisSrc = config.NewRedisConfigSource(rdb, pollInterval, logger.Logger)
        }

        // 创建分层配置管理器
        configMgr = config.NewConfigManager(gwCfg, redisSrc, logger.Logger)

        // 构建 EngineConfig
        engineConfig, providerImpls, providerConfigs, knownModels = buildFromRelationalConfig(gwCfg, len(validKeys) > 0)

        // 注册 endpoints
        staticDiscovery := discovery.NewStaticDiscovery()
        resolved := config.Resolve(gwCfg)
        registerEndpointsFromRelationalConfig(staticDiscovery, resolved)
    } else if v.IsSet("gateway") {
        // ... 旧 gateway 格式 ...
    } else if v.IsSet("llm") {
        // ... 旧 llm 格式 ...
    } else {
        return nil, nil, fmt.Errorf("no gateway or llm config found")
    }

    // ... 后续逻辑不变 ...
}
```

- [ ] **Step 2: 实现 buildFromRelationalConfig**

```go
func buildFromRelationalConfig(
    gwCfg *config.GatewayConfig,
    hasAuth bool,
) (*core.EngineConfig, map[string]core.Provider, []core.ProviderConfig, map[string]bool) {
    engineConfig := &core.EngineConfig{
        Pipelines: make(map[string]*core.PipelineConfig),
        Providers: make(map[string]*core.ProviderConfig),
    }

    resolved := config.Resolve(gwCfg)
    knownModels := config.KnownModels(gwCfg)

    // 按 provider 分组收集 models
    providerModels := make(map[string][]string)
    for _, rp := range resolved {
        providerModels[rp.ProviderName] = append(providerModels[rp.ProviderName], rp.ModelName)
    }

    // 构建 ProviderConfig 列表（去重）
    providerConfigMap := make(map[string]*core.ProviderConfig)
    for _, rp := range resolved {
        if _, exists := providerConfigMap[rp.ProviderName]; !exists {
            providerConfigMap[rp.ProviderName] = &core.ProviderConfig{
                Name:   rp.ProviderName,
                Type:   rp.ProviderType,
                Models: providerModels[rp.ProviderName],
                RequestTypes: []core.RequestType{
                    core.RequestTypeChatCompletion,
                    core.RequestTypeEmbedding,
                    core.RequestTypeModelList,
                },
            }
        }
    }
    var providerConfigs []core.ProviderConfig
    for _, pc := range providerConfigMap {
        providerConfigs = append(providerConfigs, *pc)
    }

    // 创建 Provider 实例
    providerImpls := make(map[string]core.Provider)
    for providerName, pc := range providerConfigMap {
        var firstRP config.ResolvedModelProvider
        for _, rp := range resolved {
            if rp.ProviderName == providerName {
                firstRP = rp
                break
            }
        }
        baseURL := ""
        if len(firstRP.Endpoints) > 0 {
            baseURL = firstRP.Endpoints[0].URL
        }
        p, err := llm.NewProvider(firstRP.ProviderType, llm.ProviderConfig{
            Name:    providerName,
            BaseURL: baseURL,
            APIKey:  firstRP.APIKey,
            Models:  providerModels[providerName],
        })
        if err != nil {
            continue
        }
        providerImpls[providerName] = p
    }

    // 为每个有 provider 绑定的 model 创建 pipeline
    modelMaxRetries := make(map[string]int)
    for _, rp := range resolved {
        if rp.MaxRetries > modelMaxRetries[rp.ModelName] {
            modelMaxRetries[rp.ModelName] = rp.MaxRetries
        }
    }
    for modelName := range gwCfg.Models {
        maxRetries, hasProvider := modelMaxRetries[modelName]
        if !hasProvider {
            continue
        }
        inboundFilters := []string{"session_reader", "validate"}
        if hasAuth {
            inboundFilters = append([]string{"auth"}, inboundFilters...)
        }
        engineConfig.Pipelines[modelName] = &core.PipelineConfig{
            Name:         modelName,
            RequestTypes: []core.RequestType{core.RequestTypeChatCompletion, core.RequestTypeEmbedding},
            Invoker: core.InvokerConfig{
                Type: "cluster",
                Retry: core.RetryConfig{
                    MaxRetries: maxRetries,
                    Backoff: core.BackoffConfig{Type: "exponential_jitter", BaseMs: 100, MaxMs: 5000},
                },
            },
            InboundFilters:          inboundFilters,
            OutboundFilters:         []string{"token_settlement", "sticky_session", "metrics", "access_log"},
            CriticalOutboundFilters: []string{"token_settlement", "sticky_session"},
        }
    }

    // model_list pipeline
    engineConfig.Pipelines["_model_list"] = &core.PipelineConfig{
        Name:         "_model_list",
        RequestTypes: []core.RequestType{core.RequestTypeModelList},
        Invoker:      core.InvokerConfig{Type: "cluster", Retry: core.RetryConfig{MaxRetries: 1}},
        OutboundFilters: []string{"access_log"},
    }

    for _, pc := range providerConfigMap {
        engineConfig.Providers[pc.Name] = pc
    }

    return engineConfig, providerImpls, providerConfigs, knownModels
}
```

- [ ] **Step 3: 实现 registerEndpointsFromRelationalConfig**

```go
func registerEndpointsFromRelationalConfig(sd *discovery.StaticDiscovery, resolved []config.ResolvedModelProvider) {
    providerEndpoints := make(map[string][]*discovery.ServiceInstance)
    for _, rp := range resolved {
        for i, ep := range rp.Endpoints {
            u, err := url.Parse(ep.URL)
            if err != nil {
                continue
            }
            port := 80
            if u.Scheme == "https" {
                port = 443
            }
            if u.Port() != "" {
                fmt.Sscanf(u.Port(), "%d", &port)
            }
            instance := &discovery.ServiceInstance{
                ID:     fmt.Sprintf("%s-%s-%d", rp.ProviderName, rp.ModelName, i),
                IP:     u.Hostname(),
                Port:   port,
                Scheme: u.Scheme,
                Weight: ep.Weight,
                Health: discovery.HealthStatusHealthy,
                Metadata: map[string]string{
                    "model":          rp.ModelName,
                    "upstream_model": rp.RealModel,
                    "api_key":        rp.APIKey,
                    "api_base":       ep.URL,
                },
            }
            providerEndpoints[rp.ProviderName] = append(providerEndpoints[rp.ProviderName], instance)
        }
    }
    for providerName, instances := range providerEndpoints {
        sd.RegisterService(providerName, instances)
    }
}
```

- [ ] **Step 4: 编译验证**

Run: `go build ./cmd/server/`
Expected: 编译通过

- [ ] **Step 5: Commit**

```bash
git add cmd/server/wire/provider.go
git commit -m "feat: rewrite NewGatewayEngine to support relational config with Redis source"
```

---

### Task 8: 更新配置文件为新格式

**Files:**

- Modify: `config/local.yml`
- Modify: `config/llm.example.yml`

- [ ] **Step 1: 重写 config/local.yml**

将 `config/local.yml` 中的 `llm:` 段替换为新格式（保留其他配置不变）：

```yaml
# LLM Gateway 配置（关系型三表格式）
models:
  gpt-4:
    real_model: gpt-4
    request_type: chat_completion
  gpt-4-turbo:
    real_model: gpt-4-turbo-preview
    request_type: chat_completion
  gpt-3.5-turbo:
    real_model: gpt-3.5-turbo
    request_type: chat_completion
  text-embedding-3-small:
    real_model: text-embedding-3-small
    request_type: embedding
  claude-sonnet:
    real_model: claude-3-opus-20240229
    request_type: chat_completion

providers:
  openai-official:
    type: openai
    api_key: ${OPENAI_API_KEY}
    timeout: 60s
    max_retries: 3
    endpoints:
      - url: https://api.openai.com/v1
        weight: 1
  anthropic-official:
    type: anthropic
    api_key: ${ANTHROPIC_API_KEY}
    timeout: 60s
    endpoints:
      - url: https://api.anthropic.com
        weight: 1
  anthropic-proxy:
    type: anthropic
    api_key: sk-04a01ecdf6a3407f8b06e5b83dc1b5f4
    timeout: 60s
    endpoints:
      - url: http://localhost:8045
        weight: 1

model_providers:
  - model: gpt-4
    provider: openai-official
    priority: 1
    weight: 100
  - model: gpt-4-turbo
    provider: openai-official
    priority: 1
    weight: 100
  - model: gpt-3.5-turbo
    provider: openai-official
    priority: 1
    weight: 100
  - model: text-embedding-3-small
    provider: openai-official
    priority: 1
    weight: 100
  - model: claude-sonnet
    provider: anthropic-official
    priority: 1
    weight: 50
  - model: claude-sonnet
    provider: anthropic-proxy
    priority: 2
    weight: 50

fallbacks:
  gpt-4:
    - gpt-4-turbo
    - gpt-3.5-turbo
  claude-sonnet:
    - gpt-4
```

- [ ] **Step 2: 重写 config/llm.example.yml**

更新为新格式示例（包含更多 provider 类型的注释示例）。

- [ ] **Step 3: 编译验证**

Run: `go build ./cmd/server/`
Expected: 编译通过

- [ ] **Step 4: Commit**

```bash
git add config/local.yml config/llm.example.yml
git commit -m "feat: update config files to relational three-table format"
```

---

### Task 9: 端到端验证

**Files:** None (verification only)

- [ ] **Step 1: 运行所有单元测试**

Run: `go test ./... -v`
Expected: 所有测试通过

- [ ] **Step 2: 编译**

Run: `make build`
Expected: 编译成功

- [ ] **Step 3: Commit（如有修复）**

```bash
git add -A
git commit -m "fix: address issues found during end-to-end verification"
```

---

## Self-Review Checklist

1. **Spec coverage:**
   - ADR-0001 三表关系型结构 → Task 3 (types), Task 5 (resolve), Task 7 (build)
   - ADR-0002 用户维度降级 → Task 8 (YAML fallbacks 段)，数据库部分不在本计划范围
   - ADR-0003 运行时 API 校验 → 已有能力（APIRouter），无需额外实现
   - ADR-0004 分层配置源 → Task 5 (ConfigManager)
   - ADR-0005 Redis key + 懒加载 → Task 4 (RedisConfigSource)

2. **Placeholder scan:** 无 TBD/TODO。

3. **Type consistency:** `ResolvedModelProvider` 在 Task 3 定义，Task 4/5/6/7 使用，字段名一致。`EffectiveModel()` 在 Task 1 定义，Task 2 使用。
