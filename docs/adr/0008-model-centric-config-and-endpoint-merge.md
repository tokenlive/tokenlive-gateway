# Model-Centric 配置与 Endpoint 合并

将配置结构从 provider-centric 的三表关系（models / providers / model_providers）改为 model-centric 的两层结构（models / providers），移除 `model_provider` 中间层，将关联信息（`model_id`、`real_model`、`priority`）下沉到 `endpoint` 层。Endpoint 成为路由的最小单元，每个 endpoint 自描述完整的路由信息。

## Context

ADR-0001 引入了三表结构解决了"一个 provider 多种协议"的问题，但带来了新的复杂度：

- **创建流程冗长**：Model → Provider → ModelProvider → Endpoint，四步操作
- **查询需要多表关联**：路由时 Model → ModelProvider → Provider → Endpoint，链路过长
- **数据一致性风险**：ModelProvider 和 Endpoint 中的信息可能不同步
- **配置结构不直观**：`model_providers` 作为独立段，既不属于 model 也不属于 provider，编写者需要在三个段之间跳转

从 admin 端的数据库设计出发，决定移除 `model_provider` 表，将关联信息迁移到 `endpoint` 表。gateway 侧同步调整配置结构和 Redis 数据契约。

## Considered Options

1. **保持三表结构不变，仅同步 schema.sql** — 改动最小，但 YAML 配置与数据库设计语义不一致，长期维护负担大。
2. **Provider-centric 简化**（删除 model_providers 段，model 信息放到 provider.endpoints 下）— 减少一个配置段，但 model 降为 provider 的属性，与"model 是服务角色"的认知不符。
3. **Model-centric 两层结构**（选中）— model 是一等公民，endpoint 挂在 model 下，provider 只定义基础设施。配置编写者以 model 为入口组织路由，语义最清晰。

## Decision

### 配置结构改为 Model-Centric

```yaml
models:
  gpt-4:
    request_type: chat_completion
    endpoints:
      - provider: openai-official
        url: http://proxy:8100/v1
        real_model: gpt-4-turbo
        priority: 1
        weight: 100

providers:
  openai-official:
    protocol: openai
    api_key: ${OPENAI_API_KEY}
    timeout: 60000
    max_retries: 3

fallbacks:
  gpt-4: [gpt-4:free]
```

- `models:` — 一等入口，定义 model 元数据（request_type、RequestTypes）和所属 endpoints
- `providers:` — 纯基础设施定义（protocol、api_key、timeout），不再包含 endpoints
- `fallbacks:` — 顶级字段，表达 model 间的降级关系

### 运行时结构扁平化

`ResolvedModelProvider` → `ResolvedEndpoint`，每个 endpoint 独立持有完整的路由信息：

```go
type ResolvedEndpoint struct {
    ModelName        string `json:"model_name"`
    RealModel        string `json:"real_model"`
    ProviderName     string `json:"provider_name"`
    ProviderProtocol string `json:"provider_protocol"`
    APIKey           string `json:"api_key"`
    URL              string `json:"url"`
    Timeout          int64  `json:"timeout"`      // 毫秒
    MaxRetries       int    `json:"max_retries"`
    Priority         int    `json:"priority"`
    Weight           int    `json:"weight"`
}
```

### Redis 数据契约

| Key | 类型 | 说明 |
|-----|------|------|
| `aigw:config:version` | STRING | 配置版本号，变更时递增 |
| `aigw:config:endpoints:{modelName}` | STRING (JSON) | 该 model 的所有 `[]ResolvedEndpoint` |
| `aigw:user:{userID}:models` | SET | 用户已开通的 model 集合（不变） |

admin 端负责维护这些 key，gateway 轮询 version 并按需加载 endpoints。

### 字段继承规则

endpoint 未指定的字段从 provider 继承：

- `api_key`：endpoint > provider
- `timeout`：endpoint > provider > 默认 60s
- `max_retries`：provider > 默认 3

`real_model` 无 provider 级别回退，endpoint 必须显式指定。

### 删除的配置和表

- **YAML**：`model_providers:` 段删除
- **Go 类型**：`ModelProviderConfig` 删除
- **SQL**：`model_provider` 表、`model_provider_endpoint` 表删除
- **Redis key**：`aigw:config:model_providers:{modelName}` 改为 `aigw:config:endpoints:{modelName}`

## Consequences

- YAML 配置从三段式变为两段式，编写者以 model 为入口组织，减少跨段跳转
- `ProviderConfig` 不再包含 endpoints，职责更纯粹（纯基础设施）
- `Resolve()` 逻辑从"遍历 model_providers 合并三级字段"变为"遍历 model.endpoints 继承 provider 字段"，复杂度相当
- Redis payload 每个 endpoint 独立完整，不再需要运行时合并，查询更直接
- `ModelService` 不受影响（只关心 model name，不涉及 model_provider 结构）
- `RequestType` 仅在配置校验时使用，不进入 `ResolvedEndpoint` 和 Redis，运行时从请求体推断
- admin 端需要同步更新推送逻辑，写入新的 Redis key 和数据结构
