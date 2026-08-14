# ADR 0022: 模型上下文长度与最大输出 Token 运行时消费与元数据暴露机制

## Status

Accepted

## 上下文 (Context)

在 `tokenlive` 的元数据体系中，Admin 后台在 `Model` 及 `ModelCatalog` 表中维护了 `context_length`（最大上下文窗口）与 `max_output_tokens`（最大输出 Token），但在之前的版本中，Gateway 运行时（`ResolvedEndpoint` / `core.Endpoint`）并未消费这两个字段：

1. **协议翻译硬编码与参数溢出风险**：
   - 当客户端使用 OpenAI Chat/Responses 协议请求 Anthropic 上游端点时，Anthropic Messages API 强制要求请求必须携带 `max_tokens`。之前网关在 `messages_chat.go`（硬编码 4000）和 `responses_messages.go`（硬编码 8192）中使用了静态兜底；
   - 当客户端 UI（如 OpenWebUI、Chatbox、Dify 等）设置了较大的 `max_tokens`（如 32768/64000），而实际后端模型仅支持 8192 时，上游 Anthropic 直接返回 400 校验失败（`max_tokens exceeds limit`），导致请求被无谓拒绝甚至引发网关重试风暴。
2. **模型能力感知断层**：
   - 外部客户端或前端应用通过 `GET /v1/models` 获取可用模型列表时，网关仅返回了 `id`、`object`、`created`、`owned_by`，客户端无法自动探测当前模型的上下文上限，用户需在客户端界面手动配置滑块最大值。

## 决策 (Decision)

我们决定将 `context_length` 和 `max_output_tokens` 作为路由端点的自描述物理能力（Capabilities），完成从 Admin 到 Gateway 运行时的全链路贯通：

### 1. 数据载体与同步契约 (Data Model & Inheritance)

- **结构扩展**：在 Admin 和 Gateway 的 `ResolvedEndpoint` 以及 `core.Endpoint` 结构中新增 `ContextLength int64` 和 `MaxOutputTokens int64` 字段（JSON Tag: `context_length` / `max_output_tokens`）。
- **继承与覆盖**：Admin 在将已合并数据写入 Redis（`aigw:config:endpoints:{modelCode}`）时，默认继承 `Model` 表中的 `context_length` 和 `max_output_tokens`，并允许在 Endpoint `metadata` 中单独覆盖（以支持自建或特定供应商端点的差异化上下文规格）。
- **复用现有机制**：无需新增独立的 Redis Key 或轮询通道，网关在原有的 1 RTT lazy-polling 机制中直接加载。

### 2. 跨协议翻译动态兜底与自动钳位 (Protocol Translation Defaulting & Clamping)

在 Provider/Translate 跨协议转换模块（如 OpenAI Chat/Responses → Anthropic Messages）处理 `max_tokens` 时执行如下规则：

1. **未传值时（Defaulting）**：优先使用目标 `Endpoint.MaxOutputTokens`（若未配置或 ≤0 则回退到默认 4096 / 8192）；
2. **超限传值时（Clamping）**：若客户端传入的 `max_tokens` > `Endpoint.MaxOutputTokens`（且 `Endpoint.MaxOutputTokens > 0`），网关自动将其钳位为 `Endpoint.MaxOutputTokens`，避免上游 Provider 直接报 400 错误。

### 3. 模型列表标准元数据暴露 (`GET /v1/models`)

- 在 `LLMHandler.ListModels` 的响应中平铺输出 `context_length` 与 `max_output_tokens` 顶级字段（若未设置则为 0 或缺省）：
  ```json
  {
    "id": "claude-3-5-sonnet",
    "object": "model",
    "created": 0,
    "owned_by": "anthropic",
    "context_length": 200000,
    "max_output_tokens": 8192
  }
  ```
- **别名继承**：所有 Model 别名（Alias）直接继承主模型的 `context_length` 和 `max_output_tokens`。

## 后果 (Consequences)

### 优点
1. **零额外网络 I/O**：复用现有的 `ResolvedEndpoint` 数据分发体系，不增加 Redis 查询次数或存储结构复杂度；
2. **极大提升跨协议兼容性与容错能力**：消除硬编码默认值，自动钳位超限参数，避免客户端默认配置造成的 400 失败与无效重试；
3. **前端工具友好**：OpenAI 兼容的客户端应用可直接感知上下文规格，自动初始化参数边界。

### 限制与未来演进
- **长 Prompt 快速失败（Fail-Fast）与上下文感知路由**：可在后续基于 Token 估算器（Token Estimator）演进入站过滤与多端点筛选。
