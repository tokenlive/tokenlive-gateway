# ADR 0015: Protocol Translation at Provider Invoker

为网关层提供协议转换（Protocol Translation）与同名转发（Same-Path Forwarding）能力，支持在不直接兼容上游 API 的场景下，对客户端请求与响应进行跨协议翻译。典型场景为：客户端通过 Anthropic `/v1/messages` 协议请求，网关将其透明翻译转换为 OpenAI 兼容的 `/v1/chat/completions` 并路由至对应上游，再将响应逆向翻译回 Anthropic 格式。

## Context

随着多供应商（Multi-Provider）治理 of 深入，网关面临不同 LLM 厂商协议契约差异带来的兼容性挑战：

1. **客户端与模型能力错位**：客户端希望使用特定厂商的专属客户端（如使用 Anthropic SDK 发送 `/v1/messages` 格式的复杂 Payload）请求网关，但网关后端绑定的模型端点可能是只支持 OpenAI 协议规范的兼容端点（仅支持 `/v1/chat/completions`）。
2. **路由连贯性被破坏**：如果在路由分发（Routing）或过滤器（Pipeline Filters）前置对 Payload 进行了整体翻译重写，会使过滤器链路耦合了特定厂商的协议转换逻辑，甚至导致度量（Metrics）、计费（Billing）和染色标签（Dyeing Tags）拿到了已被修改的请求类型而导致数据失真。
3. **流式 SSE 协议的实时翻译难题**：大模型流式（Stream）响应在网络上是逐字节分块下发的 Server-Sent Events。如果不进行实时事件的解包与重组，客户端将无法以预期的协议消费上游不同格式的流。

## Considered Options

为了实现该协议翻译，我们面临以下架构选择：

1. **在 Pipeline Filter（如 Inbound/Outbound Filter）层进行翻译**：
   - *缺点*：这会使得全局过滤器直接耦合了上游厂商细碎的协议结构定义，极大地破坏了 Pipeline 的纯粹性，也不利于未来新增非标准协议时的扩展。
2. **在路由分发层（ClusterInvoker）进行中转和翻译**：
   - *缺点*：ClusterInvoker 的核心定位是负责调度、路由链执行、多优先级过滤及重试容错。如果在此处承载跨协议翻译的 Payload 重写，会导致调度中心过载。
3. **在 Provider 适配器层（ProviderInvoker）进行双向翻译（选中方案）**：
   - *优点*：Provider 适配层本身就是直面上游物理端点的底层隔离层。在此层做翻译能保持极高的内聚性。
   - *优点*：通过在 OpenAI Provider 的 `RequestTypes()` 中声明自己也支持 `RequestTypeMessages`，使得 API 路由器在路由阶段能以标准统一的方式直接筛选并保留该端点，路由层不需要做任何硬编码逻辑。

## Decision

我们决定采用**方案 3（在 Provider 适配器层进行双向翻译）**。

### 1. 路由兼容与声明

OpenAI Provider 支持的 `RequestTypes` 列表被扩充，除原有的 `chat_completion`、`embedding`、`model_list` 外，显式加入 `messages`。
当客户端访问 `/v1/messages` 时，路由链（`API` 路由器）通过对齐 RequestTypes，能将 OpenAI 兼容的物理端点安全地包含在可用候选集内，完成正常的熔断检查、标签路由以及负载均衡筛选。

### 2. ProviderInvoker 内部翻译执行

当 `Invoker.Invoke` 触发时，若请求类型为 `RequestTypeMessages`：

- **Request Translation**：OpenAI Provider 执行翻译。将 Anthropic 协议体中的 `system`（顶层字符串）合并为 OpenAI `messages` 列表的第一个元素（`role: system`），映射核心的 `max_tokens` 为 `max_tokens` / `max_completion_tokens`，保留 `stream` 等核心控制字段，其余未知扩展字段执行**优雅退化（Graceful Mode）**直接保留透传。
- **同名转发覆盖**：将请求的目标 URL 重写为上游 Provider 的 `/chat/completions` 路径，并用转换后的 JSON RawBody 覆盖 `gctx.RawBody` 发送物理请求。
- **Response Translation**：
  - **非流式**：解析上游返回的 OpenAI 格式 JSON，逆向重构为 Anthropic 协议格式（如包括 `id`, `model`, `role: assistant`, `content` 数组以及 `usage`），赋值给 `gctx.Response` 或者是 `gctx.UpstreamBody` 输出。
  - **流式**：在 `handleMessagesStream` 中启动实时流翻译。利用 `SSEParser` 拦截并实时解码上游每一帧 OpenAI SSE 事件对象（如 `choices[0].delta.content`），转换为对应的 Anthropic 协议事件（如 `message_start` , `content_block_delta` , `message_delta` , `message_stop`），再重新格式化为 `data: ...\n\n` 字节流写回 ResponseWriter，保证客户端消费契约不受破坏且保持低时延体验。

## Consequences

- **系统解耦**：路由匹配与过滤器对协议翻译零感知。所有的状态统计（`status_collector`）、模型限流（`rate_limit`）均能正常消费原始租户的 messages 路由契约。
- **对扩展开放**：未来若要支持将 OpenAI 请求翻译为 Anthropic 格式（反向翻译），只需在 Anthropic Provider 中对应注册 `RequestTypeChatCompletion` 并实现其翻译 Invoker 即可，符合开闭原则（Open-Closed Principle）。
