# ADR-0007: 基于 RequestType 的细粒度 RequestInvoker 拆分

## 状态
已接受 (Accepted)

## 上下文背景
在原架构设计中，`core.Provider` 的 `Invoke(gctx)` 是统一的协议适配入口。由于一个物理 LLM 供应商（如 OpenAI 官方）往往同时支持 Chat、Embedding、ModelList、Image 等多种接口类型，这导致 `OpenAIProvider` 或 `AnthropicProvider` 等具体实现类的代码逐渐膨胀。
在单个 Provider 类中需要使用复杂的 `switch-case` 块来分发不同的请求，严重违反了**单一职责原则 (SRP)**，不利于后续网关接口类型的线性扩展。

同时，不同供应商的同一类接口（如同样是 `embedding`）底层的 Payload、API Endpoint 路径以及响应格式是完全不同的。因此，我们必须保留 `providers.type`（即协议类型）这一物理字段以做底层通信协议的硬区分。

## 决策内容
1. **职责分离**：
   - 将 `core.Provider` 重新定义为“物理连接与凭证管理器”（负责管理 baseURL、apiKey、http.Client 连接池与 HealthCheck）。
   - 引入无状态的专有执行器 `core.RequestInvoker`：
     ```go
     type RequestInvoker interface {
         Invoke(gctx *GatewayContext, p Provider) error
     }
     ```
   - 每一个 `(ProviderType, RequestType)` 组合（例如 `(openai, chat_completion)`）对应一个专属的 `RequestInvoker` 实现（例如 `openaiChatInvoker`）。
2. **零侵入式动态派发**：
   - 保留原有的 `core.Provider.Invoke(gctx)` 接口签名与物理表 `providers.type` 协议标识。
   - 在各 Provider 的 `Invoke()` 实现内，通过从核心注册表动态获取对应的 `RequestInvoker` 处理器，代理完成最终调用。
   - 这样既将具体的业务编排解耦到独立执行器文件中，又避免了外部的 Engine Pipeline、ProviderInvoker 和集成测试代码发生任何侵入性变化。

## 架构收益
* **高内聚低耦合**：各接口的协议处理在独立文件中演进，清除了主驱动类内的多路条件分支。
* **高扩展性 (Open-Closed Principle)**：新增接口能力类型时，只需编写新的 `RequestInvoker` 实现并在 `init()` 中注册，对现有组件零影响。
* **高稳定性**：物理接口保持完全兼容，不引入集成与调用层回归隐患。
