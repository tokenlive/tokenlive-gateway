# ADR 0003: 大模型能力解耦与 ModelProvider 二维关联保持

## 状态

Accepted (已接受)

## 上下文

在之前的设计讨论中，我们发现把单值 `request_type` 字段（如 `chat_completion`）直接绑定在 `models` 模型定义表上有失偏失（因为某些模型系列同时拥有多项请求能力）。曾提出过一种“在 `model_providers` 关联表中引入 `request_type` 并升级为三维联合主键”的方案。

然而，经过针对实际 LLM 提供商物理生态的进一步压力测试与分析发现：

1. **上游物理模型的分离性**：在所有主流的云端大模型服务中，从来没有一个模型能够在同一个物理名称下同时对外处理文本对话与文本向量化。例如，OpenAI 对话叫 `gpt-4o`，向量化叫 `text-embedding-3-small`。它们是完全不同的模型系列，拥有独立的对外模型名。
2. **客户端调用的指向性**：客户端在调用网关不同接口时，必然需要显式传入不同的模型参数。因此它们在网关的 `models` 定义表中天然就是两条相互独立的记录（对应不同的 `model_id` / `model_code`）。
3. **避免过度设计**：既然模型记录本身已经是完全物理隔断的，那么针对 `(model_id, provider_id)` 对的多对多路由关联便也是天然不冲突的。引入三维 `(model_id, provider_id, request_type)` 会造成极大的冗余设计，并增加网关运行时路由匹配算法的开销。

对于同一个模型同时响应多种相关生成操作的场景（例如多模态 `gpt-4o` 既支持 `chat_completion` 又支持 `image_generation`），由于这属于同类会话机制，其上游 API 调用和真实模型名依然完全一致，不产生配置映射分裂冲突。

## 决策

我们最终做出如下决策：

1. **维持 `model_providers` 二维关联**：废除三维升维提案，撤销 `model_providers` 关联表中的 `request_type` 字段，并恢复唯一键约束为经典的 `uk_model_provider (model_id, provider_id)`。
2. **在 `models` 表中使用能力列表声明**：在模型定义表中通过 `RequestTypes` (JSON 数组) 来声明该模型支持哪些请求操作（如 `["chat_completion", "image_generation"]`），仅用于网关前置的路由安全拦截和过滤，不干涉多对多路由关联逻辑。

## 后果

* **正面影响**：
  * 避免了数据库过度设计，保持了 `model_providers` 极简的表结构。
  * 网关服务发现和 Endpoint 选择逻辑依然可以使用极高的 O(1) 效率直接按 `(model_id, provider_id)` 进行匹配过滤，性能保持极致。
* **负面影响/折中**：
  * 无。此设计最符合大模型真实物理分布生态。
