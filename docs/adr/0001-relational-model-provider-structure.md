# 模型-Provider 关系型三表结构

> **状态：已废弃** — 被 [ADR-0008](0008-model-centric-config-and-endpoint-merge.md) 替代。三表结构简化为 model-centric 两层结构，`model_provider` 中间层移除，关联信息下沉到 endpoint。

`model_list` 和 `providers` 从嵌套配置改为关系型三表结构（models / providers / model_providers），取代原有的 `llm.model_list` 嵌套在 `llm.providers` 下的设计。

原有设计将 model 作为 provider 的子属性，导致：一个 provider 只能绑定一种协议类型（无法表达"同一供应商提供 OpenAI 和 Anthropic 两种协议"）；model 配置和 provider 配置耦合在一起，无法独立复用。

新设计拆为三个独立实体：

- **models**：用户视角的模型定义（model_name、real_model、request_type）
- **providers**：上游来源定义（type、api_key、endpoints、timeout）
- **model_providers**：多对多关联表，携带路由元数据（priority、weight）和可覆盖字段（real_model、api_key、endpoints、timeout）

## Considered Options

1. **嵌套结构（model 嵌在 provider 下）** — 原有设计。简单但无法表达一个供应商多种协议的场景。
2. **嵌套结构（provider 嵌在 model 下）** — OpenRouter 风格。以 model 为中心，但 provider 无法独立复用。
3. **关系型三表结构** — 选中。model 和 provider 各自独立，通过关联表达关系，最灵活。

## Consequences

- YAML 配置从两个段（model_list + providers）变为三个段（models + providers + model_providers）
- 启动时需要校验 model_providers 引用的 model 和 provider 是否存在，但不做强约束——没有 provider 绑定的 model 静默忽略
- 运行时路由逻辑变为：查 model_providers → 按 priority 排序 → 过滤 RequestTypes → 按 weight 选择
