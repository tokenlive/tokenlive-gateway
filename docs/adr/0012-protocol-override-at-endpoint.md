# ADR 0012: Protocol Override at Endpoint Level

在 `endpoint` 物理节点级别引入可选的 `protocol` 字段，允许在特定 Endpoint 上覆盖 `provider` 级别的默认协议族类型（继承覆盖机制）。

## Context

在 ADR-0008 实施后，我们将模型、供应商与物理节点的结构简化为 Model-Centric 的两层结构（Models / Providers），其中 `provider` 主要承担 API Key 等连接基础设施的定义。同时，`protocol`（协议类型，如 `openai`、`anthropic`）也作为 Provider 级别的重要特征被其底下的所有 `endpoints` 默认继承。

但在实际生产环境中，面临着以下**异构协议**的复杂接入诉求：
1. **跨协议族的供应商**：部分大模型厂商（如 AWS Bedrock、阿里云百炼）或某些聚合中转商（如 OneAPI、企业自研中台），在同一个连接实体（即同一个 `provider` 名和 API 密钥下）提供了跨越不同协议族的多款模型（例如一些模型对外暴露标准的 OpenAI 接口，另一些模型只能走 Anthropic 或 AWS 原生的 API 格式）。
2. **多协议节点**：为了进行平滑灰度或协议适配升级，同一个 Provider 的不同接入端点（Endpoint URL）可能会暴露出不同的通信协议规范。

如果在 `provider` 级别强制锁定单一协议类型，我们将被迫把原本逻辑上同属一个大厂/网关的连接凭证，拆分成多个独立的 Provider（如 `aws-bedrock-openai`、`aws-bedrock-anthropic`），这违背了 Provider 的领域模型定位并增加了配置管理成本。

## Considered Options

1. **强行拆分 Provider（当前方案）**：遇到多协议供应商时，强制要求用户创建多个 Provider 并维护多套 API Key。这增加了后台配置的繁琐度，并且无法将同一个网关作为一个管理边界。
2. **在 Endpoint 增加可选 Protocol 覆盖（选中方案）**：在数据库和配置结构中，让 `provider` 声明默认的 `protocol`，而在 `endpoint` 层面开放一个可选的 `protocol` 覆盖字段。在路由构建时通过 `endpoint.protocol > provider.protocol` 合并。

## Decision

我们决定采用**方案 2（继承覆盖机制）**，使得协议解析在运行时得到更好的局部微调支持。

### 1. 配置实体演进

在配置加载和解析的 Go 类型定义中：
* `EndpointConfig` 添加可选的 `Protocol string`。
* `ResolvedEndpoint` 中的 `ProviderProtocol` 解析时执行覆盖逻辑：如果 `endpoint.protocol` 不为空，则最终展开为该字段，否则回退为 `provider.protocol`。

```go
// ResolvedEndpoint 解析合并规则
protocol := ep.Protocol
if protocol == "" {
    protocol = ep.Provider.Protocol
}
```

### 2. 数据库设计对齐
在 `endpoint` 表中增加如下字段，保持与 `api_key` 在 Endpoint 层相同的局部覆盖语义：
```sql
ALTER TABLE `endpoint` ADD COLUMN `protocol` VARCHAR(64) DEFAULT NULL COMMENT '可选，覆盖 provider 级别的 protocol';
```

## Consequences

* **灵活性显著增强**：天然支持 AWS Bedrock 等厂商“一个供应商账户，多协议族模型”的诉求。
* **操作无缝向后兼容**：90% 的普通单协议供应商依然只需在 Provider 层配置一次 `protocol`，Endpoint 无需重复填写，这降低了管理员的日常配置负担。
* **与 API Key 逻辑保持一致**：目前 `api_key` 和 `timeout` 已在 Endpoint 拥有局部覆盖机制，`protocol` 加入后统一了覆盖策略的规则和认知。
