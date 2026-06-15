# 染色（Tagging）与筛选（Routing）正交解耦策略

大模型网关在进行流量治理时，将“请求染色”与“流量筛选”做职责上的彻底正交解耦，保持两套策略体系的正交独立性。

## 背景与问题

大模型网关中往往有多种路由分流与分级需求（如 VIP 客户路由至高可用节点、长流式响应路由至高带宽节点）。在早期设计中，开发人员倾向于将动态染色（根据请求特征判定类型）和路由（根据属性决定去往哪个下游）混为一谈，导致路由动作隐式定义在打标 Action 中，降低了全局路由规则的直观性，违背了单一职责原则。

## 架构决策

网关引擎彻底解耦这两套机制：

1. **请求染色（Tagging）**：
   - 职责：只负责元数据富集与状态染色。
   - 实现：由 Inbound 阶段的 `TaggingFilter` 执行 `TaggingPolicies` 规则。根据请求特征（如 header、query、model 等），在运行时上下文 `GatewayContext.Tags` 中打上染色标签（如 `cost_tier: high`），但不参与最终的 Endpoint 物理筛选。

2. **流量筛选（Routing）**：
   - 职责：只负责根据运行时上下文特征，筛选出匹配条件的 downstream Endpoint 候选集。
   - 实现：由 `ClusterInvoker` 路由链中的 `TagRouter` 执行 `RoutePolicies` 规则。它接收已被染色标记的 `gctx.Tags`，根据匹配规则过滤并按权重选择对应的 `Destination`，完成流量的分拨。

3. **降级逃生机制**：
   - 当 `RoutePolicies` 筛选后导致可用 Endpoint 列表为空时（例如 VIP 首选的 premium 专线全部被熔断），触发降级逃生，网关自动退避至默认无标签约束的 Endpoint 候选池，保证高可用性。
