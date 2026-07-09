# ADR 0021: 治理流中的致命错误传递机制

## Status

Accepted

## 上下文

当服务治理流中发生某些不可恢复的故障（例如：用户开启了强端点亲和性策略且不允许降级，但路由匹配失败）时，继续重试或者执行跨 Model 的 Fallback 降级不仅会造成流量重试资源浪费，更会带来数据一致性风险和非预期的业务降级（即非期望的模型污染）。

为此，我们需要一种“致命错误（Fatal Error）”机制，一旦该错误产生：
1. **重试引擎**：立即终止单次请求内的所有重试 Attempt。
2. **降级引擎**：立即跳过跨 Model 的 Fallback 流程，直接向客户端投递最终错误。

然而，传统的负载均衡器（LoadBalancer）接口签名通常仅返回端点实例，若失败则返回空。这使得上层的 Invoker 在面临无端点时，无法区分这是“普通的熔断或候选列表空导致的无可用端点（需要重试/降级）”，还是“由于亲和策略约束主动拦截导致的致命无可用端点（严禁重试/降级）”。

## 决策

为了在不破坏负载均衡器（LoadBalancer）接口纯净性及避免大规模重构接口返回值的原则下，我们选择**通过请求上下文状态旁路进行致命错误标记与传递**：

1. **Go 网关端**：
   - 定义致命错误变量 `core.ErrFatalNoAvailableEndpoint`，并在请求上下文 `GatewayContext` 结构体中添加 `FatalErr error` 字段。
   - 在 `EndpointAffinityLoadBalancer.Select` 匹配失败且不允许降级时，设置 `gctx.FatalErr = core.ErrFatalNoAvailableEndpoint` 并返回 `nil`。
   - 在 `ClusterInvoker` 的路由/重试检查以及 `Engine` 的 `shouldDynamicFallback` 决策中，只要检测到 `FatalErr`，即刻返回 `false` 中断降级与重试，返回该致命错误。

2. **Java SDK 端**（为保持两端设计一致性，保留此参考）：
   - 引入一个新的致命异常类型 `FatalException`，并在 `OutboundInvocation` 上下文对象中引入 `fatalError` 属性。
   - 当 `EndpointAffinityLoadBalancer` 强制匹配未命中且不允许降级时，在其 `select` 阶段向 `invocation.setFatalError(...)` 写入该致命异常，并返回 `null`。
   - 重试层 `FailoverClusterInvoker` 与降级层 `PolicyClusterInvoker` 只要检查到该标记，即刻短路所有执行，直接返回该致命错误。

## 后果

### 优点
1. **零性能损耗**：不需要通过复杂的 Exception 栈抛出控制或反射，通过上下文属性轻量标记传递。
2. **极佳的兼容性**：保留了负载均衡器接口的原始签名。
3. **彻底的自愈阻断**：完美保障了强亲和策略的严格不降级性。

### 缺点
- 维护者在扩展新的治理策略时，必须理解该上下文生命周期中的 FatalErr 副作用标记。
