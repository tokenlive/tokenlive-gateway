# ADR 0014: Priority Active-Standby Router and Adaptive Health Checking

为网关层提供高可用的主备温备（Active-Standby）与 Failover 路由能力，并引入基于探测端点（Endpoint）的主动自适应健康检查（Active Health Check），解决在熔断模式与优先级策略下半开（Half-Open）状态无法自动恢复（流量卡死）的死锁问题。

## Context

在目前的路由与容灾体系中，存在以下挑战：
1. **主备（温备）路由缺失**：大模型网关中经常需要为某些模型配置主备通道。当主通道可用时，所有请求流向主通道（即使备通道响应快或权重高）；仅当主通道熔断或异常时，请求才切到备通道。目前网关仅支持基于权重的负载均衡，缺乏基于优先级的严格路由规则（`PriorityRouter`）。
2. **Half-Open 状态无法探路（流量卡死）**：当主通道因为出错而熔断后，流量自动 Failover 切换到优先级较低的备通道。因为在 `PriorityRouter` 选路下，所有流量都在备通道上，熔断的主通道在 Half-Open 状态下分不到任何真实请求。由于拿不到请求，主通道就永远无法触发“成功请求”，进而无法将熔断器状态由 Half-Open 重置为 Closed（即死锁），从而无法回切（Failback）。
3. **缺少实例粒度的探路机制**：原先的 `StartHealthCheck` 是基于提供商（Provider）维度的粗粒度检测，不支持端点级别的健康探测，也无法根据熔断器的不同状态智能调节探测频次。

为了彻底打破这个“主备切换与熔断器半开自动恢复”的死锁，我们需要设计：
- **`PriorityRouter` 路由机制**：在过滤掉完全熔断的节点后，基于端点的优先级（`Priority`）进行硬约束过滤，只向用户暴露可用实例中优先级最高（Priority 值最小）的节点子集。
- **主动自适应健康检查（Active Health Check）**：由网关后台的探测协程定期向配置了 `health_check_url` 的端点发送 HTTP 探测请求。探测频率需自适应：健康实例为 30s，熔断/半开实例加速到 5s。连续 3 次探测成功后，强设熔断器为 Closed 状态，自动回切到主通道。
  - **默认关闭保障开销**：为避免在高频探测下对大模型上游 API 造成意外的流量/Token 资损开销，主动健康探测功能**默认关闭**。用户需通过全局配置项 `llm.enable_active_health_check: true` 显式开启该协程。

## Considered Options

1. **依靠真实流量探路 (Passive Failback)**：
   - *缺点*：在 PriorityRouter 的作用下，备通道工作时主通道无法分到任何真实请求，导致无法回切。如果不引入主动探测，该死锁无解。
2. **通过后台 Active Health Check 强设 Closed（选中方案）**：
   - *优点*：通过主动的健康探测打破流量死锁。通过自适应频率（健康 30s，非健康 5s）降低健康通道的开销，并保证不健康通道能快速恢复。
   - *优点*：为了不增加数据库表结构的变更，将探测配置（`health_check_url`）直接保存在已有 `Endpoint` 的 `Metadata` 字段中。

## Decision

我们决定采用**方案 2（通过后台 Active Health Check 强设 Closed）**。

### 1. 路由器链条执行顺序
`PriorityRouter` 必须在 `CircuitBreakerRouter` **之后**执行。
```
Candidate Endpoints
         │
         ▼
CapabilityRouter (能力匹配)
         │
         ▼
TagRouter (标签染色路由)
         │
         ▼
CircuitBreakerRouter (熔断过滤：过滤掉 Open 实例)
         │
         ▼
PriorityRouter (优先级硬过滤：取剩余可用实例中 Priority 最小的集合)
         │
         ▼
LoadBalancer (负载均衡：在最小 Priority 集合中按权重分配)
```
- **熔断过滤先于优先级**：只有先过滤掉不健康节点，`PriorityRouter` 才能从剩下的可用通道中动态选择 Priority 最小的，完成向备通道的切换。
- **同优先级内负载均衡**：若最小 Priority 对应的可用节点有多个，则进入 LB 进行权重分流。

### 2. 自适应健康探测状态机与半开流量隔离 (Permits Flow Control)
主节点熔断发生后，用户配置的熔断冷却时间（`WaitDurationInOpenState`）严格生效。冷却期满后，状态自动转为 `Half-Open`。在半开状态下，网关通过“并发探路许可限制”防止流量洪峰震荡：
- **并发探路许可决策**：
  - **关闭主动探测时 (默认)**：半开状态并发真实请求许可数为 **1**。只放行 1 个真实流量去主节点探路，其它所有的并发真实流量依然会被隔离并路由至备节点。当该探路请求完成后（无论 RecordSuccess 还是 RecordFailure），自动释放并发许可并流转状态。
  - **开启主动探测时**：半开状态并发真实请求许可数为 **0**。禁止一切真实用户的线上流量去主节点当探路炮灰，真实流量 100% 被隔离，完全交给后台探测协程在 3 次成功探活后通过 `Reset` 恢复为 Closed。
- **背景探活协程行为**：周期性地（以 5s 为最小步长）遍历所有支持探测（`health_check_url` 不为空）的端点。若开启了主动探测且端点不健康（Open/HalfOpen），则每 5s 发送 HTTP 探测，连续 3 次成功探测（2xx）后，调用 `cbManager.Reset` 强设熔断器状态为 Closed，实现流量平滑零抖动回切。

### 3. 数据模型设计

在 `core.Endpoint` 结构体和 `config.ResolvedEndpoint` 中增加 `Priority` 字段：
```go
type Endpoint struct {
	// ...
	Priority         int
	Metadata         map[string]string
	// ...
}
```

在 `local.yml` 配置文件中支持配置 `priority` 以及 `metadata`，例如：
```yaml
    endpoints:
      - provider: openai-official
        url: http://free3.900406.xyz:8100/v1
        real_model: gpt-4
        priority: 1
        weight: 100
        metadata:
          health_check_url: http://free3.900406.xyz:8100/v1/models
      - provider: openai-custom
        url: http://free3.900406.xyz:8100/v1
        real_model: gpt-4:free
        priority: 2
        weight: 100
```

## Consequences

* **彻底解决回切死锁**：即便在主备温备场景下流量完全切给备实例，熔断的健康主实例也会由于 5s 一次的高频主动探测，在连续 3 次成功后被强设为 Closed，使得网关将后续流量无缝拉回主通道，恢复最优高可用状态。
* **零表结构变更**：利用 `Metadata["health_check_url"]` 存储探测地址，实现了灵活的探测能力，不需要执行数据库的 Migration。
* **低负载自适应**：健康实例的探测频率为 30s，不对上游造成无意义的探测压力；不健康实例的 5s 快速探测提供了秒级的高可用恢复响应能力。
* **按需开启控制开销**：因为 5s 的高频主动探活在实例众多时会占用一定的上游 Token 或接口开销，设计为默认关闭，提供全局开关，由用户决策是否开启，实现了极好的资源安全性和开销控制。
