# ADR-0012: 指标监控与遥测重构设计 (Metrics & Telemetry Redesign)

## 状态

已通过决策 (2026-06-04)

## 背景

tokenlive-gateway 目前的可观测性设计存在几点缺陷：

1. **指标冗余与混乱**：在 Gin 中间件 (`internal/middleware/metrics.go`) 和管道 Outbound 过滤器 (`pkg/filters/outbound/metrics.go`) 中存在两套重合的指标，且 Gin 层的一些 LLM 统计函数已不再使用。
2. **TTFT 监控不精确**：当前的 `gateway_request_duration_seconds` 指标将流式首字延迟与总延迟合并在一个 Histogram 中，这导致两者由于量级差异较大导致 Bucket 划分冲突。
3. **流式网络中断对账漏洞**：如果流式请求在中途因为网络异常中断，由于网关无法接收到上游最后一个 SSE 帧携带的官方 `usage` 字段，这会导致 `gateway_tokens_total` 指标统计丢失，且 `TokenSettlementFilter` 会全额退还预扣除的额度，引发网关垫付费用的经济损耗。
4. **指标基数爆炸风险**：为了满足多租户的财务账单和实时监控需要，在 Prometheus 指标中需要按租户进行区分，但这会引起指标的基数（Cardinality）激增，威胁 Prometheus 的稳定性。

## 决策

### 1. 指标收拢与时延指标解耦

* **决策**：清理 Gin 中间件中闲置未调用的 LLM 相关指标，收拢指标定义至 Pipeline Outbound Filter 层。
* **独立 TTFT 指标**：引入独立的 `gateway_ttft_seconds` Histogram，专门为流式首字耗时配置小颗粒度的 Buckets（如 `[0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0]`），与总延时 `gateway_request_duration_seconds` 的 Buckets 进行隔离。

### 2. 状态同步与最终收割机制 (State Holder & Final Harvest)

为了精确采集流式首字节延迟（TTFT）并容忍中途网络断连：

* **业务执行期**：`sse_intercept_writer` 在检测到首字节时，仅计算耗时并更新到贯穿全链路的上下文 `gctx.TTFT` 中，不直接调用 Prometheus API。
* **最终收割期**：无论请求是成功还是发生各种网络异常，网关引擎都会完整执行 Outbound 过滤器。`MetricsFilter` 作为收割者，检查发现 `gctx.IsStream && gctx.TTFT > 0` 时，统一向 Prometheus 上报这笔首字延迟。若建连前即失败（未产生首字节），则 TTFT 保持为 0，将其排除在统计之外。

### 3. 流式中断 Token 结算与字数估算降级 (Length Estimation Fallback)

* **策略**：当发生流式传输中断且 `gctx.CompletionTokens == 0` 时，触发字数估算降级机制。
* **实现**：
  1. `sse_intercept_writer` 在流传输时，实时解码并累加已下发至客户端的增量文本字符数到 `gctx.TransmittedChars`。
  2. 在 Outbound 过滤器（`token_settlement` 和 `metrics`）执行时，若发现 `gctx.IsStream` 且 `gctx.CompletionTokens == 0`，但 `gctx.TransmittedChars > 0`，则按照模型预设的比例（例如：1 字符 ≈ 0.6 Token）计算出估算的 `CompletionTokens`，以此值作为最终结算和指标上报的依据。

### 4. 租户指标基数控制（去中心化元数据标记）

* **策略**：在 Prometheus 指标中增加 `tenant` 标签，采用去中心化的 **Metadata Flag 染色驱动** 进行基数防护。
* **实现**：网关在加载解析租户 Policy 时，如果租户配置中带有 `enable_metrics_reporting: true` 标记，则该请求在 Context 中进行染色，并在 Outbound Filter 上报真实租户名；未开启该标志的租户指标在 Prometheus 侧统一合并上报为 `others`。

### 5. 指标与计费强隔离红线

* **决策**：树立架构红线，Prometheus 指标（如 `gateway_cost_total`）**仅用于实时监控展示与告警**，绝对不能用作任何财务计费账单、额度结算的依据。所有的扣费与结算对账，必须**强一致**地完全依赖 `Redis/StateStore` 扣减记录或离线分析落盘的结构化 `Access Log`。

## 影响分析

1. **性能**：
   * 状态记录改为内存变量修改，只在最后触发一次指标写入，性能有所提升。
   * 实时解析 content 增加了少量 CPU 开销，但仅对流式响应的微小 json 文本片段进行，开销可忽略。
2. **容量**：
   * 过滤了大量普通租户的 Prometheus Label，阻断了 Prometheus 内存泄露和基数爆炸。
3. **一致性**：
   * 极大降低了因为流式网络中断而给网关平台带来的 Token 计费资损风险。
