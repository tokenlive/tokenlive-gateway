# 0016. 静态 RetryConfig 与动态 RetryPolicy 的 1:1 完美扁平对齐

## 状态

已接受 (Accepted)

## 上下文

在此前的设计中，tokenlive-gateway 存在两套定义不对称的重试配置结构：

1. **静态网关配置层 (`core.RetryConfig`)**：属于代码级硬编码或本地 YAML 配置加载的重试机制，最初采用了层级嵌套的设计（如包含 `BackoffConfig` 结构体和嵌套的 `RetryRuleConfig` 数组）。
2. **治理策略策略层 (`policy.RetryPolicy`)**：属于控制台或控制流动态下发的重试策略机制，结构体声明相对扁平，包含 `BackoffType`、`BaseMs`、`ErrorCodes`、`ErrorMessages` 等字段。

两套不对称的结构导致了多项弊端：

- 重试判定与退避延迟计算的逻辑存在冗余副本，增加了网关系统的理解成本与维护难度。
- 在对齐策略字段时需要执行繁琐的映射转换，且在动态与静态策略的错误匹配中可能会出现行为不一致的隐患。

## 决策

为了消除字段的不对称并彻底收拢重试逻辑，我们做出了以下重构决策：

1. **扁平化结构对齐 (1:1)**：
   - 将静态配置 `core.RetryConfig` 与控制台策略 `policy.RetryPolicy` 的结构进行 1:1 的完美对齐。
   - 静态配置 `core.RetryConfig` 移除原本的嵌套结构（`BackoffConfig` 和 `RetryRuleConfig`），转而使用扁平化的字段：
     - `Retry` (int)：重试次数。
     - `BackoffType` (string) 和 `BaseMs` (int)：退避延迟配置。
     - `ErrorCodes` ([]string) 和 `ErrorMessages` ([]string)：匹配重试的错误条件。
     - `CodePolicy` 和 `MessagePolicy` (*ErrorParserPolicy)：自定义错误解析器。
     - `ConnectTimeout`、`TtftTimeout` 和 `TotalTimeout` (int)：各阶段超时时间。

2. **去除历史兼容旧字段**：
   - 彻底废弃并移除 `policy.RetryPolicy` 原有的旧字段，包括 `Interval`、`Timeout`、`exceptions` 以及 `TTFTimeout`（统一重命名为 `TtftTimeout` 以规范化拼写）。
   - 反序列化逻辑 (`UnmarshalJSON`) 专注解析驼峰字段的向下兼容（如 `backoffType`、`baseMs`、`ttftTimeout`、`totalTimeout`），不再对旧废弃字段（如 `interval`、`timeout`）做转换，精简了反序列化过程。

3. **重试逻辑与错误解析归一**：
   - 静态重试执行器 `RetryStrategy` 完全实现统一的 `policy.ErrorPolicy` 接口。
   - 静态重试规则判定 (`ShouldRetry`) 彻底代理并归拢到 `policy.MatchErrorWithReason(...)` 通用方法中，消除判定分支的行为差异。
   - 退避延迟计算统一采用新增的 `policy.RetryPolicy.CalcBackoff(attempt)`，以支持 `fixed` (固定延迟) 与 `exponential` (带 jitter 的指数退避)。

## 后果

- **降低维护成本**：网关的本地配置重试与动态下发策略重试的逻辑完成 100% 收拢，测试用例和生产代码无需再编写中介转换逻辑。
- **提升健壮性**：去除了所有遗留的旧字段兼容包袱，确保配置层、策略层以及各 Provider 调用的接口字段（如 `TtftTimeout`）完全一致。
- **性能优势**：精简了自定义的 JSON 反序列化逻辑，提高了策略评估效率，且消除了并发异步更新 EMA 时产生的潜在测试竞争问题。
