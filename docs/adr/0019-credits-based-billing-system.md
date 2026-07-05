# ADR 0019: Credits-Based Billing System for Personal Users

## Context (上下文)

在之前的设计中，个人 API Key 的配额控制使用的是“等效 Token 方式”（Equivalent Token）。其扣减公式是将单次请求的实际费用折算为当前被调用模型的“输入等效 Token”数量后，从 API Key 的 `quota` 字段中扣减。

然而，这种设计存在两个主要局限性：
1. **多模型计费漏洞**：由于折算等效 Token 时分母使用了当前模型的输入单价，导致调用便宜模型（如 GPT-3.5）和调用昂贵模型（如 GPT-4）在消耗同等物理 Token 时扣减的 `quota` 额度相同。这在同一个 API Key 允许跨模型调用的场景下，无法拉开真实的收费差距。
2. **语义混淆**：把金额折算为 Token 额度对管理员和最终用户来说理解成本高，且使得“配额”成为了一个黑盒状态。

我们需要将个人用户的计费限额机制重构为通用的 **Credits（积分/余额）方式**，使得一个 API Key 能在多模型间安全、等价、高并发地扣除真实消费。

## Decision (决定)

我们决定将个人 API Key 的限额机制由“等效 Token”升级为“Credits 余额制”。核心设计如下：

### 1. 存储单位与精度（微元定点数）
为避免高并发计算和累加时产生浮点数精度丢失，我们**拒绝直接使用 `float64` 存储金额**。
We continue to use the `int64` type in databases, Go structs, and Redis, but define Credits balance as **“micro-yuan”** ($1\text{ Yuan/USD} = 1,000,000\text{ micro-yuan}$, or **Micro-Credits**).
* 例如，用户充值 $10\text{ 元}$，在 Redis 中写入的余额字段值为 `10,000,000`。

### 2. 字段重构
由于该项目为全新项目，无需考虑历史老数据兼容。我们将整个网关层（包括代码、配置文件、Redis 散列键等）所有原名为 `quota`/`Quota` 的属性与变量彻底重构更名为 **`credits`/`Credits`**。

### 3. Inbound 拦截逻辑
网关在 `OnRequest` 拦截阶段（Inbound Filter）进行预检：
* 仅针对个人用户（`UserID != ""`）进行预检，租户级免除拦截。
* 不引入复杂的“请求最小额度预估拦截”算法（防止因 Output 未知导致误拦截），仅做简单的 `Credits <= 0` 判定。
* 如果用户当前 `Credits <= 0`，拦截并返回 HTTP `429 Too Many Requests` 以及错误说明。如果 `Credits > 0` 则放行，允许单次请求超支扣成负数。
* `Credits = -1` 仍保留为“无限额度”的特殊标示。

### 4. Outbound 结算扣减
请求完成时（Outbound Filter），网关根据具体的缓存命中情况与四段价格计算得出请求实际消费 `costYuan`（元），并按如下公式将其转换为微元进行扣减：
$$\text{creditsToDeduct} = \lfloor \text{costYuan} \times 1,000,000 + 0.5 \rfloor$$
* 若扣减额度 `creditsToDeduct <= 0`（完全免费请求），允许扣减 `0`。
* 扣减成功后，网关会强制清除该 API Key 在本地的 LRU 内存缓存，确保下一次请求动态回源读取最新 Credits。

## Consequences (后果)

* **优点**：
  * 支持同一个 API Key 跨模型消费时的精确计费，GPT-4 和 GPT-3.5 能按照其自身的实际单价扣除对应 Credits。
  * 完美规避了高并发下 Redis 浮点数相减的精度风险，计费准确度达到 $10^{-6}$ 金额级别。
  * 彻底消除了原“等效 Token”在代码命名和业务认知上的技术债。
* **缺点**：
  * 在网关 Inbound 阶段不对“超额消费”进行严苛拦截，会产生小额的“超支/欠费”风险，这需要在后端的充值与风控逻辑中做进一步规避。
