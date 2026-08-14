# tokenlive-gateway

LLM API 网关，提供统一的 OpenAI 兼容接口，将请求路由到多个上游 LLM 供应商。

## Language

**Model**:
用户请求的 LLM 模型，客户端用 `model_name` 标识。Model 是路由的一等入口（类似微服务的服务角色），通过 `requestTypes`（能力列表）声明自己支持的多项能力类型（如 `chat_completion`、`embedding`），并定义默认的物理容量上限（`context_length` 最大上下文窗口与 `max_output_tokens` 最大输出 Token）。每个 Model 持有一组 Endpoint，表达"这个模型可以通过哪些端点被服务"。
_Avoid_: litellm_model, upstream_model

**Provider**:
上游 LLM 来源的基础设施定义，包含默认协议类型（protocol）、认证凭证（api_key）、超时配置。Provider 不再持有 Endpoint 列表，只定义公共基础设施默认值，由 Endpoint 通过引用复用或单独覆盖。
_Avoid_: source, vendor, supplier

**Endpoint**:
路由的最小单元，每个 Endpoint 自描述完整的路由信息：关联的 Model、Provider、上游 URL、real_model、priority、weight、context_length、max_output_tokens。Endpoint 是 Model 和 Provider 的关联点，取代原 ModelProvider 的职责。Admin 将已合并的 Endpoint 数据写入 Redis，Gateway 按 Model 维度读取。
_Avoid_: model_provider, binding

**ModelProvider** _(已废弃)_:
已被 Endpoint 取消。原 Model 和 Provider 的关联关系，现由 Endpoint 承担。
_Avoid_: binding, mapping

**RequestType**:
请求类型枚举，如 `chat_completion`、`embedding`。Model 声明自己支持的能力（requestTypes）列表，Provider 声明自己支持的能力列表。路由时按此过滤。

**Fallback**:
跨模型降级策略。当请求的 model 不可用时，尝试降级到另一个 model**Policy**:
单次请求的已解析策略，挂在 GatewayContext 上。包含 Invoker 级参数（max_retries、lb_strategy、各种超时机制）和 Filter 级参数（rate_limit、permissions）。**在传输契约上，网关与 AdminProject 统一使用下划线（snake_case）命名风格，同时表中的 Params 配置项直接使用原生 JSON 对象传递（非转义字符串），从而实现零额外映射开销的极简共享。**
_Avoid_: resolved policy, strategy config

**TTFT (Time to First Token)**:
首字响应时间/首字超时。对流式请求而言，指发送请求到收到第一个流式 Token 的最大允许延迟。用来监控上游 LLM 冷启动或过载挂起。在度量指标（Metrics）采集上，TTFT 仅在成功输出首个流式 Token 时被激活并汇报（即 `gctx.TTFT > 0` 且 `gctx.IsStream = true`）。任何在流建立前被直接拒绝、拦截或报错的请求，其异常耗时不计入 TTFT 监控，只体现在常规的错误吞吐指标中。

**Token Estimator**:
Token 估算器。用于在请求进入网关时对 Prompt 进行 Token 数量预估，支持基于字符长度比例的简单估算 (`length_ratio`)，或引入具体模型的分词器（如 `tiktoken`、`llama-tokenizer`）进行精确预估。

**Token Settlement (Token 最终结算)**:
在 Outbound 过滤器阶段对实际 Token 消耗进行核销并扣减 Credits。若流式请求中途中断导致未能获取上游官方 `usage` 字段，系统将触发**字数估算降级（Length Estimation Fallback）**：利用 SSE 拦截器累计统计已发送至客户端的字符数，按模型预设比率估算 Completion Token，以此作为最终值进行指标上报与 Credits 余额扣减，规避网关计费漏扣风险。Token 提取由统一的 `TokenExtractor`（`func(data string)(in,out,cached,cacheCreation int)`，同一个提取器既吃单个 SSE 事件帧、也吃整段非流式响应体）与写入函数 `ApplyUsage`（带 `>0` 守卫，防跨帧 0 值覆盖真值）承担，流式（`SSEInterceptWriter`）与非流式（各 Provider 的 nonStream handler）共用同一套提取与写入语义，不各自内联重写。
_【架构红线】监控指标（Metrics）仅用于实时大屏和运维告警展示，严禁将 Prometheus 指标（如 `gateway_cost_total` 等）用作计费对账和账单核销的真实依据。所有计费、扣费结算必须强一致地依赖写入 ClickHouse 的结构化访问日志（Access Log）核算，同时必须通过 Redis 补偿队列机制确保 ClickHouse 故障时数据的零丢失与最终一致性。_

**Credits (积分/余额)**:
个人 API Key 的充值可用额度余额。单位为微元（定点整数，1 元 = 1,000,000 微元），以规避浮点数并发计算的精度丢失。Credits 仅对个人用户（UserID != ""）生效，预检时若其值小于等于 0 则进行拦截。
_Avoid_: quota, limit, token pool

**Tenant Metrics Whitelist (租户指标白名单)**:
用于防止 Prometheus 指标基数（Cardinality）爆炸的流量监控管理机制。网关通过去中心化的策略元数据驱动（Metadata Flag）：当解析得到的租户策略配置包含 `enable_metrics_reporting: true` 时，该请求在运行期被染色，并在 Outbound Filter 上报真实租户名（`tenant`）；未开启的租户请求指标统一合并标记为 `others`。以此实现满足重点客户监控与保护 Prometheus 稳定性之间的平衡。

**Circuit Breaker State Metrics (熔断状态指标导出)**:
在 Prometheus 中实时反映底层 Endpoint 或服务渠道熔断状态（Closed/Open/Half-Open）的监控度量。为了解决惰性熔断器无请求时不触发状态更新的问题，网关采用**被动触发 + 定期刺探（Event-Driven + Tick Probe）**的设计：在发生熔断判定的瞬时直接写 Gauge 指标，并在后台运行轻量级定时探测协程，定期计算并刷新处于隔离状态的熔断实体，保障 Grafana 大屏指标的实时性。

**Cost Limiter**:
消费额度限制器。基于大模型调用计费价格（Billing Policy）以及实际扣减额度，进行单日或单月维度的最大消费限额（USD/CNY 厘维度）流量控制。


**TTFT Slow Call**:
首字慢调用。熔断检测的一种统计指标。指流式响应中从请求发出到收到第一个 Token 的延迟（TTFT）超过了配置阈值（如 3000ms），则被计为一次慢调用，用以规避常规大模型生成时间长导致的误熔断。

**OpenAI Degrade Response**:
降级错误响应。指当上游模型或服务被熔断且无可用的跨模型降级路线时，网关返回给客户端的标准 OpenAI 兼容的 JSON 错误报文。

**Protocol Translation (协议翻译/转换)**:
指网关在 Provider / RequestInvoker 侧执行的跨协议格式转换（包括请求体翻译与响应体翻译）。短期以 **OpenAI Chat Completions 为中间枢纽（Chat hub）**，两对协议各自双向映射、不统一 content-block IR：
1. **Messages ↔ Chat**（Anthropic Messages API ↔ Chat Completions）— 纯函数落点 `pkg/llm/translate`：`MessagesRequestToChat` / `ChatRequestToMessages` / `ChatCompletionToMessages` / `MessagesToChatCompletion`。OpenAI Provider 的 messages 路径与 JoyCode 的 Claude 路径共用同一内核。
2. **Responses ↔ Chat**（OpenAI Responses API ↔ Chat Completions）— 纯函数落点 `pkg/llm/translate`：`ResponsesRequestToChat` / `ChatCompletionToResponses` / `CorrectNativeResponsesRequest`（含 tools 规范化）。流状态机 `handleResponsesStream` 仍在 Provider 侧。
流式：Messages→Chat 由 `translate.MessagesToChatStream`（含 tool_use / input_json_delta / finish_reason）；Chat→Messages / Responses 流状态机仍在 Provider 侧。
_Avoid_: 统一 IR、在 Pipeline Filter 做翻译

**SSE Stream Translation (流式事件翻译)**:
指在流式（SSE）传输过程中，网关通过 `SSEParser` 拦截并实时解包上游的 SSE 事件对象，经过结构映射和字段翻译后，重新序列化并逐帧下发给客户端，以维持客户端的协议契约与流式低时延体验。

**Upstream Call (上游调用)**:
指 Provider / RequestInvoker 打向上游 LLM 物理端点的统一 HTTP 传输模块（落点 `pkg/llm/upstream`）。职责边界：**只到拿到成功的 `*http.Response`**——解析 Policy 超时（Total / first-byte / Idle）、合并 UA + Endpoint.Headers + InjectedHeaders、执行 POST、写入 `gctx.UpstreamResponse`、对 status≥400 读 body 写入 `gctx.UpstreamBody` 并返回错误、流式时校验 SSE content-type。**不负责**协议鉴权头（由调用方填入 `Request.Header`）、请求体协议特化（如 `stream_options`）、以及响应体消费（透传流 / 协议翻译 / Token 提取）。流式 body 生命周期由 `StreamDisposition` 表达：`Consume` 表示调用方会读完并关闭；`Handoff` 表示 body 移交给翻译 invoker，cancel 绑定到 `Close`，传输层不 defer 关闭。一期覆盖 OpenAI / Anthropic / Gemini；JoyCode 后置挂接。
_Avoid_: doRequest, transport, http helper

**Request Dyeing (请求染色)**:
属于元数据丰富与状态标记。通过 `TaggingPolicy` 规则对入站请求进行特征判定（如匹配模型名称、请求头、系统参数等），并在运行期上下文 `GatewayContext.Tags` 中注入对应的染色标签，供后续组件消费。

**Traffic Routing (流量筛选)**:
属于路由过滤与导流。基于染色标签（Tags）和请求上下文，匹配 `RoutePolicy` 规则，动态筛选出满足特定硬约束的下游 Endpoint 或 Provider 候选集，并按权重分配流量。

**TagRouter (染色与路由筛选器)**:
高精度请求级动态标签路由筛选器（前身是 DynamicRoutePolicyRouter，重构后合并命名为 TagRouter）。在 `ClusterInvoker` 路由链中运行的流量筛选器，动态匹配已合并的 `RoutePolicies`，基于请求的染色标签（`gctx.Tags`）过滤符合特定 `Destination` 约束 of 下游 Endpoint，并在目标通道故障时触发降级逃生，回退到默认候选池。

**ConfigSource**:
配置数据的来源层。YAML 是默认层（Default Layer），Redis 是覆盖层（Override Layer）。同一个 model 两层都有时用 Redis 的，只有 YAML 有的保留 YAML 的。
_Avoid_: config provider, data source

**AdminProject**:
独立的后台管理工程，通过管理 API 维护 models/providers/endpoints 以及各项 Policy 配置与绑定。**当策略本身或绑定发生变动时，AdminProject 负责反查受影响的维度，并将多表数据重算聚合成大 Policy JSON 结构，即时重写覆盖或 HDel 移除对应的 Redis 散列缓存（`aigw:policies:*`）**。网关不直接操作配置，只从 Redis 读取。
_Avoid_: admin service, management backend

## Relationships

- 一个 **Model** 持有一组 **Endpoint**，表达"这个模型可以通过哪些端点被服务"
- 一个 **Endpoint** 可在关联 Model 和 Provider 的基础上，选择性覆盖 provider 级的 protocol 和 api_key 等基础设施属性，是路由的最小单元
- 一个 **Provider** 可被多个 **Endpoint** 引用，定义基础设施默认属性（protocol、api_key、timeout）
- **Fallback** 关联到用户 and Model，表达该用户请求该 model 时的降级链
- **Pipeline** 按 RequestType 构建（非 per-model），从 YAML 配置启动时 eager 构造，模型级差异由 **PolicyMatcher** 运行时注入
- **PolicyMatcher** 主数据源为 Redis（四级优先），YAML 扁平规则做兜底，版本轮询 30s 热更新
- **Auth 分层**：Gin middleware 提取 user（认证），Engine auth Filter 做授权检查（post-policy，读 gctx.Policy）
- **AdminProject** 写入 Redis（`aigw:config:endpoints:{modelName}` 以及 `aigw:policies:*`），网关从 Redis 读取 Endpoint 数据和 Policy 配置（版本轮询）统计指标。指流式响应中从请求发出到收到第一个 Token 的延迟（TTFT）超过了配置阈值（如 3000ms），则被计为一次慢调用，用以规避常规大模型生成时间长导致的误熔断。

**OpenAI Degrade Response**:
降级错误响应。指当上游模型或服务被熔断且无可用的跨模型降级路线时，网关返回给客户端的标准 OpenAI 兼容的 JSON 错误报文。

**Request Dyeing (请求染色)**:
属于元数据丰富与状态标记。通过 `TaggingPolicy` 规则对入站请求进行特征判定（如匹配模型名称、请求头、系统参数等），并在运行期上下文 `GatewayContext.Tags` 中注入对应的染色标签，供后续组件消费。

**Traffic Routing (流量筛选)**:
属于路由过滤与导流。基于染色标签（Tags）和请求上下文，匹配 `RoutePolicy` 规则，动态筛选出满足特定硬约束的下游 Endpoint 或 Provider 候选集，并按权重分配流量。

**TagRouter (染色与路由筛选器)**:
高精度请求级动态标签路由筛选器（前身是 DynamicRoutePolicyRouter，重构后合并命名为 TagRouter）。在 `ClusterInvoker` 路由链中运行的流量筛选器，动态匹配已合并的 `RoutePolicies`，基于请求的染色标签（`gctx.Tags`）过滤符合特定 `Destination` 约束的下游 Endpoint，并在目标通道故障时触发降级逃生，回退到默认候选池。

**ConfigSource**:
配置数据的来源层。YAML 是默认层（Default Layer），Redis 是覆盖层（Override Layer）。同一个 model 两层都有时用 Redis 的，只有 YAML 有的保留 YAML 的。
_Avoid_: config provider, data source

**AdminProject**:
独立的后台管理工程，通过管理 API 维护 models/providers/endpoints 配置，将已合并的扁平 Endpoint 数据（`[]ResolvedEndpoint`）按 model 维度写入 Redis。网关不直接操作配置，只从 Redis 读取。
_Avoid_: admin service, management backend

**Endpoint Affinity (端点亲和性/会话粘性)**:
打分与筛选函数中的选项，用于让同一逻辑会话（例如通过 `conversation_id` 或 Prompt 前缀指纹关联）倾向于路由到上一次命中的 Endpoint。其核心目的是最大化命中上游物理节点的 **Prompt Cache（提示词缓存）**，从而降低首字延迟（TTFT）并节省 Token 成本。在具体实现中，它既可以表现为增加打分权重的“软粘性”，也可以通过配置控制强制锁定。
_Avoid_: session affinity, sticky routing

**Exclude Failed Endpoint (排除失败端点控制)**:
重试策略中用于控制失败端点生命周期的参数（如 `exclude_failed_endpoint` 字段）。当设为 `true`（默认值）时，请求失败重试会触发故障转移（Failover），将该端点临时排除，漂移到其他端点；当设为 `false` 时，系统将触发原地重试，退避后仍在原端点上重试，常用于保留端点亲和性下的 Prompt Cache，必须配合指数退避以防重试风暴。

**Fatal Error (致命错误)**:
服务治理流中的一种阻断性异常标记。当触发某些不可恢复的故障（例如强端点亲和性强制要求且不允许降级，但路由匹配失败）时，系统会在请求上下文（`GatewayContext.FatalErr`）中写入致命错误（如 `ErrFatalNoAvailableEndpoint`）。该错误一经标记，将立刻短路重试引擎与降级引擎，立即终止单次请求内的所有重试 Attempt 并跳过 Fallback 链，直接向客户端返回错误，以防止模型污染和无谓的重试开销。


## Relationships

- 一个 **Model** 持有一组 **Endpoint**，表达"这个模型可以通过哪些端点被服务"
- 一个 **Endpoint** 可在关联 Model 和 Provider 的基础上，选择性覆盖 provider 级的 protocol 和 api_key 等基础设施属性，是路由的最小单元
- 一个 **Provider** 可被多个 **Endpoint** 引用，定义基础设施默认属性（protocol、api_key、timeout）
- **Fallback** 关联到用户和 Model，表达该用户请求该 model 时的降级链
- **Pipeline** 按 RequestType 构建（非 per-model），从 YAML 配置启动时 eager 构造，模型级差异由 **PolicyMatcher** 运行时注入
- **PolicyMatcher** 主数据源为 Redis（四级优先），YAML 扁平规则做兜底，版本轮询 30s 热更新
- **Auth 分层**：Gin middleware 提取 user（认证），Engine auth Filter 做授权检查（post-policy，读 gctx.Policy）
- **AdminProject** 写入 Redis（`aigw:config:endpoints:{modelName}`），网关从 Redis 读取 Endpoint 数据和 Policy 配置（版本轮询）
- **ConfigSource** 分两层：YAML（默认）→ Redis（覆盖），Redis 不可用时回退到 YAML
- **Exclude Failed Endpoint** 控制重试路由时是否允许端点漂移。当其为 `false`（原地重试）时，必须与指数退避配合，且重试前需校验端点的熔断器状态（熔断优先原则）
- **Fatal Error**（例如亲和性拦截触发的 `ErrFatalNoAvailableEndpoint`）一旦在 LBS 或 Invoker 阶段被写入上下文，将绕过所有重试与 Fallback，直接向客户端返回该错误
- **Upstream Call** 被 OpenAI / Anthropic / Gemini 的 RequestInvoker（及 Provider 内 HTTP 出口）共用；调用方只提供 URL、Body、鉴权 Header、StreamDisposition；超时与 headers 合并、4xx 副作用集中在 `pkg/llm/upstream`
- **Protocol Translation** 与 **SSE Stream Translation** 消费 Upstream Call 返回的 `*http.Response`（常配合 `Handoff`），不反向渗入传输层的 RequestType 特判
- **Messages ↔ Chat** 双向非流翻译集中在 `pkg/llm/translate`；RequestInvoker 只负责 probe 短路、调 Upstream Call、写 gctx token/响应头


## Example dialogue

> **Dev:** "gpt-4 请求进来后，怎么决定走 openai-official 还是 azure-openai？"
> **Arch:** "按 model 维度从 Redis（或 YAML）查所有 Endpoint，按 priority 排序，同 priority 内按 weight 做负载均衡选择。每个 Endpoint 自带 provider 信息和上游 URL，直接可用。"

> **Dev:** "如果 gpt-4 的所有 endpoint 都失败了呢？"
> **Arch:** "先查 user_model_fallbacks 看该用户有没有自定义降级链，没有就走 YAML 里的全局 fallbacks 配置。比如降级到 gpt-3.5-turbo，再从它的 endpoint 列表里选。"
