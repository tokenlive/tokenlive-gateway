# ADR 0013: Retry Policy Alignment and Dynamic Error Parsing Matching

为了支持多语言服务治理体系（如 `joylive-agent`）的统一策略分发，并提升网关在大模型场景下的错误容错精度，我们将重试策略（`RetryPolicy`）与 Java 端进行对齐，并在网关执行期支持基于 `ErrorParserPolicy` 与 `errorMessages` 的动态响应/错误码解析重试判定。

## Context

在原设计中，网关的重试机制存在以下局限：
1. **策略结构不对齐**：Go 端的 `RetryPolicy` 中 `ErrorCodes` 为 `[]int`（仅限 HTTP 状态码），而 Java 端 (`joylive-agent`) 为 `Set<String>`，且 Java 端包含 `ErrorParserPolicy`（支持通过 JsonPath/Regexp 从响应体中提取内部 API 错误码）以及对 `errorMessages`（错误消息匹配）的支持。
2. **重试判定静态化**：网关的重试过滤（`ShouldRetry`）主要依赖于静态配置文件（`local.yml`）解析而成的 `RetryStrategy`，对于保存在 Redis/DB 里的请求级动态策略 `gctx.Policy.InvocationPolicy.RetryPolicy`，网关仅动态使用了其中的最大重试次数（`Retry`），未动态匹配其中的错误判定规则。

为了实现多端策略配置的一致性，我们需要：
1. 对齐网关端、管理后台端与 Java 端的 `RetryPolicy` 配置结构，使配置能通过 Redis/DB 零开销无缝互通。
2. 支持对历史已经写入 DB/Redis 的整型 `error_codes` 配置的向后兼容。
3. 允许在网关运行期动态执行响应体解析，根据用户在策略中自定义的 `JsonPath` / `Regexp` 来提取错误码或错误消息，从而判断是否触发重试（例如触发对 `"rate_limit_exceeded"` 或特定 `"timeout"` 消息的重试）。

## Considered Options

1. **静态且不兼容方案**：仅在 Go 结构体中添加最基础的字段对齐，暂不修改网关层执行逻辑，前台依然强制限制只能填写数字状态码。该方案无法解决复杂 API 错误判定（如 200 OK 响应中携带特定错误 JSON）的重试诉求。
2. **完整动态匹配与向下兼容方案（选中方案）**：
   - 将 `ErrorCodes` 字段类型变更为 `[]string`，并通过自定义 JSON 反序列化器兼容历史整型数组输入（如 `[500, 502]` 会自动展开为 `["500", "502"]`）。
   - 扩充并统一 `ErrorParserPolicy` 结构体，支持 `Parser`、`Expression`、`Statuses` 与 `ContentTypes`。同步重构熔断策略（`CircuitBreakPolicy`）中的 `CodePolicy`，使其一并使用该通用解析器。
   - 在网关的 `ClusterInvoker` 中集成 `MatchError` 判定。如果动态重试策略存在，优先通过解析器读取 `gctx.UpstreamBody` 并利用 `github.com/yalp/jsonpath` 提取目标值以匹配 `ErrorCodes` 或 `ErrorMessages`。
   - 更新管理端 Vue 页面展示，允许可视化配置重试消息与错误解析策略。

## Decision

我们决定采用**方案 2（完整动态匹配与向下兼容方案）**。

### 1. 动态决策执行流程
当请求调用发生错误时，在 `ClusterInvoker` 内部的重试判定流程如下：

```
                 调用失败 (err != nil 或 状态码 >= 400)
                              │
                              ▼
                 是否存在动态 RetryPolicy？
                    /                   \
                 (是)                   (否)
                  /                       \
                 ▼                         ▼
  执行动态 MatchError 判定          执行静态 ShouldRetry 判定
  - 检查 CodePolicy (提取并匹配)      (匹配 status_codes / error_rules)
  - 检查 MessagePolicy (提取并匹配)
  - 匹配 error_codes / error_messages
  - 匹配 exceptions
```

### 2. 核心 Go 数据结构对齐
```go
// ErrorParserPolicy 错误响应解析策略
type ErrorParserPolicy struct {
	Parser       string   `yaml:"parser" json:"parser"`
	Expression   string   `yaml:"expression" json:"expression"`
	Statuses     []string `yaml:"statuses" json:"statuses"`
	ContentTypes []string `yaml:"content_types" json:"content_types"`
}

// RetryPolicy 重试子配置
type RetryPolicy struct {
	Retry                int                `yaml:"retry" json:"retry"`
	Interval             int                `yaml:"interval" json:"interval"`
	Timeout              int                `yaml:"timeout" json:"timeout"`
	ErrorCodes           []string           `yaml:"error_codes" json:"error_codes"`
	ErrorMessages        []string           `yaml:"error_messages" json:"error_messages"`
	CodePolicy           *ErrorParserPolicy `yaml:"code_policy" json:"code_policy"`
	MessagePolicy        *ErrorParserPolicy `yaml:"message_policy" json:"message_policy"`
	Methods              []string           `yaml:"methods" json:"methods"`
	Exceptions           []string           `yaml:"exceptions" json:"exceptions"`
	Version              int64              `yaml:"version" json:"version"`
	ConnectTimeout       int                `yaml:"connect_timeout" json:"connect_timeout"`
	TTFTimeout           int                `yaml:"ttft_timeout" json:"ttft_timeout"`
	TokenIntervalTimeout int                `yaml:"token_interval_timeout" json:"token_interval_timeout"`
	TotalTimeout         int                `yaml:"total_timeout" json:"total_timeout"`
}
```

### 3. 向后兼容实现 (UnmarshalJSON)
```go
func (r *RetryPolicy) UnmarshalJSON(data []byte) error {
	type Alias RetryPolicy
	aux := &struct {
		ErrorCodes []json.RawMessage `json:"error_codes"`
		*Alias
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if len(aux.ErrorCodes) > 0 {
		r.ErrorCodes = make([]string, len(aux.ErrorCodes))
		for i, raw := range aux.ErrorCodes {
			var s string
			if err := json.Unmarshal(raw, &s); err == nil {
				r.ErrorCodes[i] = s
			} else {
				var val float64
				if err := json.Unmarshal(raw, &val); err == nil {
					r.ErrorCodes[i] = strconv.FormatFloat(val, 'f', -1, 64)
				} else {
					r.ErrorCodes[i] = string(raw)
				}
			}
		}
	}
	return nil
}
```

## Consequences

* **多语言治理模型深度互通**：网关、管理端与 Java 端的策略数据结构完全对齐，避免了不同侧解析或分发报错的问题。
* **业务级错误重试支持**：支持了在 upstream 状态码为 200 或任意非正常返回时，通过 JsonPath/Regexp 识别类似于 `"insufficient_quota"` 或特定慢调用错误进行精确重试，极大提升了大模型网关应对抖动与冷启动的韧性。
* **零配置迁移阻碍**：由于内置的兼容反序列化器，生产环境中历史配置数据无需做任何手动清洗或迁移，网关可平滑进行版本滚动升级。
