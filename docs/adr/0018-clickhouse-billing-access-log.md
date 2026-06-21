# 0018-基于 ClickHouse 与 Redis 补偿队列的计费及访问日志设计

## 背景

在原本的设计中，网关的消费额度和计费对账主要强一致依赖 Redis (StateStore) 以及普通的本地文件结构化日志。然而，随着业务规模的增长，本地文件日志收集较为分散，缺乏高性能的分析及查询手段；同时，Prometheus 指标不能用于财务结算。
为了支持高精度的租户账单核算、实时大屏性能指标展示、以及故障排查，我们需要将网关每一次的请求记录（Access Log）实时、结构化地落库到 ClickHouse 中。

由于计费和财务对账被调整为强一致依赖 ClickHouse，数据在落库过程中的高可用与“零丢失”成为了核心设计红线。

## 架构决策

我们做出了以下几项核心架构抉择：

1. **写入通道**：
   网关采用**直连 ClickHouse 批量写入**的模式，不再引入单独的 MQ 中间件（如 Kafka）。这能够让网关系统的整体依赖保持精简，降低运维负担。
   
2. **内存批量缓冲 (Batcher)**：
   网关内部引入一个带缓冲的后台异步 Batcher 模块，通过 `select + channel + ticker` 结构，提供双重触发阈值：
   - `BatchSize` 默认 2000 条。
   - `FlushInterval` 默认 2 秒。
   
3. **数据可靠性与失败补偿 (Critical 级别)**：
   由于计费强依赖落库，`AccessLogFilter` 的可靠性由 `BestEffort` 升级为 `Critical`。
   - **拥堵与故障降级**：当 ClickHouse 出现连接超时、宕机或批量写入失败时，Batcher 会将这批请求记录整体打包，**投递至网关现有的 Redis 补偿队列 (`pkg/compensation`)**。
   - **最终一致性**：后台的 Consumer 协程会定期读取 Redis 补偿队列中的积压数据，进行指数退避重试，直到成功写入 ClickHouse，保障数据的最终一致性。
   - **防重入**：因为重试可能会造成少量重复写入，ClickHouse 端的表使用 `ReplacingMergeTree` 引擎进行去重。

4. **网关集成方式**：
   直接在现有的 `AccessLogFilter` 中进行重构扩展，避免修改 `wire.go` 依赖和多套环境配置文件（`local.yml` 等）。
   - 将其重构为双 Writer 设计：一条数据进入后，同时分发到 `ZapConsoleWriter`（打印到 stdout）和 `ClickHouseBatchWriter`（投递到内存 Buffer）。
   - 接入网关的优雅停机流程，在进程下线前，关闭接收 Channel 并强制 Flush 内存残存的所有日志。

## ClickHouse 表结构设计

### 1. 访问日志明细表 (`access_logs`)

* **引擎**：`ReplacingMergeTree(time)`
* **分区**：`PARTITION BY toYYYYMMDD(time)`（按天分区，便于快速的 TTL 物理清理）
* **排序键**：`ORDER BY (tenant_id, model, request_id, time)`

```sql
CREATE TABLE IF NOT EXISTS access_logs (
    -- 请求唯一标识与时间
    request_id String,
    time DateTime64(3) CODEC(DoubleDelta, LZ4),
    
    -- 租户与用户信息
    tenant_id LowCardinality(String),
    user_id LowCardinality(String),
    session_id String,
    api_key String,
    client_ip String,
    
    -- 模型与路由信息
    original_model LowCardinality(String),
    model LowCardinality(String),
    provider LowCardinality(String),
    endpoint_id LowCardinality(String),
    is_stream UInt8,
    attempts UInt8,
    fallback_chain Array(String),
    
    -- 性能度量
    status_code Int16,
    latency_ms UInt32,
    ttft_ms UInt32,
    error_message String,
    
    -- 计费与 Token 结算（使用 Decimal 避免精度丢失）
    input_tokens UInt32,
    output_tokens UInt32,
    cached_tokens UInt32,
    cache_creation_tokens UInt32,
    cost Decimal(18, 9)
) 
ENGINE = ReplacingMergeTree(time)
PARTITION BY toYYYYMMDD(time)
PRIMARY KEY (tenant_id, model, request_id)
ORDER BY (tenant_id, model, request_id, time)
TTL time + INTERVAL 90 DAY;
```

### 2. 租户账单小时级聚合表 (`tenant_billing_hourly`)

* **引擎**：`SummingMergeTree`
* **用途**：提供秒/毫秒级的多维度月度/天级账单对账查询。

```sql
CREATE TABLE IF NOT EXISTS tenant_billing_hourly (
    tenant_id LowCardinality(String),
    model LowCardinality(String),
    time_hourly DateTime,
    
    request_count UInt64,
    input_tokens UInt64,
    output_tokens UInt64,
    cached_tokens UInt64,
    cost Decimal(18, 9)
) 
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(time_hourly)
ORDER BY (tenant_id, time_hourly, model)
TTL time_hourly + INTERVAL 730 DAY; -- 保存两年

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_billing_hourly 
TO tenant_billing_hourly AS 
SELECT 
    tenant_id,
    model,
    toStartOfHour(time) AS time_hourly,
    count() AS request_count,
    sum(input_tokens) AS input_tokens,
    sum(output_tokens) AS output_tokens,
    sum(cached_tokens) AS cached_tokens,
    sum(cost) AS cost
FROM access_logs
GROUP BY tenant_id, model, time_hourly;
```

### 3. 服务性能监控分钟级聚合表 (`endpoint_metrics_minute`)

* **引擎**：`SummingMergeTree`
* **用途**：提供 Grafana 或网关控制台高精度的分钟级趋势监控和熔断诊断。

```sql
CREATE TABLE IF NOT EXISTS endpoint_metrics_minute (
    model LowCardinality(String),
    provider LowCardinality(String),
    endpoint_id LowCardinality(String),
    time_minute DateTime,
    
    total_count UInt64,
    success_count UInt64,
    stream_count UInt64,
    latency_sum UInt64,
    ttft_sum UInt64
) 
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(time_minute)
ORDER BY (model, provider, endpoint_id, time_minute)
TTL time_minute + INTERVAL 7 DAY; -- 分钟级保存7天即可

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_endpoint_metrics_minute 
TO endpoint_metrics_minute AS 
SELECT 
    model,
    provider,
    endpoint_id,
    toStartOfMinute(time) AS time_minute,
    count() AS total_count,
    sum(status_code = 200) AS success_count,
    sum(is_stream) AS stream_count,
    sum(latency_ms) AS latency_sum,
    sum(ttft_ms) AS ttft_ms
FROM access_logs
GROUP BY model, provider, endpoint_id, time_minute;
```

### 4. 服务性能监控小时级聚合表 (`endpoint_metrics_hourly`)

* **引擎**：`SummingMergeTree`
* **用途**：提供长达一年以上的宏观性能报表。

```sql
CREATE TABLE IF NOT EXISTS endpoint_metrics_hourly (
    model LowCardinality(String),
    provider LowCardinality(String),
    endpoint_id LowCardinality(String),
    time_hourly DateTime,
    
    total_count UInt64,
    success_count UInt64,
    stream_count UInt64,
    latency_sum UInt64,
    ttft_sum UInt64
) 
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(time_hourly)
ORDER BY (model, provider, endpoint_id, time_hourly)
TTL time_hourly + INTERVAL 365 DAY; -- 保存一年

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_endpoint_metrics_hourly 
TO endpoint_metrics_hourly AS 
SELECT 
    model,
    provider,
    endpoint_id,
    toStartOfHour(time) AS time_hourly,
    count() AS total_count,
    sum(status_code = 200) AS success_count,
    sum(is_stream) AS stream_count,
    sum(latency_ms) AS latency_sum,
    sum(ttft_ms) AS ttft_ms
FROM access_logs
GROUP BY model, provider, endpoint_id, time_hourly;
```
