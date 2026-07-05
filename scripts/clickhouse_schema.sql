-- ClickHouse schema for access logs, billing rollups, and endpoint metrics.
-- Keep this file as the source of truth for ClickHouse table initialization.

CREATE TABLE IF NOT EXISTS access_logs (
    request_id String,
    time DateTime64(3) CODEC(DoubleDelta, LZ4),
    tenant_id LowCardinality(String),
    user_id LowCardinality(String),
    session_id String,
    api_key String,
    workspace_id LowCardinality(String),
    api_key_id LowCardinality(String),
    api_key_hash String,
    client_ip String,
    original_model LowCardinality(String),
    model LowCardinality(String),
    provider LowCardinality(String),
    endpoint_id LowCardinality(String),
    is_stream UInt8,
    attempts UInt8,
    fallback_chain Array(String),
    status_code Int16,
    latency_ms UInt32,
    ttft_ms UInt32,
    error_message String,
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

ALTER TABLE access_logs ADD COLUMN IF NOT EXISTS workspace_id LowCardinality(String) AFTER api_key;

ALTER TABLE access_logs ADD COLUMN IF NOT EXISTS api_key_id LowCardinality(String) AFTER workspace_id;

ALTER TABLE access_logs ADD COLUMN IF NOT EXISTS api_key_hash String AFTER api_key_id;

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
TTL time_hourly + INTERVAL 730 DAY;

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
TTL time_minute + INTERVAL 7 DAY;

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
    sum(ttft_ms) AS ttft_sum
FROM access_logs
GROUP BY model, provider, endpoint_id, time_minute;

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
TTL time_hourly + INTERVAL 365 DAY;

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
    sum(ttft_ms) AS ttft_sum
FROM access_logs
GROUP BY model, provider, endpoint_id, time_hourly;
