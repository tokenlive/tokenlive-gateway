package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)


// NewClickHouse 初始化 ClickHouse 连接客户端，支持在没有配置时平滑降级（返回 nil）。
func NewClickHouse(conf *viper.Viper, l *log.Logger) (clickhouse.Conn, func(), error) {
	zapLogger := l.Logger

	if !conf.IsSet("data.clickhouse") {
		zapLogger.Info("ClickHouse configuration not found, skipping ClickHouse initialization")
		return nil, func() {}, nil
	}

	addr := conf.GetStringSlice("data.clickhouse.addr")
	database := conf.GetString("data.clickhouse.database")
	username := conf.GetString("data.clickhouse.username")
	password := conf.GetString("data.clickhouse.password")

	if len(addr) == 0 {
		zapLogger.Warn("ClickHouse address is empty, skipping ClickHouse initialization")
		return nil, func() {}, nil
	}

	zapLogger.Info("Initializing ClickHouse client...", zap.Strings("addr", addr), zap.String("database", database))

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: addr,
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    20,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("clickhouse open: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("clickhouse ping: %w", err)
	}

	zapLogger.Info("ClickHouse client connected successfully")

	// 异步执行建表 DDL 初始化，避免 ClickHouse 建表慢阻塞网关启动
	go func() {
		initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer initCancel()
		if err := AutoMigrateClickHouse(initCtx, conn); err != nil {
			zapLogger.Error("Auto migrate ClickHouse tables failed", zap.Error(err))
		} else {
			zapLogger.Info("Auto migrate ClickHouse tables completed successfully")
		}
	}()

	cleanup := func() {
		zapLogger.Info("Closing ClickHouse connection...")
		_ = conn.Close()
	}

	return conn, cleanup, nil
}

// AutoMigrateClickHouse 执行 DDL 初始化建表脚本
func AutoMigrateClickHouse(ctx context.Context, conn clickhouse.Conn) error {
	ddls := []string{
		// 1. 访问明细表 access_logs
		`CREATE TABLE IF NOT EXISTS access_logs (
			request_id String,
			time DateTime64(3) CODEC(DoubleDelta, LZ4),
			tenant_id LowCardinality(String),
			user_id LowCardinality(String),
			session_id String,
			api_key String,
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
		TTL time + INTERVAL 90 DAY;`,

		// 2. 租户账单小时表 tenant_billing_hourly
		`CREATE TABLE IF NOT EXISTS tenant_billing_hourly (
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
		TTL time_hourly + INTERVAL 730 DAY;`,

		// 3. 租户账单物化视图
		`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_billing_hourly 
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
		GROUP BY tenant_id, model, time_hourly;`,

		// 4. 服务性能分钟监控表 endpoint_metrics_minute
		`CREATE TABLE IF NOT EXISTS endpoint_metrics_minute (
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
		TTL time_minute + INTERVAL 7 DAY;`,

		// 5. 服务性能分钟级物化视图
		`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_endpoint_metrics_minute 
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
		GROUP BY model, provider, endpoint_id, time_minute;`,

		// 6. 服务性能小时监控表 endpoint_metrics_hourly
		`CREATE TABLE IF NOT EXISTS endpoint_metrics_hourly (
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
		TTL time_hourly + INTERVAL 365 DAY;`,

		// 7. 服务性能小时级物化视图
		`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_endpoint_metrics_hourly 
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
		GROUP BY model, provider, endpoint_id, time_hourly;`,
	}

	for i, ddl := range ddls {
		if err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("execute ddl index %d failed: %w", i, err)
		}
	}
	return nil
}
