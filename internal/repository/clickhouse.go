package repository

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/viper"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"go.uber.org/zap"
)

const clickHouseSchemaPath = "scripts/clickhouse_schema.sql"

// NewClickHouse 初始化 ClickHouse 连接客户端，支持在没有配置时平滑降级（返回 nil）。
func NewClickHouse(conf *viper.Viper, l *log.Logger) (clickhouse.Conn, func(), error) {
	zapLogger := l.Logger

	// 优先检查 clickhouse 写入开关是否开启。若为 false，则不尝试进行任何网络连接 and 初始化
	if !conf.GetBool("access_log.clickhouse.enabled") {
		zapLogger.Info("ClickHouse log writing is disabled (access_log.clickhouse.enabled is false), skipping connection initialization")
		return nil, func() {}, nil
	}

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
	content, err := os.ReadFile(clickHouseSchemaPath)
	if err != nil {
		return fmt.Errorf("read clickhouse schema %s: %w", clickHouseSchemaPath, err)
	}

	ddls := splitClickHouseDDLs(string(content))
	for i, ddl := range ddls {
		if err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("execute ddl index %d failed: %w", i, err)
		}
	}
	return nil
}

func splitClickHouseDDLs(schema string) []string {
	lines := strings.Split(schema, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		cleaned = append(cleaned, line)
	}

	statements := strings.Split(strings.Join(cleaned, "\n"), ";")
	ddls := make([]string, 0, len(statements))
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		ddls = append(ddls, statement)
	}
	return ddls
}
