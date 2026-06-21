package outbound

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// AccessLogCompensator 负责从 Redis 补偿队列中消费写入失败的 Access Logs 并重新写入 ClickHouse。
type AccessLogCompensator struct {
	chConn clickhouse.Conn
	logger *zap.Logger
}

// NewAccessLogCompensator 创建 AccessLogCompensator 实例。
func NewAccessLogCompensator(chConn clickhouse.Conn, logger *zap.Logger) *AccessLogCompensator {
	return &AccessLogCompensator{
		chConn: chConn,
		logger: logger,
	}
}

// Compensate 实现 compensation.Compensator 接口。
func (c *AccessLogCompensator) Compensate(ctx context.Context, payload map[string]any) error {
	if c.chConn == nil {
		c.logger.Warn("ClickHouse connection is nil, skipping compensation")
		return nil
	}

	logsData, ok := payload["logs"]
	if !ok {
		c.logger.Error("Access log compensation task payload missing 'logs' field, skip")
		return nil
	}

	// 转换为 JSON 进行反序列化
	jsonBytes, err := json.Marshal(logsData)
	if err != nil {
		return fmt.Errorf("marshal logs in compensation payload: %w", err)
	}

	var items []AccessLogItem
	if err := json.Unmarshal(jsonBytes, &items); err != nil {
		return fmt.Errorf("unmarshal logs in compensation payload: %w", err)
	}

	if len(items) == 0 {
		return nil
	}

	c.logger.Info("Compensating access logs batch to ClickHouse...", zap.Int("count", len(items)))

	// 执行批量插入
	if err := writeBatchToClickHouse(ctx, c.chConn, items); err != nil {
		c.logger.Error("Failed to write compensation batch to ClickHouse", zap.Error(err))
		return err
	}

	c.logger.Info("Compensate access logs batch to ClickHouse completed successfully", zap.Int("count", len(items)))
	return nil
}

// writeBatchToClickHouse 抽取为共享的方法，供 AccessLogFilter 和 AccessLogCompensator 共同调用。
func writeBatchToClickHouse(ctx context.Context, conn clickhouse.Conn, items []AccessLogItem) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO access_logs")
	if err != nil {
		return fmt.Errorf("prepare clickhouse batch: %w", err)
	}

	for _, item := range items {
		// 转换 cost 到 shopspring/decimal 以防止 float 精度丢失 (ClickHouse-go v2 需要适配)
		// 也可以直接传 float64，驱动能根据列类型转换，但通过 decimal 包会更精确
		err = batch.Append(
			item.RequestID,
			item.Time,
			item.TenantID,
			item.UserID,
			item.SessionID,
			item.APIKey,
			item.ClientIP,
			item.OriginalModel,
			item.Model,
			item.Provider,
			item.EndpointID,
			item.IsStream,
			item.Attempts,
			item.FallbackChain,
			item.StatusCode,
			item.LatencyMs,
			item.TtftMs,
			item.ErrorMessage,
			item.InputTokens,
			item.OutputTokens,
			item.CachedTokens,
			item.CacheCreationTokens,
			decimal.NewFromFloat(item.Cost),
		)
		if err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send clickhouse batch: %w", err)
	}

	return nil
}
