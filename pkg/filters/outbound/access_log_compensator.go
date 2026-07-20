package outbound

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// AccessLogCompensator consumes failed access logs from the Redis compensation queue and retries ClickHouse writes.
type AccessLogCompensator struct {
	chConn clickhouse.Conn
	logger *zap.Logger
}

// NewAccessLogCompensator creates an AccessLogCompensator.
func NewAccessLogCompensator(chConn clickhouse.Conn, logger *zap.Logger) *AccessLogCompensator {
	return &AccessLogCompensator{
		chConn: chConn,
		logger: logger,
	}
}

// Compensate implements the compensation.Compensator interface.
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

	// convert to JSON for deserialization
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

	// batch insert
	if err := writeBatchToClickHouse(ctx, c.chConn, items); err != nil {
		c.logger.Error("Failed to write compensation batch to ClickHouse", zap.Error(err))
		return err
	}

	c.logger.Info("Compensate access logs batch to ClickHouse completed successfully", zap.Int("count", len(items)))
	return nil
}

// writeBatchToClickHouse is shared by AccessLogFilter and AccessLogCompensator.
func writeBatchToClickHouse(ctx context.Context, conn clickhouse.Conn, items []AccessLogItem) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO access_logs")
	if err != nil {
		return fmt.Errorf("prepare clickhouse batch: %w", err)
	}

	for _, item := range items {
		// use decimal for cost to prevent float precision loss (ClickHouse-go v2 adaptation)
		err = batch.Append(
			item.RequestID,
			item.Time,
			item.TenantID,
			item.UserID,
			item.SessionID,
			item.APIKey,
			item.WorkspaceID,
			item.APIKeyID,
			item.APIKeyHash,
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
