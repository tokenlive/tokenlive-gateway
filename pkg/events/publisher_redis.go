package events

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisPublisher implements Publisher using Redis Stream (XADD).
type RedisPublisher struct {
	client    *redis.Client
	streamKey string
}

// NewRedisPublisher creates a new Redis Stream publisher.
func NewRedisPublisher(client *redis.Client, streamKey string) *RedisPublisher {
	if streamKey == "" {
		streamKey = "aigw:events:policy"
	}
	return &RedisPublisher{
		client:    client,
		streamKey: streamKey,
	}
}

// Publish sends an event to the Redis Stream.
func (p *RedisPublisher) Publish(ctx context.Context, event *OpsEvent) error {
	if p.client == nil {
		return nil
	}

	if event.Timestamp == 0 {
		event.Timestamp = time.Now().Unix()
	}

	values := map[string]interface{}{
		"event_type":    event.EventType,
		"tenant_code":   event.TenantCode,
		"model_code":    event.ModelCode,
		"endpoint_id":   event.EndpointID,
		"endpoint_code": event.EndpointCode,
		"provider_name": event.ProviderName,
		"policy_id":     event.PolicyID,
		"policy_name":   event.PolicyName,
		"request_id":    event.RequestID,
		"trace_id":      event.TraceID,
		"message":       event.Message,
		"ts":            strconv.FormatInt(event.Timestamp, 10),
	}

	if event.Threshold != nil {
		values["threshold"] = fmt.Sprintf("%.2f", *event.Threshold)
	}
	if event.CurrentValue != nil {
		values["current_value"] = fmt.Sprintf("%.2f", *event.CurrentValue)
	}

	args := &redis.XAddArgs{
		Stream: p.streamKey,
		MaxLen: 100000,
		Approx: true,
		Values: values,
	}

	return p.client.XAdd(ctx, args).Err()
}

// Close is a no-op since the Redis client is shared.
func (p *RedisPublisher) Close() error {
	return nil
}
