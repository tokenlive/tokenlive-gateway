package compensation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue is the compensation queue interface.
type Queue interface {
	// Enqueue adds a compensation task.
	Enqueue(ctx context.Context, task *CompensationTask) error
	// Close releases queue resources.
	Close() error
}

// RedisQueueConfig configures RedisQueue.
type RedisQueueConfig struct {
	// StreamKey is the main stream key (default "aigw:compensation:stream").
	StreamKey string
	// DelayedKey is the delayed-retry ZSet key (default "aigw:compensation:delayed").
	DelayedKey string
	// DLQKey is the dead-letter stream key (default "aigw:compensation:dlq").
	DLQKey string
	// ConsumerName is the consumer name (default "compensation-worker-1").
	ConsumerName string
	// GroupName is the consumer group name (default "compensation-group").
	GroupName string
	// MaxRetries is the max retry count (default 5).
	MaxRetries int
}

const (
	defaultStreamKey    = "aigw:compensation:stream"
	defaultDelayedKey   = "aigw:compensation:delayed"
	defaultDLQKey       = "aigw:compensation:dlq"
	defaultConsumerName = "compensation-worker-1"
	defaultGroupName    = "compensation-group"
	defaultMaxRetries   = 5
)

// RedisQueue is a Redis Stream-backed compensation queue.
// Uses redis.Cmdable (works with redis.Client and redis.ClusterClient).
type RedisQueue struct {
	client   redis.Cmdable
	stream   string
	delayed  string
	dlq      string
	consumer string
	group    string
	maxRetry int
}

// NewRedisQueue creates a RedisQueue and idempotently creates the consumer group
// (BUSYGROUP is ignored if the group already exists).
func NewRedisQueue(client redis.Cmdable, cfg *RedisQueueConfig) (*RedisQueue, error) {
	if cfg == nil {
		cfg = &RedisQueueConfig{}
	}

	q := &RedisQueue{
		client:   client,
		stream:   cfg.StreamKey,
		delayed:  cfg.DelayedKey,
		dlq:      cfg.DLQKey,
		consumer: cfg.ConsumerName,
		group:    cfg.GroupName,
		maxRetry: cfg.MaxRetries,
	}

	if q.stream == "" {
		q.stream = defaultStreamKey
	}
	if q.delayed == "" {
		q.delayed = defaultDelayedKey
	}
	if q.dlq == "" {
		q.dlq = defaultDLQKey
	}
	if q.consumer == "" {
		q.consumer = defaultConsumerName
	}
	if q.group == "" {
		q.group = defaultGroupName
	}
	if q.maxRetry == 0 {
		q.maxRetry = defaultMaxRetries
	}

	// create consumer group idempotently
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err != nil && !isBusyGroupErr(err) {
		return nil, fmt.Errorf("compensation: create consumer group: %w", err)
	}

	return q, nil
}

// Enqueue marshals the task to JSON and appends it to the Redis Stream.
func (q *RedisQueue) Enqueue(ctx context.Context, task *CompensationTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("compensation: marshal task: %w", err)
	}

	_, err = q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{
			"data": string(data),
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("compensation: xadd: %w", err)
	}
	return nil
}

// ClaimDelayed moves due tasks from the delayed ZSet back to the main stream.
// Returns how many tasks were migrated.
func (q *RedisQueue) ClaimDelayed(ctx context.Context) (int64, error) {
	now := float64(time.Now().UnixMilli())
	members, err := q.client.ZRangeByScore(ctx, q.delayed, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("compensation: zrangebyscore delayed: %w", err)
	}

	var claimed int64
	for _, member := range members {
		_, err := q.client.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			Values: map[string]any{
				"data": member,
			},
		}).Result()
		if err != nil {
			return claimed, fmt.Errorf("compensation: xadd claimed task: %w", err)
		}

		err = q.client.ZRem(ctx, q.delayed, member).Err()
		if err != nil {
			return claimed, fmt.Errorf("compensation: zrem delayed: %w", err)
		}
		claimed++
	}
	return claimed, nil
}

// Close is a no-op (satisfies Queue).
func (q *RedisQueue) Close() error {
	return nil
}

// StreamKey returns the main stream key.
func (q *RedisQueue) StreamKey() string { return q.stream }

// DelayedKey returns the delayed ZSet key.
func (q *RedisQueue) DelayedKey() string { return q.delayed }

// DLQKey returns the dead-letter stream key.
func (q *RedisQueue) DLQKey() string { return q.dlq }

// ConsumerName returns the consumer name.
func (q *RedisQueue) ConsumerName() string { return q.consumer }

// GroupName returns the consumer group name.
func (q *RedisQueue) GroupName() string { return q.group }

// MaxRetries returns the max retry count.
func (q *RedisQueue) MaxRetries() int { return q.maxRetry }

// isBusyGroupErr reports whether err is Redis BUSYGROUP (group already exists).
func isBusyGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
