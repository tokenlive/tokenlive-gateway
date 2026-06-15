package compensation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue 补偿队列接口。
type Queue interface {
	// Enqueue 将补偿任务加入队列。
	Enqueue(ctx context.Context, task *CompensationTask) error
	// Close 关闭队列（释放资源等）。
	Close() error
}

// RedisQueueConfig Redis 补偿队列配置。
type RedisQueueConfig struct {
	// StreamKey 主 Stream 键名（默认 "aigw:compensation:stream"）
	StreamKey string
	// DelayedKey 延迟重试 ZSet 键名（默认 "aigw:compensation:delayed"）
	DelayedKey string
	// DLQKey 死信队列 Stream 键名（默认 "aigw:compensation:dlq"）
	DLQKey string
	// ConsumerName 消费者名称（默认 "compensation-worker-1"）
	ConsumerName string
	// GroupName 消费者组名称（默认 "compensation-group"）
	GroupName string
	// MaxRetries 最大重试次数（默认 5）
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

// RedisQueue 基于 Redis Stream 的补偿队列实现。
// 使用 redis.Cmdable 接口，兼容 redis.Client 和 redis.ClusterClient。
type RedisQueue struct {
	client   redis.Cmdable
	stream   string
	delayed  string
	dlq      string
	consumer string
	group    string
	maxRetry int
}

// NewRedisQueue 创建 RedisQueue 实例。
// 会幂等创建消费者组（通过 BUSYGROUP 检查忽略已存在错误）。
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

	// 幂等创建消费者组
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err != nil && !isBusyGroupErr(err) {
		return nil, fmt.Errorf("compensation: create consumer group: %w", err)
	}

	return q, nil
}

// Enqueue 将补偿任务序列化为 JSON 并写入 Redis Stream。
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

// ClaimDelayed 从延迟 ZSet 中取出已过期的任务，重新放回主 Stream。
// 返回成功迁移的任务数。
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
		// 将任务数据重新添加到主 Stream
		_, err := q.client.XAdd(ctx, &redis.XAddArgs{
			Stream: q.stream,
			Values: map[string]any{
				"data": member,
			},
		}).Result()
		if err != nil {
			return claimed, fmt.Errorf("compensation: xadd claimed task: %w", err)
		}

		// 从延迟 ZSet 中移除
		err = q.client.ZRem(ctx, q.delayed, member).Err()
		if err != nil {
			return claimed, fmt.Errorf("compensation: zrem delayed: %w", err)
		}
		claimed++
	}
	return claimed, nil
}

// Close 关闭队列（当前为 no-op，满足接口要求）。
func (q *RedisQueue) Close() error {
	return nil
}

// Getters for testing and worker access

// StreamKey 返回主 Stream 键名。
func (q *RedisQueue) StreamKey() string { return q.stream }

// DelayedKey 返回延迟 ZSet 键名。
func (q *RedisQueue) DelayedKey() string { return q.delayed }

// DLQKey 返回死信队列 Stream 键名。
func (q *RedisQueue) DLQKey() string { return q.dlq }

// ConsumerName 返回消费者名称。
func (q *RedisQueue) ConsumerName() string { return q.consumer }

// GroupName 返回消费者组名称。
func (q *RedisQueue) GroupName() string { return q.group }

// MaxRetries 返回最大重试次数。
func (q *RedisQueue) MaxRetries() int { return q.maxRetry }

// isBusyGroupErr 检查 Redis 错误是否为消费者组已存在的错误（BUSYGROUP）。
func isBusyGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
