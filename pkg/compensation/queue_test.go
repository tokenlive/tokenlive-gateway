package compensation

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestQueue(t *testing.T) (*RedisQueue, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	q, err := NewRedisQueue(client, &RedisQueueConfig{
		StreamKey:    "test:stream",
		DelayedKey:   "test:delayed",
		DLQKey:       "test:dlq",
		ConsumerName: "test-consumer",
		GroupName:    "test-group",
		MaxRetries:   3,
	})
	require.NoError(t, err)
	return q, client
}

func TestRedisQueue_Enqueue(t *testing.T) {
	q, client := setupTestQueue(t)
	ctx := context.Background()

	task := &CompensationTask{
		ID:         "task-1",
		FilterName: "token_settlement",
		Payload:    map[string]any{"key": "value"},
		EnqueueAt:  time.Now(),
	}

	// 入队第一条
	err := q.Enqueue(ctx, task)
	require.NoError(t, err)

	length, err := client.XLen(ctx, q.StreamKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)

	// 入队第二条
	task2 := &CompensationTask{
		ID:         "task-2",
		FilterName: "sticky_session",
		Payload:    map[string]any{"session": "abc"},
		EnqueueAt:  time.Now(),
	}
	err = q.Enqueue(ctx, task2)
	require.NoError(t, err)

	length, err = client.XLen(ctx, q.StreamKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), length)
}

func TestRedisQueue_ClaimDelayed(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	q, err := NewRedisQueue(client, &RedisQueueConfig{
		StreamKey:    "test:stream",
		DelayedKey:   "test:delayed",
		DLQKey:       "test:dlq",
		ConsumerName: "test-consumer",
		GroupName:    "test-group",
		MaxRetries:   3,
	})
	require.NoError(t, err)
	ctx := context.Background()

	// 手动添加一个过去时间的任务到延迟 ZSet
	taskData := `{"id":"delayed-1","filter_name":"token_settlement","payload":{"key":"val"},"attempt_count":1}`
	pastScore := float64(time.Now().Add(-10 * time.Second).UnixMilli())
	err = client.ZAdd(ctx, "test:delayed", redis.Z{
		Score:  pastScore,
		Member: taskData,
	}).Err()
	require.NoError(t, err)

	// 调用 ClaimDelayed
	claimed, err := q.ClaimDelayed(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), claimed)

	// 验证主 Stream 中有 1 条消息
	length, err := client.XLen(ctx, "test:stream").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)

	// 验证延迟 ZSet 已清空
	zcount, err := client.ZCard(ctx, "test:delayed").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcount)
}

func TestRedisQueue_ClaimDelayed_NoneExpired(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	q, err := NewRedisQueue(client, &RedisQueueConfig{
		StreamKey:    "test:stream",
		DelayedKey:   "test:delayed",
		DLQKey:       "test:dlq",
		ConsumerName: "test-consumer",
		GroupName:    "test-group",
		MaxRetries:   3,
	})
	require.NoError(t, err)
	ctx := context.Background()

	// 添加一个未来时间的任务
	taskData := `{"id":"future-1","filter_name":"token_settlement","payload":{},"attempt_count":1}`
	futureScore := float64(time.Now().Add(10 * time.Minute).UnixMilli())
	err = client.ZAdd(ctx, "test:delayed", redis.Z{
		Score:  futureScore,
		Member: taskData,
	}).Err()
	require.NoError(t, err)

	// ClaimDelayed 不应迁移未来任务
	claimed, err := q.ClaimDelayed(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), claimed)

	// 主 Stream 应为空
	length, err := client.XLen(ctx, "test:stream").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)

	// 延迟 ZSet 应仍有 1 个
	zcount, err := client.ZCard(ctx, "test:delayed").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcount)
}

func TestRedisQueue_Close(t *testing.T) {
	q, _ := setupTestQueue(t)
	err := q.Close()
	assert.NoError(t, err)
}

func TestNewRedisQueue_DefaultConfig(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	q, err := NewRedisQueue(client, nil)
	require.NoError(t, err)

	assert.Equal(t, defaultStreamKey, q.StreamKey())
	assert.Equal(t, defaultDelayedKey, q.DelayedKey())
	assert.Equal(t, defaultDLQKey, q.DLQKey())
	assert.Equal(t, defaultConsumerName, q.ConsumerName())
	assert.Equal(t, defaultGroupName, q.GroupName())
	assert.Equal(t, defaultMaxRetries, q.MaxRetries())
}

func TestNewRedisQueue_IdempotentGroup(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	cfg := &RedisQueueConfig{
		StreamKey:    "test:stream",
		GroupName:    "test-group",
		ConsumerName: "test-consumer",
	}

	// 第一次创建
	_, err := NewRedisQueue(client, cfg)
	require.NoError(t, err)

	// 第二次创建应幂等成功（不报 BUSYGROUP 错误）
	_, err = NewRedisQueue(client, cfg)
	require.NoError(t, err)
}
