package compensation

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// testEnv 测试环境，封装所有测试依赖。
type testEnv struct {
	client *redis.Client
	queue  *RedisQueue
	worker *Worker
	s      *miniredis.Miniredis
}

func setupTestEnv(t *testing.T, maxRetries int) *testEnv {
	t.Helper()
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	q, err := NewRedisQueue(client, &RedisQueueConfig{
		StreamKey:    "test:stream",
		DelayedKey:   "test:delayed",
		DLQKey:       "test:dlq",
		ConsumerName: "test-consumer",
		GroupName:    "test-group",
		MaxRetries:   maxRetries,
	})
	require.NoError(t, err)

	logger := zap.NewNop()
	w := NewWorker(client, q, logger)
	// 使用固定时间以便测试可预测的延迟
	w.nowFunc = func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }

	return &testEnv{client: client, queue: q, worker: w, s: s}
}

// enqueueTask 直接向主 Stream 添加一条消息（模拟 Enqueue）。
func (env *testEnv) enqueueTask(t *testing.T, task *CompensationTask) string {
	t.Helper()
	ctx := context.Background()
	data, err := json.Marshal(task)
	require.NoError(t, err)

	id, err := env.client.XAdd(ctx, &redis.XAddArgs{
		Stream: env.queue.StreamKey(),
		Values: map[string]any{"data": string(data)},
	}).Result()
	require.NoError(t, err)
	return id
}

func TestWorker_ProcessSuccess(t *testing.T) {
	env := setupTestEnv(t, 3)
	ctx := context.Background()

	// 注册成功的补偿器
	env.worker.RegisterCompensator("token_settlement", CompensatorFunc(
		func(ctx context.Context, payload map[string]any) error {
			return nil
		},
	))

	// 入队一个任务
	task := &CompensationTask{
		ID:         "task-success-1",
		FilterName: "token_settlement",
		Payload:    map[string]any{"user_id": "u1", "tokens": 100},
		EnqueueAt:  time.Now(),
	}
	env.enqueueTask(t, task)

	// 验证 Stream 有 1 条消息
	length, err := env.client.XLen(ctx, env.queue.StreamKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)

	// 执行一次 ProcessBatch
	err = env.worker.ProcessBatch(ctx)
	require.NoError(t, err)

	// ACK 后，pending 列表应为空
	pending, err := env.client.XPending(ctx, env.queue.StreamKey(), env.queue.GroupName()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)

	// 延迟 ZSet 应为空
	zcount, err := env.client.ZCard(ctx, env.queue.DelayedKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcount)
}

func TestWorker_ProcessFailure_Retries(t *testing.T) {
	env := setupTestEnv(t, 3)
	ctx := context.Background()

	testErr := errors.New("settlement failed")
	env.worker.RegisterCompensator("token_settlement", CompensatorFunc(
		func(ctx context.Context, payload map[string]any) error {
			return testErr
		},
	))

	task := &CompensationTask{
		ID:         "task-fail-1",
		FilterName: "token_settlement",
		Payload:    map[string]any{"user_id": "u1"},
		EnqueueAt:  time.Now(),
	}
	env.enqueueTask(t, task)

	// 执行一次 ProcessBatch，补偿器失败
	err := env.worker.ProcessBatch(ctx)
	require.NoError(t, err)

	// 主 Stream 已 ACK，pending 应为空
	pending, err := env.client.XPending(ctx, env.queue.StreamKey(), env.queue.GroupName()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)

	// 任务应移入延迟 ZSet（attempt=1, delay=1^2=1秒）
	zcount, err := env.client.ZCard(ctx, env.queue.DelayedKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), zcount)

	// 验证延迟 ZSet 中的任务数据
	members, err := env.client.ZRangeByScore(ctx, env.queue.DelayedKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	require.NoError(t, err)
	require.Len(t, members, 1)

	var delayedTask CompensationTask
	err = json.Unmarshal([]byte(members[0]), &delayedTask)
	require.NoError(t, err)
	assert.Equal(t, "task-fail-1", delayedTask.ID)
	assert.Equal(t, 1, delayedTask.AttemptCount)
	assert.Equal(t, "settlement failed", delayedTask.LastError)
}

func TestWorker_ProcessFailure_DLQ(t *testing.T) {
	maxRetries := 3
	env := setupTestEnv(t, maxRetries)
	ctx := context.Background()

	env.worker.RegisterCompensator("token_settlement", CompensatorFunc(
		func(ctx context.Context, payload map[string]any) error {
			return errors.New("always fails")
		},
	))

	task := &CompensationTask{
		ID:         "task-dlq-1",
		FilterName: "token_settlement",
		Payload:    map[string]any{"user_id": "u1"},
		EnqueueAt:  time.Now(),
	}

	// 模拟多次重试失败
	for i := 0; i < maxRetries; i++ {
		// 如果任务在延迟 ZSet 中，先手动回收到主 Stream
		if i > 0 {
			// 清空延迟 ZSet 并手动放回 Stream
			members, err := env.client.ZRangeByScore(ctx, env.queue.DelayedKey(), &redis.ZRangeBy{
				Min: "-inf",
				Max: "+inf",
			}).Result()
			require.NoError(t, err)
			for _, m := range members {
				_, err := env.client.XAdd(ctx, &redis.XAddArgs{
					Stream: env.queue.StreamKey(),
					Values: map[string]any{"data": m},
				}).Result()
				require.NoError(t, err)
				err = env.client.ZRem(ctx, env.queue.DelayedKey(), m).Err()
				require.NoError(t, err)
			}
		} else {
			env.enqueueTask(t, task)
		}

		// 处理一批
		err := env.worker.ProcessBatch(ctx)
		require.NoError(t, err)
	}

	// 达到最大重试次数后，任务应在 DLQ 中
	dlqLen, err := env.client.XLen(ctx, env.queue.DLQKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), dlqLen, "DLQ 应包含 1 条消息")

	// 验证 DLQ 中的任务数据
	dlqMessages, err := env.client.XRange(ctx, env.queue.DLQKey(), "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, dlqMessages, 1)

	var dlqTask CompensationTask
	err = json.Unmarshal([]byte(dlqMessages[0].Values["data"].(string)), &dlqTask)
	require.NoError(t, err)
	assert.Equal(t, "task-dlq-1", dlqTask.ID)
	assert.Equal(t, maxRetries, dlqTask.AttemptCount)
	assert.Equal(t, "always fails", dlqTask.LastError)

	// 延迟 ZSet 应为空
	zcount, err := env.client.ZCard(ctx, env.queue.DelayedKey()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), zcount)
}

func TestWorker_HandleMessage_InvalidJSON(t *testing.T) {
	env := setupTestEnv(t, 3)
	ctx := context.Background()

	// 直接插入无效 JSON 到 Stream
	id, err := env.client.XAdd(ctx, &redis.XAddArgs{
		Stream: env.queue.StreamKey(),
		Values: map[string]any{"data": "not-valid-json"},
	}).Result()
	require.NoError(t, err)

	msg := redis.XMessage{ID: id, Values: map[string]any{"data": "not-valid-json"}}
	env.worker.HandleMessage(ctx, msg)

	// 无效消息应被 ACK（pending 为空）
	pending, err := env.client.XPending(ctx, env.queue.StreamKey(), env.queue.GroupName()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

func TestWorker_HandleMessage_MissingDataField(t *testing.T) {
	env := setupTestEnv(t, 3)
	ctx := context.Background()

	id, err := env.client.XAdd(ctx, &redis.XAddArgs{
		Stream: env.queue.StreamKey(),
		Values: map[string]any{"other": "field"},
	}).Result()
	require.NoError(t, err)

	msg := redis.XMessage{ID: id, Values: map[string]any{"other": "field"}}
	env.worker.HandleMessage(ctx, msg)

	pending, err := env.client.XPending(ctx, env.queue.StreamKey(), env.queue.GroupName()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

func TestWorker_HandleMessage_UnknownCompensator(t *testing.T) {
	env := setupTestEnv(t, 3)
	ctx := context.Background()

	// 不注册任何 compensator
	task := &CompensationTask{
		ID:         "task-unknown",
		FilterName: "nonexistent_filter",
		Payload:    map[string]any{},
		EnqueueAt:  time.Now(),
	}
	taskData, _ := json.Marshal(task)

	id, err := env.client.XAdd(ctx, &redis.XAddArgs{
		Stream: env.queue.StreamKey(),
		Values: map[string]any{"data": string(taskData)},
	}).Result()
	require.NoError(t, err)

	msg := redis.XMessage{
		ID:     id,
		Values: map[string]any{"data": string(taskData)},
	}
	env.worker.HandleMessage(ctx, msg)

	// 未知补偿器应 ACK
	pending, err := env.client.XPending(ctx, env.queue.StreamKey(), env.queue.GroupName()).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

func TestWorker_RegisterCompensator(t *testing.T) {
	env := setupTestEnv(t, 3)

	called := false
	env.worker.RegisterCompensator("test_filter", CompensatorFunc(
		func(ctx context.Context, payload map[string]any) error {
			called = true
			return nil
		},
	))

	ctx := context.Background()
	task := &CompensationTask{
		ID:         "reg-test",
		FilterName: "test_filter",
		Payload:    map[string]any{},
	}
	env.enqueueTask(t, task)

	err := env.worker.ProcessBatch(ctx)
	require.NoError(t, err)
	assert.True(t, called)
}
