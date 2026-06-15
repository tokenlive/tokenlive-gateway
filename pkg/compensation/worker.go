package compensation

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Compensator 补偿操作接口。
// 每个 FilterName 对应一个 Compensator 实现。
type Compensator interface {
	// Compensate 执行补偿操作。
	Compensate(ctx context.Context, payload map[string]any) error
}

// CompensatorFunc 函数适配器，允许将普通函数作为 Compensator 使用。
type CompensatorFunc func(ctx context.Context, payload map[string]any) error

// Compensate 实现 Compensator 接口。
func (f CompensatorFunc) Compensate(ctx context.Context, payload map[string]any) error {
	return f(ctx, payload)
}

// Worker 补偿队列后台工作者，负责从 Redis Stream 读取任务并执行补偿操作。
type Worker struct {
	client redis.Cmdable
	queue  *RedisQueue
	logger *zap.Logger

	compensators map[string]Compensator
	mu           sync.RWMutex

	stopCh chan struct{}
	done   chan struct{}

	// nowFunc 返回当前时间，测试时可替换。
	nowFunc func() time.Time
}

// NewWorker 创建 Worker 实例。
func NewWorker(client redis.Cmdable, queue *RedisQueue, logger *zap.Logger) *Worker {
	return &Worker{
		client:       client,
		queue:        queue,
		logger:       logger,
		compensators: make(map[string]Compensator),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
		nowFunc:      time.Now,
	}
}

// RegisterCompensator 注册一个补偿器，filterName 为对应的过滤器名称。
func (w *Worker) RegisterCompensator(filterName string, c Compensator) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.compensators[filterName] = c
}

// Run 启动工作者的阻塞主循环，直到 ctx 被取消或调用 Close。
func (w *Worker) Run(ctx context.Context) {
	defer close(w.done)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.runCycle(ctx)
		}
	}
}

// Close 信号停止工作者。
func (w *Worker) Close() {
	close(w.stopCh)
	<-w.done
}

// runCycle 执行一个完整的工作周期：先回收延迟任务，再处理一批消息。
func (w *Worker) runCycle(ctx context.Context) {
	// 1. 回收已过期的延迟任务
	claimed, err := w.queue.ClaimDelayed(ctx)
	if err != nil {
		w.logger.Error("回收延迟任务失败", zap.Error(err))
	}
	if claimed > 0 {
		w.logger.Info("回收延迟任务", zap.Int64("count", claimed))
	}

	// 2. 处理一批消息
	if err := w.ProcessBatch(ctx); err != nil {
		w.logger.Error("处理批次失败", zap.Error(err))
	}
}

// ProcessBatch 从主 Stream 读取一批消息（最多 10 条）并处理。导出以便测试。
func (w *Worker) ProcessBatch(ctx context.Context) error {
	streams, err := w.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    w.queue.GroupName(),
		Consumer: w.queue.ConsumerName(),
		Streams:  []string{w.queue.StreamKey(), ">"},
		Count:    10,
		Block:    100 * time.Millisecond,
	}).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("compensation: xreadgroup: %w", err)
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			w.HandleMessage(ctx, msg)
		}
	}
	return nil
}

// HandleMessage 处理单条 Stream 消息。导出以便测试。
func (w *Worker) HandleMessage(ctx context.Context, msg redis.XMessage) {
	data, ok := msg.Values["data"].(string)
	if !ok {
		w.logger.Error("消息缺少 data 字段，直接 ACK",
			zap.String("id", msg.ID))
		w.ack(ctx, msg.ID)
		return
	}

	var task CompensationTask
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		w.logger.Error("消息反序列化失败，直接 ACK",
			zap.String("id", msg.ID), zap.Error(err))
		w.ack(ctx, msg.ID)
		return
	}

	w.mu.RLock()
	compensator, exists := w.compensators[task.FilterName]
	w.mu.RUnlock()

	if !exists {
		w.logger.Error("未找到补偿器，直接 ACK",
			zap.String("filter_name", task.FilterName),
			zap.String("task_id", task.ID))
		w.ack(ctx, msg.ID)
		return
	}

	err := compensator.Compensate(ctx, task.Payload)
	if err != nil {
		w.logger.Warn("补偿操作失败",
			zap.String("task_id", task.ID),
			zap.String("filter_name", task.FilterName),
			zap.Int("attempt", task.AttemptCount+1),
			zap.Error(err))
		w.handleFailure(ctx, msg.ID, &task, err)
		return
	}

	w.logger.Info("补偿操作成功",
		zap.String("task_id", task.ID),
		zap.String("filter_name", task.FilterName))
	w.ack(ctx, msg.ID)
}

// handleFailure 处理补偿失败：超过最大重试次数则移入 DLQ，否则调度延迟重试。
func (w *Worker) handleFailure(ctx context.Context, msgID string, task *CompensationTask, err error) {
	task.AttemptCount++
	task.LastError = err.Error()

	if task.AttemptCount >= w.queue.MaxRetries() {
		// 移入死信队列
		w.moveToDLQ(ctx, task)
		w.ack(ctx, msgID)
		w.logger.Error("任务已达最大重试次数，移入 DLQ",
			zap.String("task_id", task.ID),
			zap.Int("attempt_count", task.AttemptCount))
		return
	}

	// 指数退避延迟重试：attempt^2 秒
	delaySeconds := math.Pow(float64(task.AttemptCount), 2)
	nextRetry := w.nowFunc().Add(time.Duration(delaySeconds) * time.Second)
	task.NextRetryAt = nextRetry

	w.scheduleDelayed(ctx, task)
	w.ack(ctx, msgID)
}

// scheduleDelayed 将任务放入延迟 ZSet，延迟到 NextRetryAt 时被 ClaimDelayed 回收。
func (w *Worker) scheduleDelayed(ctx context.Context, task *CompensationTask) {
	data, err := json.Marshal(task)
	if err != nil {
		w.logger.Error("序列化延迟任务失败", zap.Error(err))
		return
	}

	score := float64(task.NextRetryAt.UnixMilli())
	err = w.client.ZAdd(ctx, w.queue.DelayedKey(), redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
	if err != nil {
		w.logger.Error("添加延迟任务失败", zap.Error(err))
	}
}

// moveToDLQ 将任务移入死信队列。
func (w *Worker) moveToDLQ(ctx context.Context, task *CompensationTask) {
	data, err := json.Marshal(task)
	if err != nil {
		w.logger.Error("序列化 DLQ 任务失败", zap.Error(err))
		return
	}

	_, err = w.client.XAdd(ctx, &redis.XAddArgs{
		Stream: w.queue.DLQKey(),
		Values: map[string]any{
			"data": string(data),
		},
	}).Result()
	if err != nil {
		w.logger.Error("添加 DLQ 任务失败", zap.Error(err))
	}
}

// ack 确认消息。
func (w *Worker) ack(ctx context.Context, msgID string) {
	err := w.client.XAck(ctx, w.queue.StreamKey(), w.queue.GroupName(), msgID).Err()
	if err != nil {
		w.logger.Error("ACK 消息失败", zap.String("id", msgID), zap.Error(err))
	}
}
