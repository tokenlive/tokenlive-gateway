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

// Compensator runs a compensation action.
// Each FilterName has one Compensator implementation.
type Compensator interface {
	// Compensate performs the compensation for payload.
	Compensate(ctx context.Context, payload map[string]any) error
}

// CompensatorFunc adapts a plain function to Compensator.
type CompensatorFunc func(ctx context.Context, payload map[string]any) error

// Compensate implements Compensator.
func (f CompensatorFunc) Compensate(ctx context.Context, payload map[string]any) error {
	return f(ctx, payload)
}

// Worker reads compensation tasks from Redis Stream and runs compensators.
type Worker struct {
	client redis.Cmdable
	queue  *RedisQueue
	logger *zap.Logger

	compensators map[string]Compensator
	mu           sync.RWMutex

	stopCh chan struct{}
	done   chan struct{}

	// nowFunc returns current time; overridable in tests.
	nowFunc func() time.Time
}

// NewWorker creates a Worker.
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

// RegisterCompensator registers a compensator for filterName.
func (w *Worker) RegisterCompensator(filterName string, c Compensator) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.compensators[filterName] = c
}

// Run blocks until ctx is cancelled or Close is called.
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

// Close signals the worker to stop and waits for exit.
func (w *Worker) Close() {
	close(w.stopCh)
	<-w.done
}

// runCycle claims due delayed tasks, then processes one batch.
func (w *Worker) runCycle(ctx context.Context) {
	claimed, err := w.queue.ClaimDelayed(ctx)
	if err != nil {
		w.logger.Error("claim delayed tasks failed", zap.Error(err))
	}
	if claimed > 0 {
		w.logger.Info("claimed delayed tasks", zap.Int64("count", claimed))
	}

	if err := w.ProcessBatch(ctx); err != nil {
		w.logger.Error("process batch failed", zap.Error(err))
	}
}

// ProcessBatch reads up to 10 messages from the main stream and handles them.
// Exported for tests.
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

// HandleMessage processes one stream message. Exported for tests.
func (w *Worker) HandleMessage(ctx context.Context, msg redis.XMessage) {
	data, ok := msg.Values["data"].(string)
	if !ok {
		w.logger.Error("message missing data field, acking",
			zap.String("id", msg.ID))
		w.ack(ctx, msg.ID)
		return
	}

	var task CompensationTask
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		w.logger.Error("message unmarshal failed, acking",
			zap.String("id", msg.ID), zap.Error(err))
		w.ack(ctx, msg.ID)
		return
	}

	w.mu.RLock()
	compensator, exists := w.compensators[task.FilterName]
	w.mu.RUnlock()

	if !exists {
		w.logger.Error("compensator not found, acking",
			zap.String("filter_name", task.FilterName),
			zap.String("task_id", task.ID))
		w.ack(ctx, msg.ID)
		return
	}

	err := compensator.Compensate(ctx, task.Payload)
	if err != nil {
		w.logger.Warn("compensation failed",
			zap.String("task_id", task.ID),
			zap.String("filter_name", task.FilterName),
			zap.Int("attempt", task.AttemptCount+1),
			zap.Error(err))
		w.handleFailure(ctx, msg.ID, &task, err)
		return
	}

	w.logger.Info("compensation succeeded",
		zap.String("task_id", task.ID),
		zap.String("filter_name", task.FilterName))
	w.ack(ctx, msg.ID)
}

// handleFailure moves to DLQ after max retries, otherwise schedules delayed retry.
func (w *Worker) handleFailure(ctx context.Context, msgID string, task *CompensationTask, err error) {
	task.AttemptCount++
	task.LastError = err.Error()

	if task.AttemptCount >= w.queue.MaxRetries() {
		w.moveToDLQ(ctx, task)
		w.ack(ctx, msgID)
		w.logger.Error("max retries reached, moved to DLQ",
			zap.String("task_id", task.ID),
			zap.Int("attempt_count", task.AttemptCount))
		return
	}

	// exponential backoff: attempt^2 seconds
	delaySeconds := math.Pow(float64(task.AttemptCount), 2)
	nextRetry := w.nowFunc().Add(time.Duration(delaySeconds) * time.Second)
	task.NextRetryAt = nextRetry

	w.scheduleDelayed(ctx, task)
	w.ack(ctx, msgID)
}

// scheduleDelayed puts the task in the delayed ZSet until NextRetryAt (ClaimDelayed reclaims it).
func (w *Worker) scheduleDelayed(ctx context.Context, task *CompensationTask) {
	data, err := json.Marshal(task)
	if err != nil {
		w.logger.Error("marshal delayed task failed", zap.Error(err))
		return
	}

	score := float64(task.NextRetryAt.UnixMilli())
	err = w.client.ZAdd(ctx, w.queue.DelayedKey(), redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
	if err != nil {
		w.logger.Error("add delayed task failed", zap.Error(err))
	}
}

// moveToDLQ appends the task to the dead-letter queue.
func (w *Worker) moveToDLQ(ctx context.Context, task *CompensationTask) {
	data, err := json.Marshal(task)
	if err != nil {
		w.logger.Error("marshal DLQ task failed", zap.Error(err))
		return
	}

	_, err = w.client.XAdd(ctx, &redis.XAddArgs{
		Stream: w.queue.DLQKey(),
		Values: map[string]any{
			"data": string(data),
		},
	}).Result()
	if err != nil {
		w.logger.Error("add DLQ task failed", zap.Error(err))
	}
}

// ack acknowledges a stream message.
func (w *Worker) ack(ctx context.Context, msgID string) {
	err := w.client.XAck(ctx, w.queue.StreamKey(), w.queue.GroupName(), msgID).Err()
	if err != nil {
		w.logger.Error("ack failed", zap.String("id", msgID), zap.Error(err))
	}
}
