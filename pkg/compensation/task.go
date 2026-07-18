// Package compensation provides a queue to retry failed critical OutboundFilter
// operations (e.g. token settlement, sticky session writes).
// Uses Redis Stream + consumer group + delayed ZSet with exponential backoff.
package compensation

import (
	"time"
)

// CompensationTask is a retryable compensation job and its context.
type CompensationTask struct {
	// ID uniquely identifies the task.
	ID string `json:"id"`
	// FilterName is the source filter (e.g. "token_settlement", "sticky_session").
	FilterName string `json:"filter_name"`
	// Payload holds data needed for the compensation action.
	Payload map[string]any `json:"payload"`
	// EnqueueAt is when the task was first enqueued.
	EnqueueAt time.Time `json:"enqueue_at"`
	// NextRetryAt is when the next delayed retry should run.
	NextRetryAt time.Time `json:"next_retry_at"`
	// AttemptCount is how many times the task has been tried.
	AttemptCount int `json:"attempt_count"`
	// LastError is the last failure message.
	LastError string `json:"last_error"`
}
