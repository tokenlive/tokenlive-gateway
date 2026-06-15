// Package compensation 提供补偿队列，用于重试失败的关键 OutboundFilter 操作（如 token 结算、粘性会话写入）。
// 使用 Redis Stream + Consumer Group + 延迟 ZSet 实现指数退避重试。
package compensation

import (
	"time"
)

// CompensationTask 表示一个补偿任务，记录需要重试的操作及其上下文。
type CompensationTask struct {
	// ID 任务唯一标识
	ID string `json:"id"`
	// FilterName 产生此任务的过滤器名称（如 "token_settlement", "sticky_session"）
	FilterName string `json:"filter_name"`
	// Payload 补偿操作所需的业务数据
	Payload map[string]any `json:"payload"`
	// EnqueueAt 任务首次入队时间
	EnqueueAt time.Time `json:"enqueue_at"`
	// NextRetryAt 下次重试时间（用于延迟重试）
	NextRetryAt time.Time `json:"next_retry_at"`
	// AttemptCount 已尝试次数
	AttemptCount int `json:"attempt_count"`
	// LastError 上次失败的错误信息
	LastError string `json:"last_error"`
}
