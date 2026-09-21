package core

import "time"

// SmartRouter is optional request orchestration, separate from endpoint filters.
type SmartRouter interface {
	Prepare(*GatewayContext) error
	Invoke(*GatewayContext, *Pipeline) (bool, error)
}

// SmartRoutingRecord contains routing metadata only; never raw prompts or keys.
type SmartRoutingRecord struct {
	Model           string            `json:"model"`
	Version         int64             `json:"version"`
	PromptVersion   string            `json:"prompt_version"`
	JudgeModel      string            `json:"judge_model"`
	Score           *int              `json:"score"`
	Reason          string            `json:"reason"`
	MatchedModel    string            `json:"matched_model,omitempty"`
	ExecutedModel   string            `json:"executed_model,omitempty"`
	Escalations     []SmartEscalation `json:"escalations,omitempty"`
	JudgeDurationMs int64             `json:"judge_duration_ms"`
	DurationMs      int64             `json:"duration_ms"`
	JudgeUsage      *SmartJudgeUsage  `json:"judge_usage"`
}

type SmartEscalation struct {
	Model  string `json:"model"`
	Reason string `json:"reason"`
}

type SmartJudgeUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	CachedTokens int `json:"cached_tokens"`
}

// LimitReservation captures actual precharges; it survives provider retries.
type LimitReservation struct {
	Key       string
	Type      string
	Estimated int64
	Window    time.Duration
	Rate      int64
	Capacity  int64
	Burst     bool
	Settled   bool
}
