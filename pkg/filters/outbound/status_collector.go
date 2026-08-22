package outbound

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type AttemptMetric struct {
	EndpointID string `json:"endpoint_id"`
	Success    bool   `json:"success"`
}

type RequestMetric struct {
	Time                int64           `json:"time"` // Unix timestamp
	Model               string          `json:"model"`
	Success             bool            `json:"success"`
	InputTokens         int64           `json:"input_tokens"`
	OutputTokens        int64           `json:"output_tokens"`
	CachedTokens        int64           `json:"cached_tokens"`
	CacheCreationTokens int64           `json:"cache_creation_tokens"`
	Cost                float64         `json:"cost"`
	EndpointID          string          `json:"endpoint_id,omitempty"`
	TTFTMs              int64           `json:"ttft_ms,omitempty"`
	DurationMs          int64           `json:"duration_ms,omitempty"`
	Attempts            []AttemptMetric `json:"attempts"`
}

type endpointPerfWrite struct {
	EndpointID string
	TTFTMs     int64
	Output     int64
	DurationMs int64
}

func winningEndpointID(gctx *core.GatewayContext) string {
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.ID != "" {
		return gctx.SelectedEndpoint.ID
	}
	for i := len(gctx.History) - 1; i >= 0; i-- {
		if gctx.History[i].EndpointID != "" {
			return gctx.History[i].EndpointID
		}
	}
	return ""
}

func collectEndpointPerfWrite(gctx *core.GatewayContext) endpointPerfWrite {
	write := endpointPerfWrite{EndpointID: winningEndpointID(gctx)}
	if write.EndpointID == "" {
		return write
	}
	if gctx.TTFT > 0 {
		write.TTFTMs = gctx.TTFT.Milliseconds()
	}
	if gctx.Err == nil && gctx.OutputTokens > 0 && !gctx.StartTime.IsZero() {
		dur := time.Since(gctx.StartTime).Milliseconds()
		if dur > 0 {
			write.Output = int64(gctx.OutputTokens)
			write.DurationMs = dur
		}
	}
	return write
}

// StatusCollectorFilter collects model success/failure status for recent status display.
type StatusCollectorFilter struct {
	rdb        *redis.Client
	cbManager  *core.CircuitBreakerManager
	adminURL   string
	syncToken  string
	httpClient *http.Client
	logger     *zap.Logger

	// async HTTP reporting channel
	metricCh chan RequestMetric
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewStatusCollectorFilter creates a StatusCollectorFilter.
func NewStatusCollectorFilter(rdb *redis.Client, cbManager *core.CircuitBreakerManager, adminURL string, syncToken string, logger *zap.Logger) *StatusCollectorFilter {
	f := &StatusCollectorFilter{
		rdb:        rdb,
		cbManager:  cbManager,
		adminURL:   adminURL,
		syncToken:  syncToken,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		logger:     logger,
	}

	if rdb == nil && adminURL != "" {
		f.metricCh = make(chan RequestMetric, 5000)
		f.ctx, f.cancel = context.WithCancel(context.Background())
		go f.startWorker()
	}

	return f
}

func (f *StatusCollectorFilter) Name() string                        { return "status_collector" }
func (f *StatusCollectorFilter) Order() int                          { return 31 }
func (f *StatusCollectorFilter) Criticality() core.FilterCriticality { return core.BestEffort }
func (f *StatusCollectorFilter) InboundSafe()                        {}

func (f *StatusCollectorFilter) OnResponse(gctx *core.GatewayContext) error {
	if gctx.Model == "" {
		return nil
	}

	model := gctx.Model
	hasErr := gctx.Err != nil
	inputTokens := int64(gctx.InputTokens)
	outputTokens := int64(gctx.OutputTokens)
	cachedTokens := int64(gctx.CachedTokens)
	cacheCreationTokens := int64(gctx.CacheCreationTokens)
	cost := gctx.Cost
	perf := collectEndpointPerfWrite(gctx)

	// defensive truncation to ensure Redis daily metrics data integrity
	if cachedTokens+cacheCreationTokens > inputTokens {
		cachedTokens = inputTokens
		cacheCreationTokens = 0
	}

	// copy attempt history to prevent async races
	type epAttempt struct {
		endpointID string
		success    bool
	}
	var epAttempts []epAttempt
	for _, rec := range gctx.History {
		if rec.EndpointID != "" {
			epAttempts = append(epAttempts, epAttempt{
				endpointID: rec.EndpointID,
				success:    rec.Success,
			})
		}
	}

	// HTTP mode: enqueue to buffer for async batch reporting
	if f.rdb == nil {
		if f.metricCh != nil {
			var attempts []AttemptMetric
			for _, att := range epAttempts {
				attempts = append(attempts, AttemptMetric{
					EndpointID: att.endpointID,
					Success:    att.success,
				})
			}
			m := RequestMetric{
				Time:                time.Now().Unix(),
				Model:               model,
				Success:             !hasErr,
				InputTokens:         inputTokens,
				OutputTokens:        outputTokens,
				CachedTokens:        cachedTokens,
				CacheCreationTokens: cacheCreationTokens,
				Cost:                cost,
				EndpointID:          perf.EndpointID,
				TTFTMs:              perf.TTFTMs,
				DurationMs:          perf.DurationMs,
				Attempts:            attempts,
			}
			select {
			case f.metricCh <- m:
			default:
				// queue full: safely drop, don't block the request
			}
		}
		return nil
	}

	go func() {
		minute := time.Now().Unix() / 60
		var statusKey string
		var globalKey string
		if !hasErr {
			statusKey = fmt.Sprintf("aigw:status:model:%s:%d:s", model, minute)
			globalKey = fmt.Sprintf("aigw:status:global:%d:s", minute)
		} else {
			statusKey = fmt.Sprintf("aigw:status:model:%s:%d:f", model, minute)
			globalKey = fmt.Sprintf("aigw:status:global:%d:f", minute)
		}

		dateStr := time.Now().Format("2006-01-02")
		dailyReqKey := fmt.Sprintf("aigw:status:daily:req:%s", dateStr)
		dailyInputKey := fmt.Sprintf("aigw:status:daily:input_tokens:%s", dateStr)
		dailyOutputKey := fmt.Sprintf("aigw:status:daily:output_tokens:%s", dateStr)
		dailyCostKey := fmt.Sprintf("aigw:status:daily:cost:%s", dateStr)

		// use context.Background() so async writes survive request cancellation after the main goroutine exits
		bgCtx := context.Background()
		pipe := f.rdb.Pipeline()

		// 1. per-model and global per-minute stats
		pipe.Incr(bgCtx, statusKey)
		pipe.Expire(bgCtx, statusKey, 2*time.Hour)
		pipe.Incr(bgCtx, globalKey)
		pipe.Expire(bgCtx, globalKey, 2*time.Hour)

		if perf.TTFTMs > 0 {
			modelTtftSumKey := fmt.Sprintf("aigw:status:model:%s:%d:ttft_sum", model, minute)
			modelTtftCntKey := fmt.Sprintf("aigw:status:model:%s:%d:ttft_cnt", model, minute)
			pipe.IncrBy(bgCtx, modelTtftSumKey, perf.TTFTMs)
			pipe.Expire(bgCtx, modelTtftSumKey, 2*time.Hour)
			pipe.Incr(bgCtx, modelTtftCntKey)
			pipe.Expire(bgCtx, modelTtftCntKey, 2*time.Hour)
		}
		if perf.Output > 0 && perf.DurationMs > 0 {
			modelOutKey := fmt.Sprintf("aigw:status:model:%s:%d:out", model, minute)
			modelDurKey := fmt.Sprintf("aigw:status:model:%s:%d:dur_ms", model, minute)
			pipe.IncrBy(bgCtx, modelOutKey, perf.Output)
			pipe.Expire(bgCtx, modelOutKey, 2*time.Hour)
			pipe.IncrBy(bgCtx, modelDurKey, perf.DurationMs)
			pipe.Expire(bgCtx, modelDurKey, 2*time.Hour)
		}

		// 1.2 endpoint attempt stats
		for _, att := range epAttempts {
			var epKey string
			if att.success {
				epKey = fmt.Sprintf("aigw:status:endpoint:%s:%d:s", att.endpointID, minute)
			} else {
				epKey = fmt.Sprintf("aigw:status:endpoint:%s:%d:f", att.endpointID, minute)
			}
			pipe.Incr(bgCtx, epKey)
			pipe.Expire(bgCtx, epKey, 2*time.Hour)
		}

		if perf.EndpointID != "" {
			if perf.TTFTMs > 0 {
				ttftSumKey := fmt.Sprintf("aigw:status:endpoint:%s:%d:ttft_sum", perf.EndpointID, minute)
				ttftCntKey := fmt.Sprintf("aigw:status:endpoint:%s:%d:ttft_cnt", perf.EndpointID, minute)
				pipe.IncrBy(bgCtx, ttftSumKey, perf.TTFTMs)
				pipe.Expire(bgCtx, ttftSumKey, 2*time.Hour)
				pipe.Incr(bgCtx, ttftCntKey)
				pipe.Expire(bgCtx, ttftCntKey, 2*time.Hour)
			}
			if perf.Output > 0 && perf.DurationMs > 0 {
				outKey := fmt.Sprintf("aigw:status:endpoint:%s:%d:out", perf.EndpointID, minute)
				durKey := fmt.Sprintf("aigw:status:endpoint:%s:%d:dur_ms", perf.EndpointID, minute)
				pipe.IncrBy(bgCtx, outKey, perf.Output)
				pipe.Expire(bgCtx, outKey, 2*time.Hour)
				pipe.IncrBy(bgCtx, durKey, perf.DurationMs)
				pipe.Expire(bgCtx, durKey, 2*time.Hour)
			}
		}

		// 2. daily cumulative stats
		pipe.Incr(bgCtx, dailyReqKey)
		pipe.Expire(bgCtx, dailyReqKey, 48*time.Hour)

		if inputTokens > 0 {
			pipe.IncrBy(bgCtx, dailyInputKey, inputTokens)
			pipe.Expire(bgCtx, dailyInputKey, 48*time.Hour)
		}
		if outputTokens > 0 {
			pipe.IncrBy(bgCtx, dailyOutputKey, outputTokens)
			pipe.Expire(bgCtx, dailyOutputKey, 48*time.Hour)
		}
		if cachedTokens > 0 {
			dailyCachedKey := fmt.Sprintf("aigw:status:daily:cached_tokens:%s", dateStr)
			pipe.IncrBy(bgCtx, dailyCachedKey, cachedTokens)
			pipe.Expire(bgCtx, dailyCachedKey, 48*time.Hour)
		}
		if cacheCreationTokens > 0 {
			dailyCacheCreationKey := fmt.Sprintf("aigw:status:daily:cache_creation_tokens:%s", dateStr)
			pipe.IncrBy(bgCtx, dailyCacheCreationKey, cacheCreationTokens)
			pipe.Expire(bgCtx, dailyCacheCreationKey, 48*time.Hour)
		}
		if cost > 0 {
			pipe.IncrByFloat(bgCtx, dailyCostKey, cost)
			pipe.Expire(bgCtx, dailyCostKey, 48*time.Hour)
		}

		_, _ = pipe.Exec(bgCtx)
	}()

	return nil
}

func (f *StatusCollectorFilter) startWorker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var batch []RequestMetric

	for {
		select {
		case <-f.ctx.Done():
			return
		case m := <-f.metricCh:
			batch = append(batch, m)
			if len(batch) >= 100 {
				f.flush(batch)
				batch = nil
			}
		case <-ticker.C:
			// periodic heartbeat even without requests (syncs circuit breaker state)
			f.flush(batch)
			batch = nil
		}
	}
}

func (f *StatusCollectorFilter) flush(batch []RequestMetric) {
	var openEndpoints []string
	var openServices []string
	if f.cbManager != nil {
		openEndpoints, openServices = f.cbManager.GetOpenBreakers()
	}

	payload := struct {
		Metrics       []RequestMetric `json:"metrics"`
		OpenEndpoints []string        `json:"open_endpoints"`
		OpenServices  []string        `json:"open_services"`
	}{
		Metrics:       batch,
		OpenEndpoints: openEndpoints,
		OpenServices:  openServices,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		f.logger.Error("failed to marshal metrics payload", zap.Error(err))
		return
	}

	urlStr := f.adminURL + "/api/v1/gateway/metrics"
	req, err := http.NewRequestWithContext(f.ctx, "POST", urlStr, bytes.NewReader(jsonData))
	if err != nil {
		f.logger.Error("failed to create metrics request", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sync-Token", f.syncToken)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		f.logger.Warn("failed to send metrics to admin", zap.String("url", urlStr), zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		f.logger.Warn("admin returned non-ok status for metrics", zap.String("url", urlStr), zap.Int("status", resp.StatusCode))
	}
}
