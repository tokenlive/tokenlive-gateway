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
	Attempts            []AttemptMetric `json:"attempts"`
}

// StatusCollectorFilter 收集模型成功/失败的过滤器，用于最近状态显示
type StatusCollectorFilter struct {
	rdb        *redis.Client
	cbManager  *core.CircuitBreakerManager
	adminURL   string
	syncToken  string
	httpClient *http.Client
	logger     *zap.Logger

	// HTTP 异步上报通道
	metricCh chan RequestMetric
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewStatusCollectorFilter 创建 StatusCollectorFilter
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

	// 防御性截断，确保写入 Redis 日指标的数据逻辑绝对成立
	if cachedTokens+cacheCreationTokens > inputTokens {
		cachedTokens = inputTokens
		cacheCreationTokens = 0
	}

	// 提前拷贝 AttemptHistory 中各 endpoint 尝试记录，防止异步竞争
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

	// HTTP 模式下，直接加入缓冲队列进行异步批量上报
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
				Attempts:            attempts,
			}
			select {
			case f.metricCh <- m:
			default:
				// 队列满则安全丢弃，不阻塞请求主流程
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

		// 必须采用 context.Background()，以确保在 HTTP 请求结束、主协程退出后，异步指标写入不会因原 Context 的 Cancel 而中断。
		bgCtx := context.Background()
		pipe := f.rdb.Pipeline()

		// 1. 模型分钟级统计与全局分钟级统计
		pipe.Incr(bgCtx, statusKey)
		pipe.Expire(bgCtx, statusKey, 2*time.Hour)
		pipe.Incr(bgCtx, globalKey)
		pipe.Expire(bgCtx, globalKey, 2*time.Hour)

		// 1.2 端点尝试统计
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

		// 2. 自然日累计值统计
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
			// 即使无请求，也定期发送心跳（用于同步熔断器状态）
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
