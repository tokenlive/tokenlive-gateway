package outbound

import (
	"context"
	"fmt"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/redis/go-redis/v9"
)

// StatusCollectorFilter 收集模型成功/失败的过滤器，用于最近状态显示
type StatusCollectorFilter struct {
	rdb *redis.Client
}

// NewStatusCollectorFilter 创建 StatusCollectorFilter
func NewStatusCollectorFilter(rdb *redis.Client) *StatusCollectorFilter {
	return &StatusCollectorFilter{
		rdb: rdb,
	}
}

func (f *StatusCollectorFilter) Name() string                        { return "status_collector" }
func (f *StatusCollectorFilter) Order() int                          { return 31 }
func (f *StatusCollectorFilter) Criticality() core.FilterCriticality { return core.BestEffort }

func (f *StatusCollectorFilter) OnResponse(gctx *core.GatewayContext) error {
	if f.rdb == nil || gctx.Model == "" {
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
