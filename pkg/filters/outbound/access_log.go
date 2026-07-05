package outbound

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// redactKey 脱敏 API Key，只保留首尾各 4 个字符
func redactKey(key string) string {
	if len(key) <= 8 {
		return "***"
	}
	return key[:4] + "***" + key[len(key)-4:]
}

// AccessLogItem 结构化访问日志实体，供明细落库及 Redis 补偿传输使用
type AccessLogItem struct {
	RequestID           string    `json:"request_id"`
	Time                time.Time `json:"time"`
	TenantID            string    `json:"tenant_id"`
	UserID              string    `json:"user_id"`
	SessionID           string    `json:"session_id"`
	APIKey              string    `json:"api_key"`
	WorkspaceID         string    `json:"workspace_id"`
	APIKeyID            string    `json:"api_key_id"`
	APIKeyHash          string    `json:"api_key_hash"`
	ClientIP            string    `json:"client_ip"`
	OriginalModel       string    `json:"original_model"`
	Model               string    `json:"model"`
	Provider            string    `json:"provider"`
	EndpointID          string    `json:"endpoint_id"`
	IsStream            uint8     `json:"is_stream"`
	Attempts            uint8     `json:"attempts"`
	FallbackChain       []string  `json:"fallback_chain"`
	StatusCode          int16     `json:"status_code"`
	LatencyMs           uint32    `json:"latency_ms"`
	TtftMs              uint32    `json:"ttft_ms"`
	ErrorMessage        string    `json:"error_message"`
	InputTokens         uint32    `json:"input_tokens"`
	OutputTokens        uint32    `json:"output_tokens"`
	CachedTokens        uint32    `json:"cached_tokens"`
	CacheCreationTokens uint32    `json:"cache_creation_tokens"`
	Cost                float64   `json:"cost"`
}

// AccessLogFilter 结构化访问日志过滤器，支持本地 Zap 日志输出与 ClickHouse 批量异步落库（附 Redis 容灾重试）
type AccessLogFilter struct {
	logger        *zap.Logger
	rdb           redis.Cmdable
	compQueue     compensation.Queue
	chConn        clickhouse.Conn
	chEnabled     bool
	logChan       chan AccessLogItem
	batchSize     int
	flushInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewAccessLogFilter 创建 AccessLogFilter 实例，内置内存 Batcher 攒批协程
func NewAccessLogFilter(
	logger *zap.Logger,
	rdb redis.Cmdable,
	compQueue compensation.Queue,
	chConn clickhouse.Conn,
	conf *viper.Viper,
) *AccessLogFilter {
	batchSize := 2000
	flushInterval := 2 * time.Second
	chEnabled := false

	if conf != nil {
		if conf.IsSet("access_log.clickhouse.enabled") {
			chEnabled = conf.GetBool("access_log.clickhouse.enabled")
		}
		if conf.IsSet("access_log.batch.batch_size") {
			batchSize = conf.GetInt("access_log.batch.batch_size")
		}
		if conf.IsSet("access_log.batch.flush_interval") {
			flushInterval = conf.GetDuration("access_log.batch.flush_interval")
		}
	}

	f := &AccessLogFilter{
		logger:        logger,
		rdb:           rdb,
		compQueue:     compQueue,
		chConn:        chConn,
		chEnabled:     chEnabled,
		logChan:       make(chan AccessLogItem, batchSize*2),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}

	if chEnabled && chConn != nil {
		f.wg.Add(1)
		go f.startBatchLoop()
	}

	return f
}

func (f *AccessLogFilter) Name() string                        { return "access_log" }
func (f *AccessLogFilter) Order() int                          { return 40 }
func (f *AccessLogFilter) Criticality() core.FilterCriticality { return core.BestEffort }
func (f *AccessLogFilter) InboundSafe()                        {}

// OnResponse 提取 GatewayContext 字段，完成 Zap 本地日志打印与 ClickHouse 异步攒批投递
func (f *AccessLogFilter) OnResponse(gctx *core.GatewayContext) error {
	provider := ""
	endpointID := ""
	if gctx.SelectedEndpoint != nil {
		provider = gctx.SelectedEndpoint.Provider
		endpointID = gctx.SelectedEndpoint.ID
	}

	// 1. 本地结构化日志输出
	fields := []zap.Field{
		zap.String("original_model", gctx.OriginalModel),
		zap.String("model", gctx.Model),
		zap.String("provider", provider),
		zap.String("endpoint", endpointID),
		zap.Bool("stream", gctx.IsStream),
		zap.Duration("latency", time.Since(gctx.StartTime)),
		zap.Duration("ttft", gctx.TTFT),
		zap.Int("input_tokens", gctx.InputTokens),
		zap.Int("output_tokens", gctx.OutputTokens),
		zap.Int("cached_tokens", gctx.CachedTokens),
		zap.Int("cache_creation_tokens", gctx.CacheCreationTokens),
		zap.Float64("cost", gctx.Cost),
		zap.Int("attempts", gctx.AttemptCount),
		zap.Strings("fallback_chain", gctx.FallbackChain),
		zap.String("api_key", redactKey(gctx.APIKey)),
		zap.String("user_id", gctx.UserID),
		zap.String("session_id", gctx.SessionID),
	}
	if gctx.Err != nil {
		fields = append(fields, zap.Error(gctx.Err))
		gctx.Logger(f.logger).Error("request completed with error", fields...)
	} else {
		gctx.Logger(f.logger).Info("request completed", fields...)
	}

	// 2. 构造 ClickHouse 访问日志实体
	item := f.buildAccessLogItem(gctx)

	// 3. 投递至内存 Batcher 队列
	if f.chEnabled && f.chConn != nil {
		select {
		case f.logChan <- item:
		default:
			// 拥堵降级：Channel 满时直接序列化并投递进 Redis 补偿队列，保障网关绝对可用且计费数据零丢失
			f.logger.Error("Access log channel buffer overflow, falling back to Redis compensation queue directly")
			f.enqueueCompensation([]AccessLogItem{item}, fmt.Errorf("buffer channel overflow"))
		}
	}

	return nil
}

func (f *AccessLogFilter) buildAccessLogItem(gctx *core.GatewayContext) AccessLogItem {
	provider := ""
	endpointID := ""
	if gctx.SelectedEndpoint != nil {
		provider = gctx.SelectedEndpoint.Provider
		endpointID = gctx.SelectedEndpoint.ID
	}

	errMsg := ""
	statusCode := int16(200)
	if gctx.Err != nil {
		errMsg = gctx.Err.Error()
		statusCode = int16(500)
	}

	isStreamVal := uint8(0)
	if gctx.IsStream {
		isStreamVal = uint8(1)
	}

	requestID := ""
	if reqIds := gctx.GetHeader("X-Request-ID"); len(reqIds) > 0 {
		requestID = reqIds[0]
	} else if reqIds := gctx.GetHeader("X-Correlation-ID"); len(reqIds) > 0 {
		requestID = reqIds[0]
	} else {
		requestID = uuid.NewString()
	}

	tenantID := gctx.Tenant
	if tenantID == "" {
		tenantID = gctx.UserTenant
	}

	clientIP := ""
	if ips := gctx.GetHeader("X-Forwarded-For"); len(ips) > 0 {
		clientIP = ips[0]
	} else if ips := gctx.GetHeader("X-Real-IP"); len(ips) > 0 {
		clientIP = ips[0]
	} else if gctx.Request != nil {
		clientIP = gctx.Request.RemoteAddr
	}

	item := AccessLogItem{
		RequestID:           requestID,
		Time:                gctx.StartTime,
		TenantID:            tenantID,
		UserID:              gctx.UserID,
		SessionID:           gctx.SessionID,
		APIKey:              redactKey(gctx.APIKey),
		WorkspaceID:         gctx.WorkspaceID,
		APIKeyID:            gctx.APIKeyID,
		APIKeyHash:          gctx.APIKeyHash,
		ClientIP:            clientIP,
		OriginalModel:       gctx.OriginalModel,
		Model:               gctx.Model,
		Provider:            provider,
		EndpointID:          endpointID,
		IsStream:            isStreamVal,
		Attempts:            uint8(gctx.AttemptCount),
		FallbackChain:       gctx.FallbackChain,
		StatusCode:          statusCode,
		LatencyMs:           uint32(time.Since(gctx.StartTime).Milliseconds()),
		TtftMs:              uint32(gctx.TTFT.Milliseconds()),
		ErrorMessage:        errMsg,
		InputTokens:         uint32(gctx.InputTokens),
		OutputTokens:        uint32(gctx.OutputTokens),
		CachedTokens:        uint32(gctx.CachedTokens),
		CacheCreationTokens: uint32(gctx.CacheCreationTokens),
		Cost:                gctx.Cost,
	}
	return item
}

// Close 优雅下线，排空当前 Channel 中积压的所有日志并批量刷入 ClickHouse (失败则投递 Redis 补偿)
func (f *AccessLogFilter) Close() {
	if !f.chEnabled || f.chConn == nil {
		return
	}

	f.logger.Info("Stopping AccessLogFilter Batcher...")
	close(f.stopCh)
	f.wg.Wait()
	f.logger.Info("AccessLogFilter Batcher stopped gracefully")
}

// startBatchLoop 运行后台批处理循环，兼顾最大批次与最大等待时间
func (f *AccessLogFilter) startBatchLoop() {
	defer f.wg.Done()
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	var batch []AccessLogItem

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := writeBatchToClickHouse(ctx, f.chConn, batch); err != nil {
			f.logger.Error("Failed to write batch to ClickHouse, falling back to Redis compensation queue", zap.Error(err))
			f.enqueueCompensation(batch, err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case item, ok := <-f.logChan:
			if !ok {
				flush()
				return
			}
			batch = append(batch, item)
			if len(batch) >= f.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-f.stopCh:
			// 退出时非阻塞排空 channel 中积压的消息，避免丢失
			for {
				select {
				case item, ok := <-f.logChan:
					if !ok {
						flush()
						return
					}
					batch = append(batch, item)
					if len(batch) >= f.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// enqueueCompensation 组装异常任务打包，投递至 Redis 补偿 Stream
func (f *AccessLogFilter) enqueueCompensation(items []AccessLogItem, err error) {
	if f.compQueue == nil {
		f.logger.Warn("Redis compensation queue not configured, access logs will be lost!", zap.Int("count", len(items)))
		return
	}

	taskID := fmt.Sprintf("access_log-%d-%d", time.Now().UnixNano(), len(items))
	task := &compensation.CompensationTask{
		ID:         taskID,
		FilterName: "access_log",
		Payload: map[string]any{
			"logs": items,
		},
		EnqueueAt: time.Now(),
		LastError: err.Error(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if qerr := f.compQueue.Enqueue(ctx, task); qerr != nil {
		f.logger.Error("Enqueue access logs to compensation queue failed, logs completely lost!", zap.String("taskId", taskID), zap.Error(qerr))
	} else {
		f.logger.Warn("Enqueued failed access logs batch to Redis compensation queue successfully", zap.String("taskId", taskID), zap.Int("count", len(items)))
	}
}
