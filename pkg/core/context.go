package core

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// GatewayContext 贯穿整个管线的请求上下文
// 不实现 context.Context 接口（强类型字段优先）
type GatewayContext struct {
	// ===== 请求常量（不可变） =====
	Ctx            context.Context
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	RawBody        []byte
	RequestType    RequestType
	OriginalModel  string
	IsStream       bool

	// InboundFilter 填充
	APIKey     string
	UserID     string
	Tenant     string // Tenant identifier (for toB scenario)
	UserTenant string // User's tenant (for toC scenario, used for model filtering)
	SessionID  string

	// ===== 决策结果（Fallback 可重写 Model） =====
	Model  string
	Policy *policy.Policy

	// ===== Per-attempt（ResetAttempt 清空） =====
	SelectedInvoker  Invoker
	SelectedEndpoint *Endpoint
	UpstreamConnect  time.Time
	UpstreamResponse *http.Response
	UpstreamBody     []byte
	UpstreamError    error
	TTFT             time.Duration

	// ===== 累积字段 =====
	AttemptCount  int
	FallbackChain []string
	History       []AttemptRecord
	StartTime     time.Time
	TotalLatency  time.Duration

	// ===== 动态标签（InboundFilter 打标，全链路可读） =====
	Tags map[string]string

	// ===== 最终结果 =====
	InputTokens         int
	OutputTokens        int
	CachedTokens        int // 缓存命中/读取 Token 数
	CacheCreationTokens int // 缓存创建/写入 Token 数
	TransmittedChars    int // 已下发至客户端的响应字符数（用于异常断连时估算 Token）
	Cost                float64
	Response            interface{}
	Err                 error
	cancelTTFTimer      func()
}

// ResetAttempt 清空 per-attempt 字段
func (c *GatewayContext) ResetAttempt() {
	c.SelectedInvoker = nil
	c.SelectedEndpoint = nil
	c.UpstreamConnect = time.Time{}
	c.UpstreamResponse = nil
	c.UpstreamBody = nil
	c.UpstreamError = nil
	// TTFT 不重置 —— 一旦置位表示已发首字节
}

// RecordAttempt 推一条 attempt 记录
func (c *GatewayContext) RecordAttempt(success bool) {
	rec := AttemptRecord{
		Model:      c.Model,
		Latency:    time.Since(c.UpstreamConnect),
		StatusCode: getStatusCode(c.UpstreamResponse),
		Error:      getErrorString(c.UpstreamError),
		Success:    success,
		Timestamp:  time.Now(),
	}
	if c.SelectedEndpoint != nil {
		rec.EndpointID = c.SelectedEndpoint.ID
		rec.Provider = c.SelectedEndpoint.Provider
	}
	c.History = append(c.History, rec)
	c.AttemptCount++
}

func getStatusCode(resp *http.Response) int {
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}

func getErrorString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// ===== 池化 =====
var ctxPool = sync.Pool{
	New: func() any { return &GatewayContext{} },
}

// AcquireContext 从池中获取并初始化 GatewayContext
func AcquireContext(w http.ResponseWriter, r *http.Request) *GatewayContext {
	gctx := ctxPool.Get().(*GatewayContext)
	gctx.Ctx = r.Context()
	gctx.Request = r
	gctx.ResponseWriter = w
	gctx.StartTime = time.Now()
	gctx.Tags = make(map[string]string)
	return gctx
}

// ReleaseContext 归还 GatewayContext 到池
func ReleaseContext(gctx *GatewayContext) {
	*gctx = GatewayContext{}
	ctxPool.Put(gctx)
}

// GetHeader 获取指定 HTTP Header 键的所有属性（支持多值匹配）
func (c *GatewayContext) GetHeader(key string) []string {
	if c.Request == nil {
		return nil
	}
	actual := c.Request.Header[key]
	if len(actual) == 0 {
		val := c.Request.Header.Get(key)
		if val != "" {
			return []string{val}
		}
	}
	return actual
}

// GetQuery 获取指定 URL Query 参数的所有属性（支持多值匹配）
func (c *GatewayContext) GetQuery(key string) []string {
	if c.Request == nil || c.Request.URL == nil {
		return nil
	}
	return c.Request.URL.Query()[key]
}

// GetCookie 获取指定 Cookie 键的值
func (c *GatewayContext) GetCookie(key string) string {
	if c.Request == nil {
		return ""
	}
	cookie, err := c.Request.Cookie(key)
	if err != nil {
		return ""
	}
	return cookie.Value
}

// GetSystemValue 提取系统内置上下文变量（如 "user", "model"）
func (c *GatewayContext) GetSystemValue(key string) string {
	switch key {
	case "model":
		return c.Model
	case "user":
		return c.UserID
	}
	return ""
}

// GetTagValue 获取动态标签值
func (c *GatewayContext) GetTagValue(key string) string {
	if c.Tags == nil {
		return ""
	}
	return c.Tags[key]
}

// Logger 返回带 Trace ID 的 logger。如果 context 中有 "zapLogger"，则使用 context 中的 logger，否则返回传入的默认 logger。
func (c *GatewayContext) Logger(defaultLogger *zap.Logger) *zap.Logger {
	if c.Ctx != nil {
		if zl := c.Ctx.Value("zapLogger"); zl != nil {
			if ctxLogger, ok := zl.(*zap.Logger); ok {
				return ctxLogger
			}
		}
	}
	return defaultLogger
}

// RegisterTTFTimer 注册单次尝试的首字超时定时器取消闭包。
func (c *GatewayContext) RegisterTTFTimer(cancel func()) {
	c.cancelTTFTimer = cancel
}

// TriggerFirstByte 触发首字节返回事件，记录首包耗时 TTFT 并停止首包定时器。
func (c *GatewayContext) TriggerFirstByte() {
	if c.TTFT == 0 {
		c.TTFT = time.Since(c.StartTime)
		if c.cancelTTFTimer != nil {
			c.cancelTTFTimer()
			c.cancelTTFTimer = nil
		}
	}
}
