package core

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

// ErrGatewayFirstByteTimeout indicates an active disconnect caused by gateway timeout policy (connect timeout + first-byte timeout).
var ErrGatewayFirstByteTimeout = errors.New("gateway policy timeout: first byte timeout (connect timeout + ttft timeout exceeded)")

// ErrClientDisconnected indicates that the downstream client canceled the request.
// It is not an upstream provider failure and must not affect retry or circuit-breaker health.
var ErrClientDisconnected = errors.New("client disconnected")

// GatewayContext is the request context threaded through the entire pipeline.
// Does not implement context.Context (strong-typed fields preferred).
type GatewayContext struct {
	// ===== Request constants (immutable) =====
	Ctx            context.Context
	Request        *http.Request
	ResponseWriter http.ResponseWriter
	RawBody        []byte
	RequestType    RequestType
	OriginalModel  string
	IsStream       bool

	// Populated by InboundFilter
	APIKey      string
	APIKeyID    string
	APIKeyHash  string
	UserID      string
	Tenant      string // Tenant identifier (for toB scenario)
	WorkspaceID string // Workspace identifier (for Portal scenario)
	UserTenant  string // User's tenant (for toC scenario, used for model filtering)
	SessionID   string

	// ===== Decision results (Fallback may rewrite Model) =====
	Model  string
	Policy *policy.Policy

	// ===== Per-attempt (ResetAttempt clears these) =====
	SelectedInvoker  Invoker
	SelectedEndpoint *Endpoint
	UpstreamConnect  time.Time
	UpstreamResponse *http.Response
	UpstreamBody     []byte
	UpstreamError    error
	TTFT             time.Duration
	FatalErr         error // Fatal error; when set, skips retry and fallback, terminates immediately

	// ===== Cumulative fields =====
	AttemptCount  int
	FallbackChain []string
	History       []AttemptRecord
	StartTime     time.Time

	// ===== Dynamic tags (labeled by InboundFilter, readable across the pipeline) =====
	Tags            map[string]string
	InjectedHeaders map[string]string // request headers injected at runtime (appended to upstream request in Invoker phase)

	// ===== Final results =====
	PolicyEventEmitted  bool `json:"-"` // Set true after ClusterInvoker emits a policy-error event; OutboundFilter suppresses fallback event accordingly
	InputTokens         int
	OutputTokens        int
	CachedTokens        int // tokens from cache hit/read
	CacheCreationTokens int // tokens from cache creation/write
	TransmittedChars    int // response chars sent to client (used to estimate tokens on abnormal disconnect)
	Cost                float64
	Response            interface{}
	Err                 error
	cancelTTFTimer      func()
}

// perAttemptTagKeys lists dynamic tag keys cleared on each retry.
// Register new per-attempt tags here to prevent stale value leakage on retry.
var perAttemptTagKeys = []string{
	"response_id",
	"response_model",
	"response_completed_sent",
	"message_stop_sent",
	"upstream_finish_reason",
	"anthropic_stop_reason",
	"stream_saw_done",
	"transmitted_chars",
}

// ResetAttempt clears per-attempt fields and token stats to prevent stale values on retry.
func (c *GatewayContext) ResetAttempt() {
	c.SelectedInvoker = nil
	c.SelectedEndpoint = nil
	c.UpstreamConnect = time.Time{}
	c.UpstreamResponse = nil
	c.UpstreamBody = nil
	c.UpstreamError = nil
	// TTFT is not reset — once set, the first byte has been sent

	// Clear token stats for this attempt to avoid polluting final results
	c.InputTokens = 0
	c.OutputTokens = 0
	c.CachedTokens = 0
	c.CacheCreationTokens = 0
	c.TransmittedChars = 0
	c.Cost = 0
	c.Response = nil
	c.Err = nil

	// Clear dynamic tags related to this attempt
	if c.Tags != nil {
		for _, k := range perAttemptTagKeys {
			delete(c.Tags, k)
		}
	}
}

// RecordAttempt appends an attempt record to history.
func (c *GatewayContext) RecordAttempt(success bool) {
	rec := AttemptRecord{
		Model:      c.Model,
		Latency:    time.Since(c.UpstreamConnect),
		StatusCode: getStatusCode(c.UpstreamResponse),
		Body:       append([]byte(nil), c.UpstreamBody...),
		Error:      getErrorString(c.UpstreamError),
		Success:    success,
		Timestamp:  time.Now(),
	}
	if c.UpstreamResponse != nil {
		rec.ContentType = c.UpstreamResponse.Header.Get("Content-Type")
	}
	if c.SelectedEndpoint != nil {
		rec.EndpointID = c.SelectedEndpoint.ID
		rec.EndpointCode = c.SelectedEndpoint.Code
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

// ===== Pooling =====
var ctxPool = sync.Pool{
	New: func() any { return &GatewayContext{} },
}

// AcquireContext gets and initializes a GatewayContext from the pool.
func AcquireContext(w http.ResponseWriter, r *http.Request) *GatewayContext {
	gctx := ctxPool.Get().(*GatewayContext)
	gctx.Ctx = r.Context()
	gctx.Request = r
	gctx.ResponseWriter = w
	gctx.StartTime = time.Now()
	gctx.Tags = make(map[string]string)
	gctx.InjectedHeaders = make(map[string]string)
	return gctx
}

// ReleaseContext returns a GatewayContext to the pool.
func ReleaseContext(gctx *GatewayContext) {
	*gctx = GatewayContext{}
	ctxPool.Put(gctx)
}

// GetHeader returns all values for the given HTTP header key (supports multi-value).
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

// GetQuery returns all values for the given URL query parameter (supports multi-value).
func (c *GatewayContext) GetQuery(key string) []string {
	if c.Request == nil || c.Request.URL == nil {
		return nil
	}
	return c.Request.URL.Query()[key]
}

// GetCookie returns the value of the given cookie key.
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

// GetSystemValue extracts built-in context variables (e.g. "user", "model").
func (c *GatewayContext) GetSystemValue(key string) string {
	switch key {
	case "model":
		return c.Model
	case "user":
		return c.UserID
	}
	return ""
}

// GetTagValue returns the value of a dynamic tag.
func (c *GatewayContext) GetTagValue(key string) string {
	if c.Tags == nil {
		return ""
	}
	return c.Tags[key]
}

// Logger returns a logger with Trace ID. Uses the logger from context if "zapLogger" is present, otherwise returns the provided default.
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

// RegisterTTFTimer registers the cancel closure for the per-attempt first-byte timeout timer.
func (c *GatewayContext) RegisterTTFTimer(cancel func()) {
	c.cancelTTFTimer = cancel
}

// TriggerFirstByte records TTFT and stops the first-byte timer on the first byte event.
func (c *GatewayContext) TriggerFirstByte() {
	if c.TTFT == 0 {
		c.TTFT = time.Since(c.StartTime)
		if c.cancelTTFTimer != nil {
			c.cancelTTFTimer()
			c.cancelTTFTimer = nil
		}
	}
}
