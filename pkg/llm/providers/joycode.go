package providers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"

	"github.com/google/uuid"
)

func init() {
	core.RegisterProviderFactory(core.ProviderJoyCode, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewJoyCodeProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeChatCompletion, &joycodeChatInvoker{})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeResponses, &joycodeResponsesInvoker{})
}

type JoyCodeProvider struct {
	name    string
	baseURL string
	apiKey  string // 对应 SECRET_KEY
	client  *http.Client
	models  []string
}

func NewJoyCodeProvider(name, baseURL, apiKey string, models []string) *JoyCodeProvider {
	if apiKey == "" {
		apiKey = "0691a3f0b37b4a85aeb63ad0fc7db3ed"
	}
	return &JoyCodeProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{},
		models:  models,
	}
}

func (p *JoyCodeProvider) Name() string            { return p.name }
func (p *JoyCodeProvider) Type() core.ProviderType { return core.ProviderJoyCode }
func (p *JoyCodeProvider) ValidateConfig() error   { return nil }
func (p *JoyCodeProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{
		core.RequestTypeChatCompletion,
		core.RequestTypeResponses,
	}
}

func (p *JoyCodeProvider) HealthCheck(ctx context.Context) error {
	return nil
}

func (p *JoyCodeProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

type joycodeChatInvoker struct{}

func (i *joycodeChatInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("invalid provider type: expected *JoyCodeProvider, got %T", p)
	}
	return jp.doRequest(gctx, "chat_completions")
}

func (p *JoyCodeProvider) doRequest(gctx *core.GatewayContext, functionId string) error {
	appid := "joycode_ide"
	tStr := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)

	// 计算 HMAC-SHA256 签名，拼接格式同 Python values 连接：appid&functionId&t
	signString := fmt.Sprintf("%s&%s&%s", appid, functionId, tStr)
	mac := hmac.New(sha256.New, []byte(p.apiKey))
	mac.Write([]byte(signString))
	sign := hex.EncodeToString(mac.Sum(nil))

	// 拼接带签名的 URL
	url := fmt.Sprintf("%s?appid=%s&functionId=%s&t=%s&sign=%s", p.baseURL, appid, functionId, tStr, sign)

	// 动态解析超时，如果没有配置具体首字超时，则使用最大超时时间进行等待
	totalTimeout := 60000
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout > 0 {
		totalTimeout = gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout
	} else if gctx.IsStream {
		totalTimeout = 600000
	}

	firstByteTimeout := totalTimeout
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		p := gctx.Policy.InvocationPolicy.RetryPolicy
		if p.ConnectTimeout > 0 || p.TtftTimeout > 0 {
			firstByteTimeout = p.ConnectTimeout + p.TtftTimeout
		}
	}

	singleCtx, singleCancel := context.WithCancelCause(gctx.Ctx)
	shouldCancel := true
	defer func() {
		if shouldCancel {
			singleCancel(nil)
		}
	}()

	// 注册首包前定时器
	timer := time.AfterFunc(time.Duration(firstByteTimeout)*time.Millisecond, func() {
		if gctx.TTFT == 0 {
			singleCancel(core.ErrGatewayFirstByteTimeout)
		}
	})
	defer timer.Stop()

	gctx.RegisterTTFTimer(func() {
		timer.Stop()
	})

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, url, bytes.NewReader(gctx.RawBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// 默认头部设置
	req.Host = "api-ai.jd.com"
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("accept", "*/*")
	req.Header.Set("accept-language", "*")
	req.Header.Set("sec-fetch-mode", "cors")
	req.Header.Set("user-agent", "node")

	// 动态生成 x-ms-client-request-id
	taskUUID := uuid.New().String()
	sessionUUID := uuid.New().String()[:8]
	clientReqID := fmt.Sprintf("task-%s_session-%s_%s", taskUUID, sessionUUID, tStr)
	req.Header.Set("x-ms-client-request-id", clientReqID)

	// 透传 ptKey, loginType, tenant 头部逻辑
	ptKey := gctx.Request.Header.Get("ptKey")
	loginType := gctx.Request.Header.Get("loginType")
	tenant := gctx.Request.Header.Get("tenant")

	if ptKey == "" && gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
		ptKey = gctx.SelectedEndpoint.Headers["ptKey"]
	}
	if loginType == "" && gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
		loginType = gctx.SelectedEndpoint.Headers["loginType"]
	}
	if tenant == "" && gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
		tenant = gctx.SelectedEndpoint.Headers["tenant"]
	}

	if ptKey != "" {
		req.Header.Set("ptKey", ptKey)
	}
	if loginType != "" {
		req.Header.Set("loginType", loginType)
	}
	if tenant != "" {
		req.Header.Set("tenant", tenant)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		if context.Cause(singleCtx) == core.ErrGatewayFirstByteTimeout {
			return fmt.Errorf("upstream request timeout (gateway policy active disconnect, first byte timeout): %w", err)
		}
		return fmt.Errorf("upstream request: %w", err)
	}

	shouldCloseBody := true
	defer func() {
		if shouldCloseBody {
			resp.Body.Close()
		}
	}()
	gctx.UpstreamResponse = resp

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		gctx.UpstreamBody = body
		return fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(body))
	}

	if gctx.IsStream {
		idleTimeout := 0
		if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
			idleTimeout = gctx.Policy.InvocationPolicy.RetryPolicy.IdleTimeout
		}
		if idleTimeout > 0 {
			resp.Body = llm.WrapIdleTimeoutReader(resp.Body, time.Duration(idleTimeout)*time.Millisecond, func() { singleCancel(nil) })
		}

		contentType := resp.Header.Get("Content-Type")
		if !strings.Contains(contentType, "text/event-stream") {
			body, _ := io.ReadAll(resp.Body)
			gctx.UpstreamBody = body
			return fmt.Errorf("upstream stream request returned non-stream content-type: %s, body: %s", contentType, string(body))
		}
		if gctx.RequestType == core.RequestTypeMessages || (gctx.RequestType == core.RequestTypeResponses && functionId == "chat_completions") {
			shouldCloseBody = false
			shouldCancel = false
			resp.Body = &cancelReadCloser{
				ReadCloser: resp.Body,
				cancel:     func() { singleCancel(nil) },
			}
			return nil
		}
		if functionId == "responses_completions" {
			resp.Body = newJoyCodeSanitizedReader(resp.Body)
		}
		return handleOpenAIStream(gctx, resp)
	}
	return handleOpenAINonStream(gctx, resp)
}

type joycodeResponsesInvoker struct{}

func (i *joycodeResponsesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("invalid provider type: expected *JoyCodeProvider, got %T", p)
	}

	// 1. 分流判定：端点是否原生支持 responses
	hasResponseCapability := false
	if gctx.SelectedEndpoint != nil {
		for _, cap := range gctx.SelectedEndpoint.RequestTypes {
			if cap == core.RequestTypeResponses {
				hasResponseCapability = true
				break
			}
		}
	}

	// 分支 A：原生同名转发，functionId 为 responses_completions
	if hasResponseCapability {
		return jp.doRequest(gctx, "responses_completions")
	}

	// 分支 B：协议降级与翻译 (Responses -> Chat/Completions)
	newBody, err := translateResponsesToChatCompletion(gctx.RawBody)
	if err != nil {
		return err
	}
	gctx.RawBody = newBody

	// 调用 JoyCode 的 doRequest，以 functionId=chat_completions 发送
	if err := jp.doRequest(gctx, "chat_completions"); err != nil {
		return err
	}

	// 翻译响应体 (OpenAI Chat -> Responses)
	if gctx.IsStream {
		return handleResponsesStream(gctx, gctx.UpstreamResponse)
	} else {
		if err := translateResponsesNonStreamResponse(gctx); err != nil {
			return fmt.Errorf("translate response: %w", err)
		}
	}

	return nil
}

type joycodeSanitizedReader struct {
	underlying io.ReadCloser
	reader     *bufio.Reader
	buf        bytes.Buffer
}

func newJoyCodeSanitizedReader(rc io.ReadCloser) io.ReadCloser {
	return &joycodeSanitizedReader{
		underlying: rc,
		reader:     bufio.NewReader(rc),
	}
}

func (r *joycodeSanitizedReader) Read(p []byte) (int, error) {
	if r.buf.Len() > 0 {
		return r.buf.Read(p)
	}

	line, err := r.reader.ReadString('\n')
	if len(line) > 0 {
		cleaned := line
		if strings.HasPrefix(cleaned, "data: event:") {
			cleaned = "event:" + strings.TrimPrefix(cleaned, "data: event:")
		} else if strings.HasPrefix(cleaned, "data: data:") {
			cleaned = "data:" + strings.TrimPrefix(cleaned, "data: data:")
		}
		r.buf.WriteString(cleaned)
	}

	if err != nil {
		if r.buf.Len() > 0 {
			n, _ := r.buf.Read(p)
			return n, nil
		}
		return 0, err
	}

	return r.buf.Read(p)
}

func (r *joycodeSanitizedReader) Close() error {
	return r.underlying.Close()
}
