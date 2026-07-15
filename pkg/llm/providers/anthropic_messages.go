package providers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"

	"go.uber.org/zap"
)

type anthropicMessagesInvoker struct{}

func (i *anthropicMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	ap, ok := p.(*AnthropicProvider)
	if !ok {
		return fmt.Errorf("expected *AnthropicProvider, got %T", p)
	}

	endpoint := ap.baseURL + "/messages"

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
	defer singleCancel(nil)

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

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, endpoint, bytes.NewReader(gctx.RawBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	
	isOAuth := false
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.AuthType == "oauth_token" {
		isOAuth = true
	}

	if isOAuth {
		req.Header.Set("Authorization", "Bearer "+ap.apiKey)
	} else {
		req.Header.Set("x-api-key", ap.apiKey)
		// 如果是非官方 Anthropic 域名且 apiKey 不为空，则自动补充 Authorization: Bearer <key>
		// 以兼容类似于商汤(Sensenova)等使用 Anthropic 协议但采用 OpenAI 鉴权机制的第三方提供商
		if ap.apiKey != "" && !strings.Contains(ap.baseURL, "anthropic.com") {
			req.Header.Set("Authorization", "Bearer "+ap.apiKey)
		}
	}
	req.Header.Set("anthropic-version", "2023-06-01")

	var ua string
	if gctx.Request != nil {
		ua = gctx.Request.Header.Get("User-Agent")
	}
	if ua == "" {
		ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	}
	req.Header.Set("User-Agent", ua)

	if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
		for k, v := range gctx.SelectedEndpoint.Headers {
			req.Header.Set(k, v)
		}
	}

	if len(gctx.InjectedHeaders) > 0 {
		for k, v := range gctx.InjectedHeaders {
			req.Header.Set(k, v)
		}
	}

	var endpointID string
	if gctx.SelectedEndpoint != nil {
		endpointID = gctx.SelectedEndpoint.ID
	}
	gctx.Logger(zap.L()).Debug("sending request to upstream with headers",
		zap.String("endpoint_id", endpointID),
		zap.String("url", req.URL.String()),
		zap.Any("headers", req.Header),
	)

	resp, err := ap.client.Do(req)
	if err != nil {
		if context.Cause(singleCtx) == core.ErrGatewayFirstByteTimeout {
			return fmt.Errorf("upstream request timeout (gateway policy active disconnect, first byte timeout): %w", err)
		}
		return fmt.Errorf("upstream request: %w", err)
	}
	defer func() {
		resp.Body.Close()
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
		return ap.handleMessagesStream(gctx, resp)
	}
	return ap.handleMessagesNonStream(gctx, resp)
}
