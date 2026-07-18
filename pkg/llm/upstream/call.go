package upstream

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

// StreamDisposition controls body ownership after a successful Call.
// Call never closes body; caller must Close. Semantics:
//   - Consume: caller reads and closes (passthrough stream/non-stream)
//   - Handoff: body passed to translate invoker; cancel on Close
type StreamDisposition int

const (
	// Consume: caller consumes and closes body.
	Consume StreamDisposition = iota
	// Handoff: body handed to translate layer; transport does not defer close.
	Handoff
)

// Request is caller-controlled params for one upstream HTTP POST.
// Auth, Content-Type, body by caller; timeout and common headers by Call.
type Request struct {
	Client *http.Client
	URL    string
	Body   []byte
	Header http.Header
	// Stream only applies when gctx.IsStream.
	Stream StreamDisposition
}

// Call POSTs upstream until a successful *http.Response.
// On success sets gctx.UpstreamResponse; body Close cancels attempt context.
// status>=400: read body into gctx.UpstreamBody and return error.
func Call(gctx *core.GatewayContext, req Request) (*http.Response, error) {
	if req.Client == nil {
		return nil, fmt.Errorf("upstream call: nil http.Client")
	}
	if req.URL == "" {
		return nil, fmt.Errorf("upstream call: empty URL")
	}

	totalTimeout := 60000
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout > 0 {
		totalTimeout = gctx.Policy.InvocationPolicy.RetryPolicy.TotalTimeout
	} else if gctx.IsStream {
		totalTimeout = 600000
	}

	firstByteTimeout := totalTimeout
	if gctx.Policy != nil && gctx.Policy.InvocationPolicy != nil && gctx.Policy.InvocationPolicy.RetryPolicy != nil {
		rp := gctx.Policy.InvocationPolicy.RetryPolicy
		if rp.ConnectTimeout > 0 || rp.TtftTimeout > 0 {
			firstByteTimeout = rp.ConnectTimeout + rp.TtftTimeout
		}
	}

	singleCtx, singleCancel := context.WithCancelCause(gctx.Ctx)

	timer := time.AfterFunc(time.Duration(firstByteTimeout)*time.Millisecond, func() {
		if gctx.TTFT == 0 {
			singleCancel(core.ErrGatewayFirstByteTimeout)
		}
	})
	gctx.RegisterTTFTimer(func() {
		timer.Stop()
	})

	httpReq, err := http.NewRequestWithContext(singleCtx, http.MethodPost, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		timer.Stop()
		singleCancel(nil)
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, vs := range req.Header {
		for _, v := range vs {
			httpReq.Header.Add(k, v)
		}
	}

	var ua string
	if gctx.Request != nil {
		ua = gctx.Request.Header.Get("User-Agent")
	}
	if ua == "" {
		ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	}
	httpReq.Header.Set("User-Agent", ua)

	if gctx.SelectedEndpoint != nil && len(gctx.SelectedEndpoint.Headers) > 0 {
		for k, v := range gctx.SelectedEndpoint.Headers {
			httpReq.Header.Set(k, v)
		}
	}
	if len(gctx.InjectedHeaders) > 0 {
		for k, v := range gctx.InjectedHeaders {
			httpReq.Header.Set(k, v)
		}
	}

	var endpointID string
	if gctx.SelectedEndpoint != nil {
		endpointID = gctx.SelectedEndpoint.ID
	}
	gctx.Logger(zap.L()).Debug("sending request to upstream with headers",
		zap.String("endpoint_id", endpointID),
		zap.String("url", httpReq.URL.String()),
		zap.Any("headers", httpReq.Header),
	)

	resp, err := req.Client.Do(httpReq)
	if err != nil {
		timer.Stop()
		cause := context.Cause(singleCtx)
		singleCancel(nil)
		if cause == core.ErrGatewayFirstByteTimeout {
			return nil, fmt.Errorf("upstream request timeout (gateway policy active disconnect, first byte timeout): %w", err)
		}
		return nil, fmt.Errorf("upstream request: %w", err)
	}

	gctx.UpstreamResponse = resp

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		timer.Stop()
		singleCancel(nil)
		gctx.UpstreamBody = respBody
		gctx.Logger(zap.L()).Warn("upstream error details",
			zap.String("endpoint_id", endpointID),
			zap.Int("status", resp.StatusCode),
			zap.String("req_body", string(req.Body)),
			zap.String("resp_body", string(respBody)),
		)
		return nil, fmt.Errorf("upstream error: status %d, body: %s", resp.StatusCode, string(respBody))
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
			resp.Body.Close()
			timer.Stop()
			singleCancel(nil)
			gctx.UpstreamBody = body
			return nil, fmt.Errorf("upstream stream request returned non-stream content-type: %s, body: %s", contentType, string(body))
		}
	}

	// Success: body owned by caller; Close stops first-byte timer and cancels attempt.
	resp.Body = &cancelReadCloser{
		ReadCloser: resp.Body,
		onClose: func() {
			timer.Stop()
			singleCancel(nil)
		},
	}
	return resp, nil
}

type cancelReadCloser struct {
	io.ReadCloser
	onClose func()
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	if c.onClose != nil {
		c.onClose()
		c.onClose = nil
	}
	return err
}
