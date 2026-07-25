package providers

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestHandleMessagesStream_ReasoningContent(t *testing.T) {
	// Simulate GLM-5.2 style SSE stream containing reasoning_content
	upstreamSSE := `data: {"id":"chatcmpl-123","model":"glm-5.2","choices":[{"delta":{"reasoning_content":"Thinking deeply..."}}]}

data: {"id":"chatcmpl-123","model":"glm-5.2","choices":[{"delta":{"content":"Hello world!"}}]}

data: [DONE]

`

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
	}

	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Model:          "glm-5.2",
		IsStream:       true,
	}

	err := handleMessagesStream(gctx, resp)
	assert.NoError(t, err)

	out := rec.Body.String()
	assert.Contains(t, out, "event: message_start")
	assert.Contains(t, out, "event: content_block_start")
	assert.Contains(t, out, "Thinking deeply...")
	assert.Contains(t, out, "Hello world!")
	assert.Contains(t, out, "event: message_delta")
	assert.Contains(t, out, "event: message_stop")
}

func TestHandleMessagesStream_EmptyUpstreamStream(t *testing.T) {
	// Upstream sends no content chunks before closing
	upstreamSSE := `data: [DONE]

`

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
	}

	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Model:          "glm-5.2",
		IsStream:       true,
	}

	err := handleMessagesStream(gctx, resp)
	assert.NoError(t, err)

	out := rec.Body.String()
	assert.Contains(t, out, "event: message_start")
	assert.Contains(t, out, "event: content_block_start")
	assert.Contains(t, out, "event: content_block_stop")
	assert.Contains(t, out, "event: message_delta")
	assert.Contains(t, out, "event: message_stop")
}

func TestHandleMessagesStream_HTMLError(t *testing.T) {
	htmlResp := `<!DOCTYPE html><html><head><title>502 Bad Gateway</title></head><body>Proxy Error</body></html>`

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(htmlResp)),
	}

	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Model:          "glm-5.2",
		IsStream:       true,
	}

	err := handleMessagesStream(gctx, resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upstream returned HTML error response")
}
