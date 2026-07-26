package providers

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestHandleMessagesStream_ReasoningContent(t *testing.T) {
	// Simulate GLM-5.2 style SSE stream containing reasoning_content
	upstreamSSE := `data: {"id":"chatcmpl-123","model":"glm-5.2","choices":[{"delta":{"reasoning_content":"Thinking deeply..."}}]}

data: {"id":"chatcmpl-123","model":"glm-5.2","choices":[{"delta":{"content":"Hello world!"}}]}

data: {"id":"chatcmpl-123","model":"glm-5.2","choices":[{"delta":{},"finish_reason":"stop"}]}

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
		RequestType:    core.RequestTypeMessages,
		Tags:           make(map[string]string),
	}

	err := handleMessagesStream(gctx, resp)
	assert.NoError(t, err)

	out := rec.Body.String()
	assert.Contains(t, out, "event: message_start")
	assert.Contains(t, out, `"type":"thinking"`)
	assert.Contains(t, out, `"type":"thinking_delta"`)
	assert.Contains(t, out, "Thinking deeply...")
	assert.Contains(t, out, `"type":"text_delta"`)
	assert.Contains(t, out, "Hello world!")
	assert.Contains(t, out, `"stop_reason":"end_turn"`)
	assert.Contains(t, out, "event: message_stop")
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
}

func TestHandleMessagesStream_EmptyUpstreamStreamWithDone(t *testing.T) {
	// Upstream sends no content chunks before [DONE] — valid empty completion.
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
		RequestType:    core.RequestTypeMessages,
		Tags:           make(map[string]string),
	}

	err := handleMessagesStream(gctx, resp)
	assert.NoError(t, err)

	out := rec.Body.String()
	assert.Contains(t, out, "event: message_start")
	assert.Contains(t, out, "event: content_block_start")
	assert.Contains(t, out, "event: content_block_stop")
	assert.Contains(t, out, "event: message_delta")
	assert.Contains(t, out, "event: message_stop")
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
}

func TestHandleMessagesStream_PrematureEOF_DoesNotForgeEndTurn(t *testing.T) {
	// Upstream starts streaming then drops without [DONE] / finish_reason.
	upstreamSSE := "data: {\"id\":\"chatcmpl-x\",\"model\":\"glm-5.1\",\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking...\"}}]}\n\n" +
		"data: {\"id\":\"chatcmpl-x\",\"model\":\"glm-5.1\",\"choices\":[{\"delta\":{\"content\":\"partial answer\"}}]}\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
	}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Model:          "glm-5.1",
		OriginalModel:  "glm-5.1",
		IsStream:       true,
		RequestType:    core.RequestTypeMessages,
		Tags:           make(map[string]string),
	}

	err := handleMessagesStream(gctx, resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upstream stream closed prematurely without completion event")

	out := rec.Body.String()
	assert.Contains(t, out, "partial answer")
	// Must NOT forge a successful Anthropic completion.
	assert.NotContains(t, out, `"stop_reason":"end_turn"`)
	assert.NotContains(t, out, "event: message_stop")
	assert.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
}

func TestHandleMessagesStream_ShortEndTurn_Passthrough(t *testing.T) {
	// Legitimate upstream stop must be forwarded as a normal end_turn, even if short.
	upstreamSSE := `data: {"id":"chatcmpl-1","choices":[{"delta":{"content":"参照 route/rule/rule_set_remote.go 的模式："}}],"usage":{"prompt_tokens":138000,"completion_tokens":10}}

data: {"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":138000,"completion_tokens":16}}

data: [DONE]

`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
	}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Model:          "GLM-5.1",
		OriginalModel:  "claude-opus-5.1",
		IsStream:       true,
		RequestType:    core.RequestTypeMessages,
		Tags:           make(map[string]string),
	}

	err := handleMessagesStream(gctx, resp)
	require.NoError(t, err)
	out := rec.Body.String()
	assert.Contains(t, out, "参照 route/rule/rule_set_remote.go 的模式：")
	assert.Contains(t, out, `"stop_reason":"end_turn"`)
	assert.Contains(t, out, "event: message_stop")
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
	assert.Equal(t, "stop", gctx.Tags["upstream_finish_reason"])
}

func TestHandleMessagesStream_FinishReasonMaxTokens(t *testing.T) {
	upstreamSSE := `data: {"id":"chatcmpl-1","choices":[{"delta":{"content":"cut off"}}]}

data: {"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"length"}]}

data: [DONE]

`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
	}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Model:          "glm-5.1",
		IsStream:       true,
		Tags:           make(map[string]string),
	}

	err := handleMessagesStream(gctx, resp)
	require.NoError(t, err)
	assert.Contains(t, rec.Body.String(), `"stop_reason":"max_tokens"`)
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
}

func TestHandleMessagesStream_FinishReasonToolCalls(t *testing.T) {
	upstreamSSE := `data: {"id":"chatcmpl-1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":1}"}}]}}]}

data: {"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(upstreamSSE)),
	}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Model:          "glm-5.1",
		IsStream:       true,
		Tags:           make(map[string]string),
	}

	err := handleMessagesStream(gctx, resp)
	require.NoError(t, err)
	out := rec.Body.String()
	assert.Contains(t, out, `"type":"tool_use"`)
	assert.Contains(t, out, `"stop_reason":"tool_use"`)
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
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

func TestMapOpenAIFinishReason(t *testing.T) {
	assert.Equal(t, "end_turn", mapOpenAIFinishReason("stop"))
	assert.Equal(t, "max_tokens", mapOpenAIFinishReason("length"))
	assert.Equal(t, "tool_use", mapOpenAIFinishReason("tool_calls"))
	assert.Equal(t, "tool_use", mapOpenAIFinishReason("function_call"))
	assert.Equal(t, "end_turn", mapOpenAIFinishReason(""))
}
