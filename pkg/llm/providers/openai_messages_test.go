package providers

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestHandleMessagesStream_EmptyUpstreamStreamWithDone(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("data: [DONE]\n\n"))}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{ResponseWriter: rec, Model: "glm-5.2", IsStream: true, RequestType: core.RequestTypeMessages}
	require.NoError(t, handleMessagesStream(gctx, resp))
	out := rec.Body.String()
	assert.Contains(t, out, "event: message_start")
	assert.Contains(t, out, "event: content_block_start")
	assert.Contains(t, out, "event: content_block_stop")
	assert.Contains(t, out, "event: message_delta")
	assert.Contains(t, out, "event: message_stop")
	assert.True(t, rec.Flushed)
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
}

func TestHandleMessagesStream_PrematureEOF_DoesNotForgeEndTurn(t *testing.T) {
	upstreamSSE := "data: {\"id\":\"chatcmpl-x\",\"model\":\"glm-5.1\",\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking...\"}}]}\n\n" +
		"data: {\"id\":\"chatcmpl-x\",\"model\":\"glm-5.1\",\"choices\":[{\"delta\":{\"content\":\"partial answer\"}}]}\n\n"
	resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(upstreamSSE))}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{ResponseWriter: rec, Model: "glm-5.1", OriginalModel: "glm-5.1", IsStream: true, RequestType: core.RequestTypeMessages}
	err := handleMessagesStream(gctx, resp)
	require.EqualError(t, err, "upstream stream closed prematurely without completion event")
	out := rec.Body.String()
	assert.Contains(t, out, "partial answer")
	assert.Equal(t, 2, strings.Count(out, "event: content_block_stop"))
	assert.NotContains(t, out, `"stop_reason":"end_turn"`)
	assert.NotContains(t, out, "event: message_stop")
	assert.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
	assert.True(t, rec.Flushed)
	assert.Equal(t, 25, gctx.TransmittedChars)
}

func TestHandleMessagesStream_ShortEndTurn_Passthrough(t *testing.T) {
	upstreamSSE := `data: {"id":"chatcmpl-1","choices":[{"delta":{"content":"参照 route/rule/rule_set_remote.go 的模式："}}],"usage":{"prompt_tokens":138000,"completion_tokens":10}}

data: {"id":"chatcmpl-1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":138000,"completion_tokens":16}}

data: [DONE]

`
	resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(upstreamSSE))}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{ResponseWriter: rec, Model: "GLM-5.1", OriginalModel: "claude-opus-5.1", IsStream: true, RequestType: core.RequestTypeMessages}
	require.NoError(t, handleMessagesStream(gctx, resp))
	assert.Contains(t, rec.Body.String(), "参照 route/rule/rule_set_remote.go 的模式：")
	assert.Contains(t, rec.Body.String(), `"stop_reason":"end_turn"`)
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
	assert.Equal(t, "stop", gctx.Tags["upstream_finish_reason"])
	assert.Equal(t, "end_turn", gctx.Tags["anthropic_stop_reason"])
	assert.Equal(t, "true", gctx.Tags["stream_saw_done"])
	assert.Equal(t, "0", gctx.Tags["thinking_chars"])
	assert.Equal(t, 138000, gctx.InputTokens)
	assert.Equal(t, 16, gctx.OutputTokens)
}

func TestHandleMessagesStream_HTMLError(t *testing.T) {
	for _, html := range []string{`<!DOCTYPE html><html>Proxy Error</html>`, " \n<html>error</html>", `<HTML>error</HTML>`} {
		t.Run(html, func(t *testing.T) {
			body := &messagesTrackedBody{Reader: bytes.NewBufferString(html)}
			rec := httptest.NewRecorder()
			gctx := &core.GatewayContext{ResponseWriter: rec, Model: "glm-5.2", IsStream: true}
			err := handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body})
			require.EqualError(t, err, "upstream returned HTML error response instead of SSE stream")
			assert.True(t, body.closed)
			assert.Empty(t, rec.Body.String())
			assert.Zero(t, gctx.TTFT)
			assert.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
		})
	}
}

type messagesTrackedBody struct {
	io.Reader
	closed bool
}

func (b *messagesTrackedBody) Close() error {
	b.closed = true
	return nil
}

type messagesChunkBody struct {
	chunks     []string
	beforeRead func(int)
	read       int
	closed     bool
}

func (b *messagesChunkBody) Read(p []byte) (int, error) {
	if b.beforeRead != nil {
		b.beforeRead(b.read)
	}
	b.read++
	if len(b.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.chunks[0])
	b.chunks[0] = b.chunks[0][n:]
	if b.chunks[0] == "" {
		b.chunks = b.chunks[1:]
	}
	return n, nil
}

func (b *messagesChunkBody) Close() error {
	b.closed = true
	return nil
}

type messagesFlushWriter struct {
	*httptest.ResponseRecorder
	onFlush func()
	flushes int
}

func (w *messagesFlushWriter) Flush() {
	w.flushes++
	if w.onFlush != nil {
		w.onFlush()
	}
	w.ResponseRecorder.Flush()
}

func TestHandleMessagesStream_UsageTailReadThroughEOFAndMarkAfterFlush(t *testing.T) {
	for _, originalModel := range []string{"client-model", ""} {
		t.Run(originalModel, func(t *testing.T) {
			rec := &messagesFlushWriter{ResponseRecorder: httptest.NewRecorder()}
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			req.Header.Set("anthropic-version", "2023-06-01")
			gctx := &core.GatewayContext{ResponseWriter: rec, Request: req, OriginalModel: originalModel, Model: "upstream-model", InputTokens: 11, OutputTokens: 3, IsStream: true, StartTime: time.Now().Add(-time.Second)}
			firstByteCalls := 0
			gctx.RegisterTTFTimer(func() { firstByteCalls++ })
			rec.onFlush = func() { assert.NotEqual(t, "true", gctx.Tags["message_stop_sent"]) }
			body := &messagesChunkBody{chunks: []string{
				"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":7}}\n\n",
				"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
				"data: [DONE]\n\n",
				"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":30,\"completion_tokens\":17}}\n\n",
				"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0}}\n\n",
			}}
			body.beforeRead = func(read int) {
				assert.NotContains(t, rec.Body.String(), "event: message_stop")
				assert.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
				if read > 0 {
					assert.True(t, rec.Flushed)
				}
			}
			require.NoError(t, handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body}))
			assert.True(t, body.closed)
			assert.Equal(t, 6, body.read)
			assert.Equal(t, 1, firstByteCalls)
			assert.Positive(t, gctx.TTFT)
			assert.Equal(t, 30, gctx.InputTokens)
			assert.Equal(t, 17, gctx.OutputTokens)
			assert.Equal(t, 5, gctx.TransmittedChars)
			assert.Contains(t, rec.Body.String(), `"usage":{"input_tokens":20,"output_tokens":7}`)
			assert.Contains(t, rec.Body.String(), `"usage":{"output_tokens":17}`)
			wantModel := originalModel
			if wantModel == "" {
				wantModel = "upstream-model"
			}
			assert.Contains(t, rec.Body.String(), `"model":"`+wantModel+`"`)
			assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
			assert.Equal(t, "5", gctx.Tags["text_chars"])
			assert.Equal(t, "5", gctx.Tags["transmitted_chars"])
			assert.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
			assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
			assert.Equal(t, "keep-alive", rec.Header().Get("Connection"))
			assert.Equal(t, "no", rec.Header().Get("X-Accel-Buffering"))
			assert.Equal(t, "2023-06-01", rec.Header().Get("anthropic-version"))
		})
	}
}

func TestHandleMessagesStream_SplitFramingAndUnicodeAccounting(t *testing.T) {
	body := &messagesChunkBody{chunks: []string{"event: ignored\r", "\ndata: {\"choices\":[{\"delta\":{\"reasoning_content\":\"思考\",\"content\":\"你🙂\"},\"finish_reason\":\"stop\"}]}\r", "\n\r\n", "data:  [DONE] \r\n\r\n"}}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{ResponseWriter: rec, Model: "model"}
	require.NoError(t, handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body}))
	assert.True(t, body.closed)
	assert.Contains(t, rec.Body.String(), "你🙂")
	assert.Equal(t, 13, gctx.TransmittedChars)
	assert.Equal(t, "7", gctx.Tags["text_chars"])
	assert.Equal(t, "6", gctx.Tags["thinking_chars"])
	assert.Equal(t, "true", gctx.Tags["stream_saw_done"])
}

func TestHandleMessagesStream_ToolArgumentsDoNotCountAsTransmittedChars(t *testing.T) {
	body := &messagesTrackedBody{Reader: strings.NewReader("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"你好\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")}
	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{ResponseWriter: rec, Model: "model"}
	require.NoError(t, handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body}))
	assert.True(t, body.closed)
	assert.Contains(t, rec.Body.String(), "input_json_delta")
	assert.Zero(t, gctx.TransmittedChars)
	assert.Equal(t, "0", gctx.Tags["transmitted_chars"])
	assert.True(t, rec.Flushed)
	assert.Equal(t, "true", gctx.Tags["message_stop_sent"])
}

func TestHandleMessagesStream_EmptyAndIncompleteFramingRemainErrors(t *testing.T) {
	for _, input := range []string{"", "data: {\"choices\":[{\"delta\":{\"content\":\"unframed\"}}]}", "data: not-json\n\n"} {
		t.Run(input, func(t *testing.T) {
			body := &messagesTrackedBody{Reader: strings.NewReader(input)}
			rec := httptest.NewRecorder()
			gctx := &core.GatewayContext{ResponseWriter: rec}
			err := handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body})
			require.EqualError(t, err, "empty upstream stream: no content or completion signal received")
			assert.True(t, body.closed)
			assert.Empty(t, rec.Body.String())
			assert.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
		})
	}
}
