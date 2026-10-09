package providers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestHandleMessagesStreamUpstreamNon200ReturnsError(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("anthropic-version", "2023-06-01")

	gctx := &core.GatewayContext{
		ResponseWriter: rec,
		Request:        req,
		IsStream:       true,
		Model:          "glm-5.1",
	}

	body := &messagesTrackedBody{Reader: strings.NewReader(`{"error":{"code":"invalid_parameter","message":"Model glm-5.1 does not support parameter top_k"}}`)}
	upstreamResp := &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     make(http.Header),
		Body:       body,
	}

	err := handleMessagesStream(gctx, upstreamResp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "upstream returned status 400")

	resp := rec.Result()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))
	require.Equal(t, "2023-06-01", resp.Header.Get("anthropic-version"))

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)
	require.Contains(t, bodyStr, `"type":"error"`)
	require.Contains(t, bodyStr, "Model glm-5.1 does not support parameter top_k")
	require.True(t, body.closed)
	require.Contains(t, string(gctx.UpstreamBody), "invalid_parameter")
	require.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
}

type messagesErrorBody struct {
	data   string
	err    error
	closed bool
}

func (b *messagesErrorBody) Read(p []byte) (int, error) {
	if b.data != "" {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, b.err
	}
	return 0, b.err
}

func (b *messagesErrorBody) Close() error {
	b.closed = true
	return nil
}

func TestHandleMessagesStream_ReadErrorsDoNotGenerateTerminal(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		err  error
	}{
		{"before_content", "", io.ErrUnexpectedEOF},
		{"partial_with_read_error", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", io.ErrUnexpectedEOF},
		{"completion_with_read_error", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", io.ErrUnexpectedEOF},
		{"canceled", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &messagesErrorBody{data: tc.data, err: tc.err}
			rec := httptest.NewRecorder()
			gctx := &core.GatewayContext{ResponseWriter: rec, Model: "model"}
			err := handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body})
			require.ErrorIs(t, err, tc.err)
			require.Contains(t, err.Error(), "read upstream stream:")
			require.True(t, body.closed)
			require.NotContains(t, rec.Body.String(), "event: content_block_stop")
			require.NotContains(t, rec.Body.String(), "event: message_stop")
			require.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
			if tc.data != "" {
				require.Contains(t, rec.Body.String(), "partial")
				require.Equal(t, 7, gctx.TransmittedChars)
			}
		})
	}
}

type messagesFailWriter struct {
	*httptest.ResponseRecorder
	failEvent string
	failData  string
	err       error
}

func (w *messagesFailWriter) Write(data []byte) (int, error) {
	if strings.Contains(string(data), "event: "+w.failEvent+"\n") || (w.failData != "" && strings.Contains(string(data), w.failData)) {
		return 0, w.err
	}
	return w.ResponseRecorder.Write(data)
}

func TestHandleMessagesStream_WriteErrorsDoNotMarkCompleted(t *testing.T) {
	for _, event := range []string{"message_start", "content_block_delta", "message_delta", "message_stop"} {
		t.Run(event, func(t *testing.T) {
			writeErr := errors.New("downstream write failed")
			writer := &messagesFailWriter{ResponseRecorder: httptest.NewRecorder(), failEvent: event, err: writeErr}
			body := &messagesTrackedBody{Reader: strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")}
			gctx := &core.GatewayContext{ResponseWriter: writer, Model: "model"}
			err := handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body})
			require.ErrorIs(t, err, writeErr)
			require.True(t, body.closed)
			require.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
			if event == "message_start" || event == "content_block_delta" {
				require.Zero(t, gctx.TransmittedChars)
			} else {
				require.Equal(t, 5, gctx.TransmittedChars)
			}
		})
	}
}

func TestHandleMessagesStream_WriteErrorPreservesEarlierDeltaCount(t *testing.T) {
	writeErr := errors.New("downstream write failed")
	writer := &messagesFailWriter{ResponseRecorder: httptest.NewRecorder(), failData: `"type":"text"`, err: writeErr}
	// Fail the text block start after the thinking delta was successfully written.
	body := &messagesTrackedBody{Reader: strings.NewReader("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"思考\",\"content\":\"answer\"}}]}\n\n")}
	gctx := &core.GatewayContext{ResponseWriter: writer, Model: "model"}
	require.ErrorIs(t, handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body}), writeErr)
	require.True(t, body.closed)
	require.Equal(t, 6, gctx.TransmittedChars)
	require.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
}

func TestHandleMessagesStream_PartialBlockCloseWriteErrorRemainsPrematureEOF(t *testing.T) {
	writer := &messagesFailWriter{ResponseRecorder: httptest.NewRecorder(), failEvent: "content_block_stop", err: errors.New("close write failed")}
	body := &messagesTrackedBody{Reader: strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")}
	gctx := &core.GatewayContext{ResponseWriter: writer}
	require.EqualError(t, handleMessagesStream(gctx, &http.Response{StatusCode: http.StatusOK, Body: body}), "upstream stream closed prematurely without completion event")
	require.True(t, body.closed)
	require.True(t, writer.Flushed)
	require.NotEqual(t, "true", gctx.Tags["message_stop_sent"])
}
