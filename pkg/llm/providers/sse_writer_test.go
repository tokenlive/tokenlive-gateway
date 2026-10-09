package providers

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSEWriter_PreservesFraming(t *testing.T) {
	var event bytes.Buffer
	require.NoError(t, writeSSEEvent(&event, "response.output_text.delta", []byte(`{"delta":"你好"}`)))
	require.Equal(t, "event: response.output_text.delta\ndata: {\"delta\":\"你好\"}\n\n", event.String())
	var data bytes.Buffer
	require.NoError(t, writeSSEData(&data, []byte("[DONE]")))
	require.Equal(t, "data: [DONE]\n\n", data.String())
}

func TestSSEWriter_LeavesFlushToCallerAndReturnsErrors(t *testing.T) {
	recorder := httptest.NewRecorder()
	require.NoError(t, writeSSEEvent(recorder, "message_stop", []byte(`{"type":"message_stop"}`)))
	require.False(t, recorder.Flushed)
	writeErr := errors.New("downstream write failed")
	writer := &messagesFailWriter{ResponseRecorder: httptest.NewRecorder(), failEvent: "message_stop", err: writeErr}
	require.ErrorIs(t, writeSSEEvent(writer, "message_stop", []byte(`{"type":"message_stop"}`)), writeErr)
	writer = &messagesFailWriter{ResponseRecorder: httptest.NewRecorder(), failData: "[DONE]", err: writeErr}
	require.ErrorIs(t, writeSSEData(writer, []byte("[DONE]")), writeErr)
	require.Equal(t, http.StatusOK, writer.Code)
	require.False(t, writer.Flushed)
}
