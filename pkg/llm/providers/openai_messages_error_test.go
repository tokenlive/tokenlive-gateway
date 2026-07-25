package providers

import (
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

	upstreamResp := &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_parameter","message":"Model glm-5.1 does not support parameter top_k"}}`)),
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
}
