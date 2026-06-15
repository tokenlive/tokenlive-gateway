package providers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/stretchr/testify/assert"
)

func TestJoyCodeProvider_Signature(t *testing.T) {
	assert := assert.New(t)

	secretKey := "0691a3f0b37b4a85aeb63ad0fc7db3ed"
	signString := "joycode_ide&chat_completions&1685800000000"

	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(signString))
	expectedSign := hex.EncodeToString(mac.Sum(nil))

	assert.NotEmpty(expectedSign)
	// 验证对于给定的 Key 与 Data 计算出的签名
	// 预期签名应该是固定的 HMAC-SHA256 十六进制值
	assert.Equal("d28d6d6c241fdf6e4e467fd861ca5b51f00a3b5276d31da1af6e02b8d7612ee5", expectedSign)
}

func TestJoyCodeProvider_InvokeAndHeaders(t *testing.T) {
	assert := assert.New(t)

	var capturedReq *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedReq = r
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"hello"}}],"usage":{"prompt_tokens":10,"completion_tokens":20}}`))
	}))
	defer server.Close()

	provider := NewJoyCodeProvider("test-joycode", server.URL, "0691a3f0b37b4a85aeb63ad0fc7db3ed", []string{"GLM-5.1"})

	// 构造 GatewayContext，包含带有特定 Headers 的 Request
	clientReq, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"GLM-5.1"}`))
	clientReq.Header.Set("ptKey", "test-pt-key")
	clientReq.Header.Set("loginType", "test-login-type")
	clientReq.Header.Set("tenant", "test-tenant")

	rec := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            context.Background(),
		Request:        clientReq,
		ResponseWriter: rec,
		RawBody:        []byte(`{"model":"GLM-5.1"}`),
		RequestType:    core.RequestTypeChatCompletion,
		IsStream:       false,
	}

	err := provider.Invoke(gctx)
	assert.Nil(err)

	assert.NotNil(capturedReq)
	// 验证代理请求的 Header 是否透传成功
	assert.Equal("test-pt-key", capturedReq.Header.Get("ptKey"))
	assert.Equal("test-login-type", capturedReq.Header.Get("loginType"))
	assert.Equal("test-tenant", capturedReq.Header.Get("tenant"))
	assert.Equal("api-ai.jd.com", capturedReq.Host)

	// 验证 x-ms-client-request-id 追踪头
	clientReqID := capturedReq.Header.Get("x-ms-client-request-id")
	assert.NotEmpty(clientReqID)
	assert.True(strings.HasPrefix(clientReqID, "task-"))
	assert.Contains(clientReqID, "_session-")

	// 验证签名参数被拼接到 URL 查询参数中
	query := capturedReq.URL.Query()
	assert.Equal("joycode_ide", query.Get("appid"))
	assert.Equal("chat_completions", query.Get("functionId"))
	assert.NotEmpty(query.Get("t"))
	assert.NotEmpty(query.Get("sign"))
}

func TestJoyCodeProvider_DefaultAPIKey(t *testing.T) {
	assert := assert.New(t)
	provider := NewJoyCodeProvider("test-default-joycode", "http://localhost", "", []string{"GLM-5.1"})
	assert.Equal("0691a3f0b37b4a85aeb63ad0fc7db3ed", provider.apiKey)
}

func TestJoyCodeResponses_Translation_NonStream(t *testing.T) {
	assert := assert.New(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("/v1", r.URL.Path)
		query := r.URL.Query()
		assert.Equal("joycode_ide", query.Get("appid"))
		assert.Equal("chat_completions", query.Get("functionId"))
		assert.NotEmpty(query.Get("t"))
		assert.NotEmpty(query.Get("sign"))

		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to unmarshal request: %v", err)
		}

		messages, ok := req["messages"].([]interface{})
		if !ok || len(messages) != 2 {
			t.Fatalf("expected messages length 2, got %v", req["messages"])
		}

		firstMsg, _ := messages[0].(map[string]interface{})
		assert.Equal("system", firstMsg["role"])
		assert.Equal("You are JoyCode.", firstMsg["content"])

		secondMsg, _ := messages[1].(map[string]interface{})
		assert.Equal("user", secondMsg["role"])
		assert.Equal("Who are you?", secondMsg["content"])

		assert.Nil(req["max_output_tokens"])
		assert.Equal(float64(100), req["max_completion_tokens"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"id": "chatcmpl-joycode123",
			"object": "chat.completion",
			"created": 1741476542,
			"model": "GPT-5.3-codex",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": "I am JoyCode."
					},
					"finish_reason": "stop"
				}
			],
			"usage": {
				"prompt_tokens": 15,
				"completion_tokens": 10,
				"total_tokens": 25
			}
		}`))
	}))
	defer server.Close()

	p := NewJoyCodeProvider("test-joycode-responses", server.URL+"/v1", "0691a3f0b37b4a85aeb63ad0fc7db3ed", []string{"GPT-5.3-codex"})

	reqBody := `{
		"model": "GPT-5.3-codex",
		"instructions": "You are JoyCode.",
		"input": "Who are you?",
		"max_output_tokens": 100
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "GPT-5.3-codex"
	gctx.IsStream = false

	err := p.Invoke(gctx)
	assert.Nil(err)

	var resp map[string]interface{}
	err = json.Unmarshal(gctx.UpstreamBody, &resp)
	assert.Nil(err)

	assert.Equal("resp_joycode123", resp["id"])
	assert.Equal("response", resp["object"])
	assert.Equal("completed", resp["status"])

	outputList, ok := resp["output"].([]interface{})
	assert.True(ok)
	assert.Len(outputList, 1)

	firstOutput, _ := outputList[0].(map[string]interface{})
	assert.Equal("msg_joycode123", firstOutput["id"])
	assert.Equal("message", firstOutput["type"])

	contentList, _ := firstOutput["content"].([]interface{})
	assert.Len(contentList, 1)
	contentItem, _ := contentList[0].(map[string]interface{})
	assert.Equal("output_text", contentItem["type"])
	assert.Equal("I am JoyCode.", contentItem["text"])

	usage, _ := resp["usage"].(map[string]interface{})
	assert.Equal(float64(15), usage["input_tokens"])
	assert.Equal(float64(10), usage["output_tokens"])
}

func TestJoyCodeResponses_Translation_Stream(t *testing.T) {
	assert := assert.New(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		chunks := []string{
			`data: {"id":"chatcmpl-joystream123","object":"chat.completion.chunk","created":1741290958,"model":"GPT-5.3-codex","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}], "usage":{"prompt_tokens":37,"completion_tokens":0}}`,
			`data: {"id":"chatcmpl-joystream123","object":"chat.completion.chunk","created":1741290958,"model":"GPT-5.3-codex","choices":[{"index":0,"delta":{"content":" codex!"},"finish_reason":null}], "usage":{"prompt_tokens":37,"completion_tokens":2}}`,
			`data: {"id":"chatcmpl-joystream123","object":"chat.completion.chunk","created":1741290958,"model":"GPT-5.3-codex","choices":[{"index":0,"delta":{},"finish_reason":"stop"}], "usage":{"prompt_tokens":37,"completion_tokens":5}}`,
			`data: [DONE]`,
		}

		for _, chunk := range chunks {
			_, _ = w.Write([]byte(chunk + "\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}))
	defer server.Close()

	p := NewJoyCodeProvider("test-joycode-responses-stream", server.URL, "0691a3f0b37b4a85aeb63ad0fc7db3ed", []string{"GPT-5.3-codex"})

	reqBody := `{
		"model": "GPT-5.3-codex",
		"input": "Hello",
		"stream": true
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "GPT-5.3-codex"
	gctx.IsStream = true

	err := p.Invoke(gctx)
	assert.Nil(err)

	respBody := w.Body.String()

	expectedEvents := []string{
		`event: response.created`,
		`"id":"resp_joystream123"`,
		`event: response.in_progress`,
		`event: response.output_item.added`,
		`"id":"msg_joystream123"`,
		`event: response.content_part.added`,
		`event: response.output_text.delta`,
		`"delta":"Hi codex!"`,
		`event: response.output_text.done`,
		`"text":"Hi codex!"`,
		`event: response.content_part.done`,
		`event: response.output_item.done`,
		`event: response.done`,
		`event: response.completed`,
		`"input_tokens":37`,
		`"output_tokens":5`,
		`data: [DONE]`,
	}

	for _, expected := range expectedEvents {
		assert.Contains(respBody, expected)
	}

	assert.Equal(37, gctx.InputTokens)
	assert.Equal(5, gctx.OutputTokens)
}

func TestJoyCodeResponses_Native(t *testing.T) {
	assert := assert.New(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("/v1", r.URL.Path)
		query := r.URL.Query()
		assert.Equal("joycode_ide", query.Get("appid"))
		assert.Equal("responses_completions", query.Get("functionId"))
		assert.NotEmpty(query.Get("t"))
		assert.NotEmpty(query.Get("sign"))

		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		err := json.Unmarshal(body, &req)
		assert.Nil(err)
		assert.Equal("GPT-5.3-codex", req["model"])
		assert.Equal("You are JoyCode.", req["instructions"])
		assert.Equal("Who are you?", req["input"])
		assert.Equal(float64(100), req["max_output_tokens"])
		assert.Nil(req["messages"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id": "resp_native123", "object": "response", "status": "completed", "output": []}`))
	}))
	defer server.Close()

	ep := &core.Endpoint{
		ID:           "ep-joycode-native",
		Provider:     "joycode",
		Model:        "GPT-5.3-codex",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion, core.RequestTypeResponses},
	}

	p := NewJoyCodeProvider("test-joycode-native", server.URL+"/v1", "0691a3f0b37b4a85aeb63ad0fc7db3ed", []string{"GPT-5.3-codex"})

	reqBody := `{
		"model": "GPT-5.3-codex",
		"instructions": "You are JoyCode.",
		"input": "Who are you?",
		"max_output_tokens": 100
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "GPT-5.3-codex"
	gctx.IsStream = false
	gctx.SelectedEndpoint = ep

	err := p.Invoke(gctx)
	assert.Nil(err)

	var resp map[string]interface{}
	err = json.Unmarshal(gctx.UpstreamBody, &resp)
	assert.Nil(err)
	assert.Equal("resp_native123", resp["id"])
	assert.Equal("response", resp["object"])
}

func TestJoyCodeSanitizedReader(t *testing.T) {
	assert := assert.New(t)

	rawInput := "data: event: response.completed\n\ndata: data: {\"type\":\"response.completed\"}\n\ndata: обычная строка\n"
	expectedOutput := "event: response.completed\n\ndata: {\"type\":\"response.completed\"}\n\ndata: обычная строка\n"

	reader := newJoyCodeSanitizedReader(io.NopCloser(strings.NewReader(rawInput)))
	defer reader.Close()

	buf, err := io.ReadAll(reader)
	assert.Nil(err)
	assert.Equal(expectedOutput, string(buf))
}
