package providers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestOpenAIMessages_NonStream(t *testing.T) {
	// 模拟上游 OpenAI 服务
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected auth header: %s", r.Header.Get("Authorization"))
		}

		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to unmarshal request: %v", err)
		}

		// 验证参数翻译
		messages, ok := req["messages"].([]interface{})
		if !ok {
			t.Fatal("messages field is not a slice")
		}
		if len(messages) != 2 {
			t.Fatalf("expected messages length 2, got %d", len(messages))
		}

		firstMsg, ok := messages[0].(map[string]interface{})
		if !ok {
			t.Fatal("first message is not a map")
		}
		if firstMsg["role"] != "system" || firstMsg["content"] != "You are a helpful assistant" {
			t.Errorf("expected system message, got %v", firstMsg)
		}

		secondMsg, ok := messages[1].(map[string]interface{})
		if !ok {
			t.Fatal("second message is not a map")
		}
		if secondMsg["role"] != "user" || secondMsg["content"] != "hi" {
			t.Errorf("expected user message, got %v", secondMsg)
		}

		// 验证 max_tokens 映射为 max_completion_tokens
		if _, ok := req["max_tokens"]; ok {
			t.Error("max_tokens should be deleted")
		}
		maxCompTokens, ok := req["max_completion_tokens"].(float64)
		if !ok || maxCompTokens != 100 {
			t.Errorf("expected max_completion_tokens=100, got %v", req["max_completion_tokens"])
		}

		// 验证优雅退化：非核心扩展字段透传
		temp, ok := req["temperature"].(float64)
		if !ok || temp != 0.7 {
			t.Errorf("expected temperature=0.7 to be preserved, got %v", req["temperature"])
		}

		// 验证 thinking 映射为 auto
		thinking, ok := req["thinking"].(map[string]interface{})
		if !ok {
			t.Fatal("thinking field is not a map")
		}
		if thinking["type"] != "auto" {
			t.Errorf("expected thinking type=auto, got %v", thinking["type"])
		}
		if thinking["budget_tokens"] != float64(1024) {
			t.Errorf("expected thinking budget_tokens=1024, got %v", thinking["budget_tokens"])
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "chatcmpl-123",
			"object": "chat.completion",
			"created": 1677652288,
			"model": "gpt-4",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": "hello world"
					},
					"finish_reason": "stop"
				}
			],
			"usage": {
				"prompt_tokens": 10,
				"completion_tokens": 5,
				"total_tokens": 15
			}
		}`))
	}))
	defer server.Close()

	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})

	// 模拟 Anthropic 请求 body
	reqBody := `{
		"model": "gpt-4",
		"system": "You are a helpful assistant",
		"messages": [{"role": "user", "content": "hi"}],
		"max_tokens": 100,
		"temperature": 0.7,
		"thinking": {
			"type": "adaptive",
			"budget_tokens": 1024
		}
	}`

	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-4"

	// 使用 openaiMessagesInvoker
	invoker := &openaiMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证响应翻译
	if gctx.InputTokens != 10 {
		t.Errorf("expected PromptTokens=10, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 5 {
		t.Errorf("expected CompletionTokens=5, got %d", gctx.OutputTokens)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal translated response: %v", err)
	}

	if resp["id"] != "msg_123" {
		t.Errorf("expected id=msg_123, got %v", resp["id"])
	}
	if resp["type"] != "message" {
		t.Errorf("expected type=message, got %v", resp["type"])
	}
	if resp["role"] != "assistant" {
		t.Errorf("expected role=assistant, got %v", resp["role"])
	}
	if resp["model"] != "gpt-4" {
		t.Errorf("expected model=gpt-4, got %v", resp["model"])
	}

	content, ok := resp["content"].([]interface{})
	if !ok || len(content) != 1 {
		t.Fatalf("expected content slice of length 1, got %v", resp["content"])
	}
	firstContent, ok := content[0].(map[string]interface{})
	if !ok {
		t.Fatal("content item is not a map")
	}
	if firstContent["type"] != "text" || firstContent["text"] != "hello world" {
		t.Errorf("expected content block of text type with hello world, got %v", firstContent)
	}

	usage, ok := resp["usage"].(map[string]interface{})
	if !ok {
		t.Fatal("usage field is missing or not a map")
	}
	if usage["input_tokens"] != float64(10) || usage["output_tokens"] != float64(5) {
		t.Errorf("expected input_tokens=10 and output_tokens=5, got %v", usage)
	}
}

func TestOpenAIMessages_Stream(t *testing.T) {
	// 模拟上游 OpenAI 服务
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		chunks := []string{
			`data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"},"finish_reason":null}], "usage":{"prompt_tokens":10,"completion_tokens":0}}`,
			`data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":null}], "usage":{"prompt_tokens":10,"completion_tokens":2}}`,
			`data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}], "usage":{"prompt_tokens":10,"completion_tokens":5}}`,
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

	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})

	// 模拟 Anthropic 请求 body
	reqBody := `{
		"model": "gpt-4",
		"system": "You are a helpful assistant",
		"messages": [{"role": "user", "content": "hi"}],
		"max_tokens": 100,
		"temperature": 0.7,
		"stream": true
	}`

	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-4"
	gctx.IsStream = true

	// 使用 openaiMessagesInvoker
	invoker := &openaiMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证响应体
	respBody := w.Body.String()

	// 期望的 Anthropic 事件
	expectedEvents := []string{
		`event: message_start`,
		`"type":"message_start"`,
		`"id":"msg_123"`,
		`event: content_block_start`,
		`"type":"content_block_start"`,
		`event: content_block_delta`,
		`"text":"hello"`,
		`"text":" world"`,
		`event: content_block_stop`,
		`event: message_stop`,
	}

	for _, expected := range expectedEvents {
		if !strings.Contains(respBody, expected) {
			t.Errorf("expected response to contain %q, but got:\n%s", expected, respBody)
		}
	}

	// 验证 token 统计被更新
	if gctx.InputTokens != 10 {
		t.Errorf("expected PromptTokens=10, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 5 {
		t.Errorf("expected CompletionTokens=5, got %d", gctx.OutputTokens)
	}

	// 验证 TransmittedChars 应该等于 "hello world" 的长度 11
	if gctx.TransmittedChars != 11 {
		t.Errorf("expected TransmittedChars=11, got %d", gctx.TransmittedChars)
	}
}

func TestOpenAIMessages_NonStream_Tools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		_ = json.Unmarshal(body, &req)

		// 验证 tools 翻译
		tools, ok := req["tools"].([]interface{})
		if !ok || len(tools) != 1 {
			t.Fatalf("expected 1 tool, got %v", req["tools"])
		}
		tool := tools[0].(map[string]interface{})
		if tool["type"] != "function" {
			t.Errorf("expected tool type=function, got %v", tool["type"])
		}
		function := tool["function"].(map[string]interface{})
		if function["name"] != "get_weather" || function["description"] != "Get weather" {
			t.Errorf("unexpected function data: %v", function)
		}
		params := function["parameters"].(map[string]interface{})
		if params["type"] != "object" {
			t.Errorf("unexpected parameters: %v", params)
		}

		// 验证 tool_choice 翻译
		toolChoice, ok := req["tool_choice"].(map[string]interface{})
		if !ok || toolChoice["type"] != "function" {
			t.Fatalf("expected tool_choice type=function, got %v", req["tool_choice"])
		}
		choiceFunc := toolChoice["function"].(map[string]interface{})
		if choiceFunc["name"] != "get_weather" {
			t.Errorf("expected choice name=get_weather, got %v", choiceFunc["name"])
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "chatcmpl-tool",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": null,
					"tool_calls": [{
						"id": "call_123",
						"type": "function",
						"function": {
							"name": "get_weather",
							"arguments": "{\"location\":\"San Francisco\"}"
						}
					}]
				},
				"finish_reason": "tool_calls"
			}],
			"usage": {"prompt_tokens": 20, "completion_tokens": 15}
		}`))
	}))
	defer server.Close()

	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})

	reqBody := `{
		"model": "gpt-4",
		"messages": [{"role": "user", "content": "weather?"}],
		"max_tokens": 100,
		"tools": [
			{
				"name": "get_weather",
				"description": "Get weather",
				"input_schema": {
					"type": "object",
					"properties": {
						"location": {"type": "string"}
					},
					"required": ["location"]
				}
			}
		],
		"tool_choice": {
			"type": "tool",
			"name": "get_weather"
		}
	}`

	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-4"

	invoker := &openaiMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["stop_reason"] != "tool_use" {
		t.Errorf("expected stop_reason=tool_use, got %v", resp["stop_reason"])
	}

	content, ok := resp["content"].([]interface{})
	if !ok || len(content) != 1 {
		t.Fatalf("expected 1 content block, got %v", resp["content"])
	}
	item := content[0].(map[string]interface{})
	if item["type"] != "tool_use" || item["id"] != "toolu_123" || item["name"] != "get_weather" {
		t.Errorf("unexpected content block: %v", item)
	}
	input := item["input"].(map[string]interface{})
	if input["location"] != "San Francisco" {
		t.Errorf("expected input.location=San Francisco, got %v", input["location"])
	}
}

func TestOpenAIMessages_Stream_Tools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		chunks := []string{
			`data: {"id":"chatcmpl-tool","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_123","type":"function","function":{"name":"get_weather"}}]}}]}`,
			`data: {"id":"chatcmpl-tool","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"loca"}}]}}]}`,
			`data: {"id":"chatcmpl-tool","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"tion\":\"SF\"}"}}]}}]}`,
			`data: {"id":"chatcmpl-tool","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}], "usage":{"prompt_tokens":20,"completion_tokens":15}}`,
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

	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})

	reqBody := `{"model": "gpt-4", "messages": [{"role": "user", "content": "weather?"}], "stream": true}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-4"
	gctx.IsStream = true

	invoker := &openaiMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	respBody := w.Body.String()

	expectedEvents := []string{
		`event: message_start`,
		`"type":"message_start"`,
		`event: content_block_start`,
		`"type":"tool_use"`,
		`"id":"toolu_123"`,
		`"name":"get_weather"`,
		`event: content_block_delta`,
		`"type":"input_json_delta"`,
		`"partial_json":"{\"loca"`,
		`"partial_json":"tion\":\"SF\"}"`,
		`event: content_block_stop`,
		`event: message_stop`,
	}

	for _, expected := range expectedEvents {
		if !strings.Contains(respBody, expected) {
			t.Errorf("expected response to contain %q, but got:\n%s", expected, respBody)
		}
	}
}

func TestOpenAIMessages_NonStream_Empty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "chatcmpl-empty",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": ""
				},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 10, "completion_tokens": 0}
		}`))
	}))
	defer server.Close()

	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})

	reqBody := `{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}]}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-4"

	invoker := &openaiMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var resp map[string]interface{}
	_ = json.Unmarshal(gctx.UpstreamBody, &resp)

	content, ok := resp["content"].([]interface{})
	if !ok || len(content) != 1 {
		t.Fatalf("expected content of length 1, got %v", resp["content"])
	}
	item := content[0].(map[string]interface{})
	if item["type"] != "text" || item["text"] != "" {
		t.Errorf("expected text content block with empty text, got %v", item)
	}
}

func TestOpenAIMessages_Stream_Empty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data: {\"choices\": [{\"delta\": {}}]}\n\n"))
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})

	reqBody := `{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": true}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-4"
	gctx.IsStream = true

	invoker := &openaiMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err == nil {
		t.Fatal("expected empty stream error, got nil")
	}
	if !strings.Contains(err.Error(), "empty upstream stream") {
		t.Fatalf("expected empty upstream stream error, got %v", err)
	}
	if body := w.Body.String(); body != "" {
		t.Fatalf("expected no downstream events for empty stream, got %q", body)
	}
}

func TestOpenAIMessages_Stream_DoneOnlyReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-4"})

	reqBody := `{"model": "gpt-4", "messages": [{"role": "user", "content": "hi"}], "stream": true}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-4"
	gctx.IsStream = true

	invoker := &openaiMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err == nil {
		t.Fatal("expected empty stream error, got nil")
	}
	if !strings.Contains(err.Error(), "empty upstream stream") {
		t.Fatalf("expected empty upstream stream error, got %v", err)
	}
	if body := w.Body.String(); body != "" {
		t.Fatalf("expected no downstream events for done-only stream, got %q", body)
	}
}
