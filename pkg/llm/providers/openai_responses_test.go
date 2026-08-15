package providers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func TestOpenAIResponses_Translation_NonStream(t *testing.T) {
	// 1. 模拟上游只提供 /chat/completions
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to unmarshal request: %v", err)
		}

		// 验证参数翻译
		messages, ok := req["messages"].([]interface{})
		if !ok || len(messages) != 2 {
			t.Fatalf("expected messages length 2, got %v", req["messages"])
		}

		firstMsg, _ := messages[0].(map[string]interface{})
		if firstMsg["role"] != "system" || firstMsg["content"] != "You are a helpful assistant." {
			t.Errorf("expected system message, got %v", firstMsg)
		}

		secondMsg, _ := messages[1].(map[string]interface{})
		if secondMsg["role"] != "user" || secondMsg["content"] != "Hello unicorn!" {
			t.Errorf("expected user message, got %v", secondMsg)
		}

		// max_output_tokens 应映射为 max_completion_tokens
		if _, ok := req["max_output_tokens"]; ok {
			t.Error("max_output_tokens should be deleted")
		}
		maxComp, ok := req["max_completion_tokens"].(float64)
		if !ok || maxComp != 200 {
			t.Errorf("expected max_completion_tokens=200, got %v", req["max_completion_tokens"])
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "chatcmpl-response123",
			"object": "chat.completion",
			"created": 1741476542,
			"model": "gpt-5.4",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": "This is a unicorn story."
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

	// 2. 该 Endpoint 不支持 responses 能力（只有 chat_completion 从而触发翻译降级）
	ep := &core.Endpoint{
		ID:           "ep-translation",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{
		"model": "gpt-5.4",
		"instructions": "You are a helpful assistant.",
		"input": "Hello unicorn!",
		"max_output_tokens": 200
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 3. 验证返回数据结构是否为符合要求的 responses 格式
	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal responses result: %v", err)
	}

	if resp["id"] != "resp_response123" {
		t.Errorf("expected id=resp_response123, got %v", resp["id"])
	}
	if resp["object"] != "response" {
		t.Errorf("expected object=response, got %v", resp["object"])
	}
	if resp["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", resp["status"])
	}

	outputList, ok := resp["output"].([]interface{})
	if !ok || len(outputList) != 1 {
		t.Fatalf("expected output list size 1, got %v", resp["output"])
	}

	firstOutput, _ := outputList[0].(map[string]interface{})
	if firstOutput["id"] != "msg_response123" {
		t.Errorf("expected output id=msg_response123, got %v", firstOutput["id"])
	}
	if firstOutput["type"] != "message" {
		t.Errorf("expected output type=message, got %v", firstOutput["type"])
	}

	contentList, _ := firstOutput["content"].([]interface{})
	if len(contentList) != 1 {
		t.Fatalf("expected output content size 1, got %v", firstOutput["content"])
	}
	contentItem, _ := contentList[0].(map[string]interface{})
	if contentItem["type"] != "output_text" || contentItem["text"] != "This is a unicorn story." {
		t.Errorf("unexpected content block: %v", contentItem)
	}

	usage, _ := resp["usage"].(map[string]interface{})
	if usage["input_tokens"].(float64) != 15 || usage["output_tokens"].(float64) != 10 {
		t.Errorf("unexpected usage metrics: %v", usage)
	}
}

func TestOpenAIResponses_Translation_Stream(t *testing.T) {
	// 1. 模拟上游流式返回
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		chunks := []string{
			`data: {"id":"chatcmpl-stream123","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}], "usage":{"prompt_tokens":37,"completion_tokens":0}}`,
			`data: {"id":"chatcmpl-stream123","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"content":" there!"},"finish_reason":null}], "usage":{"prompt_tokens":37,"completion_tokens":2}}`,
			`data: {"id":"chatcmpl-stream123","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}], "usage":{"prompt_tokens":37,"completion_tokens":5}}`,
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

	// 2. 模拟不支持 responses 能力的端点
	ep := &core.Endpoint{
		ID:           "ep-translation-stream",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{
		"model": "gpt-5.4",
		"instructions": "You are a helpful assistant.",
		"input": "Hello!",
		"stream": true
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.IsStream = true
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	respBody := w.Body.String()

	// 3. 验证 8 大核心事件序列
	expectedEvents := []string{
		`event: response.created`,
		`"type":"response.created"`,
		`"id":"resp_stream123"`,
		`event: response.in_progress`,
		`event: response.output_item.added`,
		`"id":"msg_stream123"`,
		`event: response.content_part.added`,
		`event: response.output_text.delta`,
		`"delta":"Hi"`,
		`"delta":" there!"`,
		`event: response.output_text.done`,
		`"text":"Hi there!"`,
		`event: response.content_part.done`,
		`event: response.output_item.done`,
		`event: response.done`,
		`"type":"response.done"`,
		`event: response.completed`,
		`"type":"response.completed"`,
		`"input_tokens":37`,
		`"output_tokens":5`,
		`data: [DONE]`,
	}

	for _, expected := range expectedEvents {
		if !strings.Contains(respBody, expected) {
			t.Errorf("expected response stream to contain %q, but got:\n%s", expected, respBody)
		}
	}

	// 验证计费信息是否被正确更新
	if gctx.InputTokens != 37 {
		t.Errorf("expected PromptTokens=37, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 5 {
		t.Errorf("expected CompletionTokens=5, got %d", gctx.OutputTokens)
	}
	if gctx.TransmittedChars != 9 {
		t.Errorf("expected TransmittedChars=9, got %d", gctx.TransmittedChars)
	}
}

func TestOpenAIResponses_Translation_Stream_ReasoningSeparation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		chunks := []string{
			`data: {"id":"chatcmpl-reason-stream","model":"gpt-5.4","choices":[{"index":0,"delta":{"reasoning_content":"thinking"},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-reason-stream","model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-reason-stream","model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
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

	ep := &core.Endpoint{
		ID:           "ep-reason-stream",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{"model":"gpt-5.4","input":"hello","stream":true}`
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)
	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.IsStream = true
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	if err := invoker.Invoke(gctx, p); err != nil {
		t.Fatal(err)
	}

	respBody := w.Body.String()
	expected := []string{
		`event: response.reasoning_summary_part.added`,
		`event: response.reasoning_summary_text.delta`,
		`"delta":"thinking"`,
		`event: response.reasoning_summary_text.done`,
		`"text":"thinking"`,
		`event: response.reasoning_summary_part.done`,
	}
	for _, substr := range expected {
		if !strings.Contains(respBody, substr) {
			t.Errorf("missing %q in stream:\n%s", substr, respBody)
		}
	}
	if !strings.Contains(respBody, `"text":"answer"`) {
		t.Errorf("message output should contain answer text only:\n%s", respBody)
	}
	if reasoningIndex := strings.Index(respBody, `"type":"reasoning"`); reasoningIndex < 0 || reasoningIndex > strings.Index(respBody, `"type":"message"`) {
		t.Errorf("reasoning item should precede message item:\n%s", respBody)
	}
	if strings.Contains(respBody, `event: response.output_text.delta`+"\n"+"data: {\"delta\":\"thinking\"") {
		t.Errorf("reasoning must not be emitted as output text:\n%s", respBody)
	}
}

func TestOpenAIResponses_Translation_Stream_NamespaceToolCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		chunks := []string{
			`data: {"id":"chatcmpl-ns-stream","model":"glm-5.3","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_ns","type":"function","function":{"name":"collaboration.spawn_agent","arguments":"{}"}}]},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-ns-stream","model":"glm-5.3","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
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

	ep := &core.Endpoint{RequestTypes: []core.RequestType{core.RequestTypeChatCompletion}}
	p := NewOpenAIProvider("test-openai", server.URL, "test-key", []string{"glm-5.3"})
	reqBody := `{"model":"glm-5.3","input":"spawn a task","stream":true}`
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)
	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "glm-5.3"
	gctx.IsStream = true
	gctx.SelectedEndpoint = ep

	if err := (&openaiResponsesInvoker{}).Invoke(gctx, p); err != nil {
		t.Fatal(err)
	}
	body := w.Body.String()
	for _, expected := range []string{
		`"name":"spawn_agent"`,
		`"namespace":"collaboration"`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("missing %q in stream:\n%s", expected, body)
		}
	}
}

func TestOpenAIResponses_Translation_WithNamespaceAndFiltering(t *testing.T) {
	// 1. 模拟上游只提供 /chat/completions
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to unmarshal request: %v", err)
		}

		// 验证 messages 是否被正确映射且保留元数据
		messages, ok := req["messages"].([]interface{})
		if !ok || len(messages) != 3 {
			t.Fatalf("expected messages length 3, got %v", req["messages"])
		}
		msg2, _ := messages[1].(map[string]interface{})
		if msg2["role"] != "assistant" {
			t.Errorf("expected msg2 role assistant, got %v", msg2["role"])
		}
		if msg2["tool_calls"] == nil {
			t.Error("expected msg2 to contain tool_calls")
		}
		msg3, _ := messages[2].(map[string]interface{})
		if msg3["role"] != "tool" {
			t.Errorf("expected msg3 role tool, got %v", msg3["role"])
		}
		if msg3["name"] != "js" {
			t.Errorf("expected msg3 name js, got %v", msg3["name"])
		}
		if msg3["tool_call_id"] != "call_1" {
			t.Errorf("expected msg3 tool_call_id call_1, got %v", msg3["tool_call_id"])
		}

		// 验证 tools 是否被正确处理
		tools, ok := req["tools"].([]interface{})
		if !ok || len(tools) != 3 {
			t.Fatalf("expected tools length 3, got %v", req["tools"])
		}

		// 收集解析后的工具名称，验证 namespace 工具会保留可逆限定名。
		toolNames := make(map[string]bool)
		for _, tVal := range tools {
			toolMap, ok := tVal.(map[string]interface{})
			if !ok {
				t.Fatalf("expected tool to be a map")
			}
			if toolMap["type"] != "function" {
				t.Errorf("expected tool type=function, got %v", toolMap["type"])
			}
			fnMap, ok := toolMap["function"].(map[string]interface{})
			if !ok {
				t.Fatalf("expected nested function field")
			}
			toolNames[fnMap["name"].(string)] = true
		}

		if !toolNames["mcp__node_repl.js"] {
			t.Error("expected tool 'mcp__node_repl.js' to be present")
		}
		if !toolNames["apply_patch"] {
			t.Error("expected tool 'apply_patch' to be present")
		}
		if !toolNames["view_image"] {
			t.Error("expected tool 'view_image' to be present")
		}
		if toolNames["tool_search"] {
			t.Error("tool_search should have been filtered out")
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "chatcmpl-responseToolsNew",
			"object": "chat.completion",
			"created": 1741476542,
			"model": "gpt-5.4",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": "Executed successfully with complex tools."
					},
					"finish_reason": "stop"
				}
			],
			"usage": {
				"prompt_tokens": 60,
				"completion_tokens": 15,
				"total_tokens": 75
			}
		}`))
	}))
	defer server.Close()

	ep := &core.Endpoint{
		ID:           "ep-translation-tools-complex",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-openai-tools-complex", server.URL, "test-key", []string{"gpt-5.4"})

	// 使用包含 namespace, tool_search, custom type(apply_patch) 和平铺 function 的复杂请求体
	reqBody := `{
		"model": "gpt-5.4",
		"input": [
			{
				"role": "user",
				"content": "Run some complex tasks"
			},
			{
				"role": "assistant",
				"content": "Running task...",
				"tool_calls": [
					{
						"id": "call_1",
						"type": "function",
						"function": {
							"name": "js",
							"arguments": "{\"code\":\"console.log(1)\"}"
						}
					}
				]
			},
			{
				"role": "tool",
				"name": "js",
				"tool_call_id": "call_1",
				"content": "1"
			}
		],
		"tools": [
			{
				"name": "mcp__node_repl",
				"type": "namespace",
				"tools": [
					{
						"type": "function",
						"name": "js",
						"description": "Run JavaScript in persistent Node kernel",
						"strict": false,
						"parameters": {
							"type": "object"
						}
					}
				]
			},
			{
				"type": "tool_search",
				"query": "find some tool"
			},
			{
				"name": "apply_patch",
				"type": "custom",
				"description": "Apply code patch",
				"parameters": {
					"type": "object"
				}
			},
			{
				"type": "function",
				"name": "view_image",
				"description": "View an image",
				"strict": false,
				"parameters": {
					"type": "object"
				}
			}
		]
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal responses result: %v", err)
	}

	if resp["id"] != "resp_responseToolsNew" {
		t.Errorf("expected id=resp_responseToolsNew, got %v", resp["id"])
	}
}

func TestOpenAIResponses_Native_WithNamespaceAndFiltering(t *testing.T) {
	// 1. 模拟原生支持 /responses 的上游
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var req map[string]interface{}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to unmarshal request: %v", err)
		}

		// 验证 input 是否被规范清洗标准化为标准的 OpenAI 协议格式
		inputList, ok := req["input"].([]interface{})
		if !ok || len(inputList) != 2 {
			t.Fatalf("expected input list size 2, got %v", req["input"])
		}
		item0, ok := inputList[0].(map[string]interface{})
		if !ok {
			t.Fatalf("expected item0 to be map")
		}
		if item0["role"] != "system" {
			t.Errorf("expected role=system, got %v", item0["role"])
		}
		if _, exists := item0["type"]; exists {
			t.Errorf("expected type field to be deleted from message item, but it exists")
		}
		content0, ok := item0["content"].([]interface{})
		if !ok || len(content0) != 1 {
			t.Fatalf("expected content0 list size 1, got %v", item0["content"])
		}
		c0, ok := content0[0].(map[string]interface{})
		if !ok {
			t.Fatalf("expected content block to be map")
		}
		if c0["type"] != "input_text" {
			t.Errorf("expected content type to be input_text, got %v", c0["type"])
		}

		// 验证 tools 是否被正确清洗并转发
		tools, ok := req["tools"].([]interface{})
		if !ok || len(tools) != 5 {
			t.Fatalf("expected tools length 5, got %v", req["tools"])
		}

		toolTypes := make(map[string]string)
		toolNames := make(map[string]bool)

		hasWebSearch := false
		for _, tVal := range tools {
			toolMap, ok := tVal.(map[string]interface{})
			if !ok {
				t.Fatalf("expected tool to be a map")
			}
			tType, _ := toolMap["type"].(string)
			if tType == "web_search" {
				hasWebSearch = true
				// 标准字段应保留
				if toolMap["search_context_size"] != "high" {
					t.Errorf("expected web_search to keep standard field search_context_size, got: %v", toolMap)
				}
				// 客户端私有字段应被剥离
				if _, exists := toolMap["external_web_access"]; exists {
					t.Errorf("expected web_search private field external_web_access to be stripped, got: %v", toolMap)
				}
			} else {
				name, _ := toolMap["name"].(string)
				toolNames[name] = true
				toolTypes[name] = tType
			}
		}

		if !toolNames["js"] {
			t.Error("expected tool 'js' to be present")
		}
		if !toolNames["apply_patch"] {
			t.Error("expected tool 'apply_patch' to be present")
		}
		if !toolNames["view_image"] {
			t.Error("expected tool 'view_image' to be present")
		}
		if !toolNames["mcp__node_repl"] {
			t.Error("expected tool 'mcp__node_repl' to be present")
		}
		if !hasWebSearch {
			t.Error("expected tool 'web_search' to be present")
		}
		if toolTypes["mcp__node_repl"] != "mcp" {
			t.Error("expected 'mcp__node_repl' tool type to be mcp")
		}
		if _, exists := toolTypes["namespace"]; exists {
			t.Error("namespace tool type should have been removed")
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "resp_nativeToolsOk",
			"object": "response",
			"created": 1741476542,
			"model": "gpt-5.4",
			"output": [
				{
					"id": "msg_ok",
					"object": "response.output",
					"type": "message",
					"status": "completed",
					"role": "assistant",
					"content": [
						{
							"type": "text",
							"text": "Native execution with cleaned tools."
						}
					]
				}
			]
		}`))
	}))
	defer server.Close()

	ep := &core.Endpoint{
		ID:           "ep-native-tools-complex",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeResponses},
	}
	p := NewOpenAIProvider("test-openai-native-complex", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{
		"model": "gpt-5.4",
		"input": [
			{
				"role": "developer",
				"content": [
					{
						"type": "input_text",
						"text": "System prompt"
					}
				],
				"type": "input_text"
			},
			{
				"role": "user",
				"content": "Run some complex tasks natively"
			}
		],
		"tools": [
			{
				"name": "mcp__node_repl",
				"type": "namespace",
				"tools": [
					{
						"type": "function",
						"name": "js",
						"description": "Run JavaScript in persistent Node kernel",
						"strict": false,
						"parameters": {
							"type": "object"
						}
					}
				]
			},
			{
				"type": "mcp",
				"name": "mcp__node_repl",
				"mcp": {
					"server": "node_repl"
				}
			},
			{
				"name": "apply_patch",
				"type": "custom",
				"description": "Apply code patch",
				"parameters": {
					"type": "object"
				}
			},
			{
				"type": "function",
				"name": "view_image",
				"function": {
					"description": "View an image",
					"strict": false,
					"parameters": {
						"type": "object"
					}
				}
			},
			{
				"type": "web_search",
				"search_context_size": "high",
				"external_web_access": false
			}
		]
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal responses result: %v", err)
	}

	if resp["id"] != "resp_nativeToolsOk" {
		t.Errorf("expected id=resp_nativeToolsOk, got %v", resp["id"])
	}
}

func TestOpenAIResponses_Translation_ToolCalls_NonStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"id": "chatcmpl-toolCallNonStream",
			"object": "chat.completion",
			"created": 1741476542,
			"model": "gpt-5.4",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": null,
						"tool_calls": [
							{
								"id": "call_abc123",
								"type": "function",
								"function": {
									"name": "js",
									"arguments": "{\"code\":\"console.log(1)\"}"
								}
							}
						]
					},
					"finish_reason": "tool_calls"
				}
			],
			"usage": {
				"prompt_tokens": 10,
				"completion_tokens": 20,
				"total_tokens": 30
			}
		}`))
	}))
	defer server.Close()

	ep := &core.Endpoint{
		ID:           "ep-translation-toolcalls",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-openai-toolcalls", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{"model": "gpt-5.4", "input": "Run JS"}`
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal responses result: %v", err)
	}

	outputList, ok := resp["output"].([]interface{})
	if !ok || len(outputList) != 1 {
		t.Fatalf("expected output size 1, got %v", resp["output"])
	}

	item, _ := outputList[0].(map[string]interface{})
	if item["type"] != "function_call" || item["id"] != "call_abc123" || item["name"] != "js" {
		t.Errorf("unexpected output item structure: %v", item)
	}
	if item["arguments"] != "{\"code\":\"console.log(1)\"}" {
		t.Errorf("unexpected arguments: %v", item["arguments"])
	}
}

func TestOpenAIResponses_Translation_ToolCalls_Stream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)

		chunks := []string{
			`data: {"id":"chatcmpl-toolCallStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_abc123","type":"function","function":{"name":"js","arguments":""}}]},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-toolCallStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"code\""}}]},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-toolCallStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"log\"}"}}]},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-toolCallStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
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

	ep := &core.Endpoint{
		ID:           "ep-translation-toolcalls-stream",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-openai-toolcalls-stream", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{"model": "gpt-5.4", "input": "Run JS stream", "stream": true}`
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.IsStream = true
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	respBody := w.Body.String()

	expectedEvents := []string{
		`event: response.created`,
		`event: response.in_progress`,
		`event: response.output_item.added`,
		`"type":"function_call"`,
		`"name":"js"`,
		`"id":"call_abc123"`,
		`event: response.function_call.arguments.delta`,
		`"delta":"{\"code\""`,
		`"delta":":\"log\"}"`,
		`event: response.function_call.arguments.done`,
		`"arguments":"{\"code\":\"log\"}"`,
		`event: response.output_item.done`,
		`"status":"completed"`,
		`event: response.done`,
		`event: response.completed`,
		`data: [DONE]`,
	}

	for _, expected := range expectedEvents {
		if !strings.Contains(respBody, expected) {
			t.Errorf("expected response stream to contain %q, but got:\n%s", expected, respBody)
		}
	}
}

func TestOpenAIResponses_Translation_WithComplexInput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var oaiReq struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&oaiReq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// 验证收到的 messages
		if len(oaiReq.Messages) != 3 {
			http.Error(w, fmt.Sprintf("expected 3 messages, got %d", len(oaiReq.Messages)), http.StatusBadRequest)
			return
		}
		if oaiReq.Messages[0].Role != "system" || oaiReq.Messages[0].Content != "System prompt" {
			http.Error(w, "invalid msg 0", http.StatusBadRequest)
			return
		}
		if oaiReq.Messages[1].Role != "system" || oaiReq.Messages[1].Content != "Developer prompt" {
			http.Error(w, "invalid msg 1", http.StatusBadRequest)
			return
		}
		if oaiReq.Messages[2].Role != "user" || oaiReq.Messages[2].Content != "User question" {
			http.Error(w, "invalid msg 2", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"id": "chatcmpl-complexTest",
			"object": "chat.completion",
			"created": 1677649400,
			"model": "gpt-5.4",
			"choices": [
				{
					"index": 0,
					"message": {
						"role": "assistant",
						"content": "Replied"
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

	ep := &core.Endpoint{
		ID:           "ep-complex-input",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-complex-input", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{
		"model": "gpt-5.4",
		"instructions": "System prompt",
		"input": [
			{
				"role": "developer",
				"content": [
					{
						"type": "input_text",
						"text": "Developer prompt"
					}
				]
			},
			{
				"role": "user",
				"content": [
					{
						"type": "input_text",
						"text": "User question"
					}
				]
			}
		]
	}`

	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeResponses
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "gpt-5.4"
	gctx.IsStream = false
	gctx.SelectedEndpoint = ep

	invoker := &openaiResponsesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d. Body: %s", w.Code, w.Body.String())
	}
}
