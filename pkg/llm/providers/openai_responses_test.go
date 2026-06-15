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
		`"delta":"Hi there!"`,
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

		// 验证 tools 是否被正确处理
		tools, ok := req["tools"].([]interface{})
		if !ok || len(tools) != 3 {
			t.Fatalf("expected tools length 3, got %v", req["tools"])
		}

		// 收集解析后的工具名称，验证是否只保留了 js, apply_patch 和 view_image，并且均为标准嵌套格式
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

		if !toolNames["js"] {
			t.Error("expected tool 'js' to be present")
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
		"input": "Run some complex tasks",
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

func TestParseXMLToolCall(t *testing.T) {
	// 1. 测试 update_plan 的不规则格式
	xml1 := `<tool_call>
<function=update_plan>
<parameter=explanation>Let's plan</parameter>
<parameter=[{"status": "in_progress", "step": "step1"}]></parameter>
</function>
</tool_call>`
	tc1, err := parseXMLToolCall(xml1)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}
	if tc1.Name != "update_plan" {
		t.Errorf("expected update_plan, got %s", tc1.Name)
	}
	var args1 map[string]interface{}
	if err := json.Unmarshal([]byte(tc1.Arguments), &args1); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if args1["explanation"] != "Let's plan" {
		t.Errorf("expected Let's plan, got %v", args1["explanation"])
	}
	steps, ok := args1["steps"].([]interface{})
	if !ok || len(steps) != 1 {
		t.Fatalf("expected 1 step, got %v", args1["steps"])
	}
	s0 := steps[0].(map[string]interface{})
	if s0["status"] != "in_progress" || s0["step"] != "step1" {
		t.Errorf("unexpected step value: %v", s0)
	}

	// 2. 测试 apply_patch 的不规则格式
	xml2 := `<tool_call>
<function=apply_patch>
<parameter=explanation>Applying fix</parameter>
<parameter=*** Begin Patch
some patch
*** End Patch></parameter>
</function>
</tool_call>`
	tc2, err := parseXMLToolCall(xml2)
	if err != nil {
		t.Fatalf("failed to parse patch: %v", err)
	}
	if tc2.Name != "apply_patch" {
		t.Errorf("expected apply_patch, got %s", tc2.Name)
	}
	var args2 map[string]interface{}
	json.Unmarshal([]byte(tc2.Arguments), &args2)
	if args2["explanation"] != "Applying fix" {
		t.Errorf("expected explanation, got %v", args2["explanation"])
	}
	if !strings.Contains(args2["patch"].(string), "some patch") {
		t.Errorf("expected patch, got %v", args2["patch"])
	}

	// 3. 测试 exec_command 的各种不规则格式
	// Case 3A: name="cmd"
	xml3A := `<tool_call>
<function=exec_command>
<parameter name="cmd">ls -la</parameter>
</function>
</tool_call>`
	tc3A, err := parseXMLToolCall(xml3A)
	if err != nil {
		t.Fatalf("failed to parse 3A: %v", err)
	}
	var args3A map[string]interface{}
	json.Unmarshal([]byte(tc3A.Arguments), &args3A)
	if args3A["cmd"] != "ls -la" {
		t.Errorf("expected ls -la, got %v", args3A["cmd"])
	}

	// Case 3B: parameter cmd (没有=且不带引号)
	xml3B := `<tool_call>
<function=exec_command>
<parameter cmd>ls -la</parameter>
</function>
</tool_call>`
	tc3B, err := parseXMLToolCall(xml3B)
	if err != nil {
		t.Fatalf("failed to parse 3B: %v", err)
	}
	var args3B map[string]interface{}
	json.Unmarshal([]byte(tc3B.Arguments), &args3B)
	if args3B["cmd"] != "ls -la" {
		t.Errorf("expected ls -la, got %v", args3B["cmd"])
	}

	// Case 3C: 空 parameter attribute (无参数名，fallback 机制)
	xml3C := `<tool_call>
<function=exec_command>
<parameter>ls -la</parameter>
</function>
</tool_call>`
	tc3C, err := parseXMLToolCall(xml3C)
	if err != nil {
		t.Fatalf("failed to parse 3C: %v", err)
	}
	var args3C map[string]interface{}
	json.Unmarshal([]byte(tc3C.Arguments), &args3C)
	if args3C["cmd"] != "ls -la" {
		t.Errorf("expected ls -la, got %v", args3C["cmd"])
	}

	// Case 3D: 极端畸形，完全没有 parameter 标签，直接写在 function 内
	xml3D := `<tool_call>
<function=exec_command>
ls -la
</function>
</tool_call>`
	tc3D, err := parseXMLToolCall(xml3D)
	if err != nil {
		t.Fatalf("failed to parse 3D: %v", err)
	}
	var args3D map[string]interface{}
	json.Unmarshal([]byte(tc3D.Arguments), &args3D)
	if args3D["cmd"] != "ls -la" {
		t.Errorf("expected ls -la, got %v", args3D["cmd"])
	}
}

func TestOpenAIResponses_XMLToolCalls_Stream(t *testing.T) {
	// 1. 模拟上游返回的 Content 里包含 XML 标签的流
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)

		chunks := []string{
			`data: {"id":"chatcmpl-xmlStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"Preamble message. <tool_"},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-xmlStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"call>\n<function=update_plan>\n<parameter=explanation>Planning</parameter>"},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-xmlStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"\n<parameter=[{\"status\":\"in_progress\",\"step\":\"setup\"}]></parameter>\n</function>\n</tool_call>\nPostamble."},"finish_reason":null}]}`,
			`data: {"id":"chatcmpl-xmlStream","object":"chat.completion.chunk","created":1741290958,"model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
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
		ID:           "ep-translation-xml-stream",
		Provider:     "openai",
		Model:        "gpt-5.4",
		RequestTypes: []core.RequestType{core.RequestTypeChatCompletion},
	}
	p := NewOpenAIProvider("test-openai-xml-stream", server.URL, "test-key", []string{"gpt-5.4"})

	reqBody := `{"model": "gpt-5.4", "input": "Do it", "stream": true}`
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

	// 2. 验证：
	// - "Preamble message. " 应该以文本形式输出
	// - 应该成功解析出 tool_call (update_plan) 并作为 function_call 输出
	// - "Postamble." 应该以文本形式输出
	expectedEvents := []string{
		`event: response.output_text.delta`,
		`"delta":"Preamble "`,
		`"delta":"message. "`,
		`event: response.output_item.added`,
		`"type":"function_call"`,
		`"name":"update_plan"`,
		`event: response.function_call.arguments.delta`,
		`"delta":"{\"explanation\":\"Planning\",\"steps\":[{\"status\":\"in_progress\",\"step\":\"setup\"}]}"`,
		`"delta":"\nPostamble."`,
		`event: response.done`,
		`event: response.completed`,
		`data: [DONE]`,
	}

	for _, expected := range expectedEvents {
		if !strings.Contains(respBody, expected) {
			t.Errorf("expected stream to contain %q, but got:\n%s", expected, respBody)
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
