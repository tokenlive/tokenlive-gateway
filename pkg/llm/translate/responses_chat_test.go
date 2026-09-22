package translate

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestResponsesRequestToChat_Basic(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-4",
		"instructions": "Be helpful",
		"input": "hello",
		"max_output_tokens": 50
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	_ = json.Unmarshal(out, &req)
	msgs := req["messages"].([]interface{})
	if len(msgs) != 2 {
		t.Fatalf("messages len = %d", len(msgs))
	}
	sys := msgs[0].(map[string]interface{})
	if sys["role"] != "system" || sys["content"] != "Be helpful" {
		t.Errorf("system = %v", sys)
	}
	if _, ok := req["max_output_tokens"]; ok {
		t.Error("max_output_tokens should be deleted")
	}
	if req["max_completion_tokens"].(float64) != 50 {
		t.Errorf("max_completion_tokens = %v", req["max_completion_tokens"])
	}
}

func TestResponsesRequestToChat_DropsReasoningItems(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-4",
		"input": [
			{"type": "reasoning", "id": "rs_1", "summary": [{"type": "summary_text", "text": "thinking"}]},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}
		]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	msgs, ok := req["messages"].([]interface{})
	if !ok {
		t.Fatalf("messages missing: %v", req)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages len = %d, want 1: %+v", len(msgs), msgs)
	}
	msg, _ := msgs[0].(map[string]interface{})
	if msg["role"] != "user" || msg["content"] != "hello" {
		t.Errorf("message = %v", msg)
	}
}

func TestResponsesRequestToChat_StripsResponsesOnlyParams(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-4",
		"input": "hello",
		"store": false,
		"previous_response_id": "resp_1",
		"include": ["reasoning.encrypted_content"],
		"background": true,
		"truncation": "auto",
		"text": {"format": {"type": "text"}},
		"reasoning": {"effort": "medium"}
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"store", "previous_response_id", "include", "background", "truncation", "text", "reasoning"} {
		if _, exists := req[key]; exists {
			t.Errorf("responses-only param %q leaked to chat request", key)
		}
	}
	if req["reasoning_effort"] != "medium" {
		t.Errorf("reasoning_effort = %v, want medium", req["reasoning_effort"])
	}
}

func TestResponsesRequestToChat_PreservesNamespacedTools(t *testing.T) {
	raw := []byte(`{
		"model": "glm-5.3",
		"input": [
			{"type": "function_call", "call_id": "call_ns", "namespace": "collaboration", "name": "spawn_agent", "arguments": "{}"},
			{"type": "function_call_output", "call_id": "call_ns", "output": "ok"},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "continue"}]}
		],
		"tools": [{
			"name": "collaboration",
			"type": "namespace",
			"tools": [{
				"type": "function",
				"name": "spawn_agent",
				"parameters": {"type": "object"}
			}]
		}]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}

	tools, _ := req["tools"].([]interface{})
	if len(tools) != 1 {
		t.Fatalf("tools len = %d, want 1", len(tools))
	}
	tool, _ := tools[0].(map[string]interface{})
	fn, _ := tool["function"].(map[string]interface{})
	// Namespace-qualified names are sanitized to a pattern-safe form (dot -> _)
	// so strict upstreams (DeepSeek/Qwen) accept them.
	if fn["name"] != "collaboration_spawn_agent" {
		t.Fatalf("tool name = %v, want collaboration_spawn_agent", fn["name"])
	}

	msgs, _ := req["messages"].([]interface{})
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d, want 3", len(msgs))
	}
	assistant, _ := msgs[0].(map[string]interface{})
	calls, _ := assistant["tool_calls"].([]interface{})
	call, _ := calls[0].(map[string]interface{})
	callFn, _ := call["function"].(map[string]interface{})
	if callFn["name"] != "collaboration_spawn_agent" {
		t.Fatalf("history call name = %v", callFn["name"])
	}
}

func TestResponsesRequestToChat_FunctionCallOutputArray(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-4",
		"input": [
			{"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{}"},
			{"type": "function_call_output", "call_id": "call_1", "output": [
				{"type": "output_text", "text": "result text"}
			]},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "continue"}]}
		]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	msgs, _ := req["messages"].([]interface{})
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d, want 3: %+v", len(msgs), msgs)
	}
	toolMsg, _ := msgs[1].(map[string]interface{})
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "call_1" {
		t.Fatalf("tool message = %v", toolMsg)
	}
	if toolMsg["content"] != "result text" {
		t.Errorf("tool content = %v, want extracted result text", toolMsg["content"])
	}
}

func TestChatCompletionToResponses_Text(t *testing.T) {
	chat := []byte(`{
		"id": "chatcmpl-abc",
		"model": "gpt-4",
		"choices": [{
			"message": {"role": "assistant", "content": "hi"},
			"finish_reason": "stop"
		}],
		"usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4}
	}`)
	res, err := ChatCompletionToResponses(chat, "gpt-4", nil)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(res.Body, &resp)
	if resp["object"] != "response" {
		t.Errorf("object = %v", resp["object"])
	}
	if resp["status"] != "completed" {
		t.Errorf("status = %v", resp["status"])
	}
	id, _ := resp["id"].(string)
	if id != "resp_abc" {
		t.Errorf("id = %v", id)
	}
	usage := resp["usage"].(map[string]interface{})
	if usage["input_tokens"].(float64) != 3 {
		t.Errorf("usage = %v", usage)
	}
}

func TestChatCompletionToResponses_Tools(t *testing.T) {
	chat := []byte(`{
		"id": "chatcmpl-1",
		"choices": [{
			"message": {
				"role": "assistant",
				"content": "",
				"tool_calls": [{
					"id": "call_1",
					"type": "function",
					"function": {"name": "fn", "arguments": "{}"}
				}]
			},
			"finish_reason": "tool_calls"
		}],
		"usage": {"prompt_tokens": 1, "completion_tokens": 1}
	}`)
	res, err := ChatCompletionToResponses(chat, "m", nil)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(res.Body, &resp)
	out := resp["output"].([]interface{})
	if len(out) != 1 {
		t.Fatalf("output len = %d", len(out))
	}
	item := out[0].(map[string]interface{})
	if item["type"] != "function_call" || item["name"] != "fn" || item["call_id"] != "call_1" {
		t.Errorf("item = %v", item)
	}
}

func TestChatCompletionToResponses_NamespacedTool(t *testing.T) {
	chat := []byte(`{
		"id": "chatcmpl-ns",
		"choices": [{
			"message": {
				"role": "assistant",
				"content": "",
				"tool_calls": [{
					"id": "call_ns",
					"type": "function",
					"function": {"name": "collaboration.spawn_agent", "arguments": "{}"}
				}]
			},
			"finish_reason": "tool_calls"
		}]
	}`)
	res, err := ChatCompletionToResponses(chat, "m", nil)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(res.Body, &resp); err != nil {
		t.Fatal(err)
	}
	out, _ := resp["output"].([]interface{})
	item, _ := out[0].(map[string]interface{})
	if item["name"] != "spawn_agent" || item["namespace"] != "collaboration" {
		t.Fatalf("item = %v", item)
	}
}

func TestChatCompletionToResponses_Reasoning(t *testing.T) {
	chat := []byte(`{
		"id": "chatcmpl-reason",
		"model": "gpt-4",
		"choices": [{
			"message": {
				"role": "assistant",
				"content": "answer",
				"reasoning_content": "thinking"
			},
			"finish_reason": "stop"
		}],
		"usage": {"prompt_tokens": 1, "completion_tokens": 1}
	}`)
	res, err := ChatCompletionToResponses(chat, "gpt-4", nil)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(res.Body, &resp); err != nil {
		t.Fatal(err)
	}
	out, _ := resp["output"].([]interface{})
	if len(out) != 2 {
		t.Fatalf("output len = %d, want 2: %+v", len(out), out)
	}
	reasoning, _ := out[0].(map[string]interface{})
	if reasoning["type"] != "reasoning" {
		t.Fatalf("first output type = %v, want reasoning", reasoning["type"])
	}
	summary, _ := reasoning["summary"].([]interface{})
	if len(summary) != 1 {
		t.Fatalf("summary len = %d", len(summary))
	}
	summaryPart, _ := summary[0].(map[string]interface{})
	if summaryPart["type"] != "summary_text" || summaryPart["text"] != "thinking" {
		t.Errorf("summary part = %v", summaryPart)
	}
	message, _ := out[1].(map[string]interface{})
	if message["type"] != "message" {
		t.Fatalf("second output type = %v, want message", message["type"])
	}
}

func TestCorrectNativeResponsesRequest_Namespace(t *testing.T) {
	raw := []byte(`{
		"input": [{"role": "developer", "content": [{"type": "input_text", "text": "hi"}]}],
		"tools": [{
			"name": "weather_service",
			"type": "namespace",
			"tools": [{
				"type": "function",
				"name": "get_weather",
				"parameters": {"type": "object"}
			}]
		}, {
			"type": "tool_search",
			"query": "find weather"
		}]
	}`)
	body, orig, final, summary, err := CorrectNativeResponsesRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if orig != 2 || final != 2 {
		t.Fatalf("counts orig=%d final=%d, want 2", orig, final)
	}
	if len(summary) != 2 || summary[0] != "namespace:weather_service(1_tools)" || summary[1] != "tool_search:" {
		t.Errorf("summary = %v", summary)
	}
	var payload map[string]interface{}
	_ = json.Unmarshal(body, &payload)
	input := payload["input"].([]interface{})
	item := input[0].(map[string]interface{})
	if item["role"] != "system" {
		t.Errorf("role = %v", item["role"])
	}
	content := item["content"].([]interface{})
	c0 := content[0].(map[string]interface{})
	if c0["type"] != "input_text" {
		t.Errorf("content type = %v, want input_text", c0["type"])
	}

	tools := payload["tools"].([]interface{})
	nsTool := tools[0].(map[string]interface{})
	if nsTool["type"] != "namespace" || nsTool["name"] != "weather_service" {
		t.Errorf("expected namespace tool, got: %v", nsTool)
	}
	subTools := nsTool["tools"].([]interface{})
	if len(subTools) != 1 {
		t.Fatalf("expected 1 subtool, got %d", len(subTools))
	}
	if subTools[0].(map[string]interface{})["name"] != "get_weather" {
		t.Errorf("expected subtool get_weather, got %v", subTools[0])
	}
	if tools[1].(map[string]interface{})["type"] != "tool_search" {
		t.Errorf("expected tool_search to be preserved when namespace exists")
	}
}

func TestStripToolSearchDescription(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-6",
		"tools": [
			{"type": "function", "name": "shell", "description": "keep me"},
			{"type": "tool_search", "description": "drop me", "execution": "server"},
			{"type": "namespace", "name": "codex", "tools": [
				{"type": "tool_search", "description": "nested drop", "execution": "server"}
			]}
		]
	}`)
	body, stripped, err := StripToolSearchDescription(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !stripped {
		t.Fatal("expected description to be stripped")
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	tools := payload["tools"].([]interface{})
	fn := tools[0].(map[string]interface{})
	if fn["description"] != "keep me" {
		t.Errorf("function description = %v", fn["description"])
	}
	search := tools[1].(map[string]interface{})
	if _, exists := search["description"]; exists {
		t.Errorf("tool_search description still present: %v", search)
	}
	if search["execution"] != "server" {
		t.Errorf("execution = %v", search["execution"])
	}
	nested := tools[2].(map[string]interface{})["tools"].([]interface{})[0].(map[string]interface{})
	if _, exists := nested["description"]; exists {
		t.Errorf("nested tool_search description still present: %v", nested)
	}

	unchanged, stripped, err := StripToolSearchDescription([]byte(`{"tools":[{"type":"function","name":"shell"}]}`))
	if err != nil || stripped || string(unchanged) != `{"tools":[{"type":"function","name":"shell"}]}` {
		t.Fatalf("expected untouched body, stripped=%v err=%v body=%s", stripped, err, unchanged)
	}
	bad, stripped, err := StripToolSearchDescription([]byte(`not-json`))
	if err != nil || stripped || string(bad) != `not-json` {
		t.Fatalf("expected invalid JSON to pass through, stripped=%v err=%v", stripped, err)
	}
}

func TestCorrectNativeResponsesRequest_IsolatedToolSearchStripped(t *testing.T) {
	raw := []byte(`{
		"input": [{"role": "user", "content": "hi"}],
		"tools": [{
			"type": "function",
			"name": "calc",
			"parameters": {"type": "object"}
		}, {
			"type": "tool_search"
		}]
	}`)
	body, orig, final, _, err := CorrectNativeResponsesRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if orig != 2 || final != 1 {
		t.Fatalf("counts orig=%d final=%d, want orig=2 final=1", orig, final)
	}
	var payload map[string]interface{}
	_ = json.Unmarshal(body, &payload)
	tools := payload["tools"].([]interface{})
	if len(tools) != 1 || tools[0].(map[string]interface{})["name"] != "calc" {
		t.Errorf("expected isolated tool_search to be stripped, got: %v", tools)
	}
}

func TestCorrectNativeResponsesRequest_KeepsInputTextAndNormalizesTextAlias(t *testing.T) {
	raw := []byte(`{
		"input": [
			{"role": "user", "type": "message", "content": [{"type": "input_text", "text": "ping"}]},
			{"role": "user", "content": [{"type": "text", "text": "legacy"}]}
		]
	}`)
	body, _, _, _, err := CorrectNativeResponsesRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	input := payload["input"].([]interface{})
	if len(input) != 2 {
		t.Fatalf("input len = %d", len(input))
	}

	item0 := input[0].(map[string]interface{})
	if _, exists := item0["type"]; exists {
		t.Fatalf("expected message item type to be removed, got %v", item0["type"])
	}
	c0 := item0["content"].([]interface{})[0].(map[string]interface{})
	if c0["type"] != "input_text" {
		t.Fatalf("item0 content type = %v, want input_text", c0["type"])
	}

	item1 := input[1].(map[string]interface{})
	c1 := item1["content"].([]interface{})[0].(map[string]interface{})
	if c1["type"] != "input_text" {
		t.Fatalf("item1 content type = %v, want input_text (normalized from text)", c1["type"])
	}
}

func TestCleanJSONSchema_RequiredNull(t *testing.T) {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"foo": map[string]interface{}{
				"type": "string",
			},
		},
		"required": nil,
	}
	cleaned := cleanJSONSchema(schema, false)
	req, exists := cleaned["required"]
	if !exists {
		t.Fatalf("expected required to be present as empty array")
	}
	arr, ok := req.([]interface{})
	if !ok || arr == nil {
		t.Fatalf("expected required []interface{}, got %T %v", req, req)
	}
	if len(arr) != 0 {
		t.Errorf("expected empty required, got %v", arr)
	}
	encoded, err := json.Marshal(cleaned)
	if err != nil {
		t.Fatalf("failed to marshal cleaned schema: %v", err)
	}
	if !strings.Contains(string(encoded), `"required":[]`) {
		t.Errorf("encoded JSON missing required:[], got %s", string(encoded))
	}
	if strings.Contains(string(encoded), `"required":null`) {
		t.Errorf("encoded JSON contains required:null: %s", string(encoded))
	}
}

func TestCleanJSONSchema_RequiredEmptyArray(t *testing.T) {
	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"foo": map[string]interface{}{
				"type": "string",
			},
		},
		"required": []interface{}{},
	}
	cleaned := cleanJSONSchema(schema, false)
	req, exists := cleaned["required"]
	if !exists {
		t.Fatalf("expected required kept")
	}
	arr, ok := req.([]interface{})
	if !ok || len(arr) != 0 {
		t.Errorf("expected empty required array, got %v", req)
	}
	encoded, err := json.Marshal(cleaned)
	if err != nil {
		t.Fatalf("failed to marshal cleaned schema: %s", err)
	}
	if strings.Contains(string(encoded), `"required":null`) {
		t.Errorf("encoded JSON contains required:null: %s", string(encoded))
	}
	if !strings.Contains(string(encoded), `"required":[]`) {
		t.Errorf("encoded JSON missing required:[], got %s", string(encoded))
	}
}

func TestMessagesRequestToChat_RequiredEmptyArray(t *testing.T) {
	raw := []byte(`{
		"model": "claude-opus-4.5",
		"max_tokens": 100,
		"messages": [{"role": "user", "content": "hi"}],
		"tools": [{
			"name": "noop",
			"description": "no args",
			"input_schema": {
				"type": "object",
				"properties": {},
				"required": []
			}
		}]
	}`)
	out, err := MessagesRequestToChat(raw, MessagesToChatOptions{OfficialOrTest: false})
	if err != nil {
		t.Fatalf("MessagesRequestToChat: %v", err)
	}
	if strings.Contains(string(out), `"required":null`) {
		t.Fatalf("output contains required:null: %s", string(out))
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	tools := req["tools"].([]interface{})
	tool := tools[0].(map[string]interface{})
	fn := tool["function"].(map[string]interface{})
	params := fn["parameters"].(map[string]interface{})
	arr, ok := params["required"].([]interface{})
	if !ok {
		t.Fatalf("expected required array, got %v in %s", params["required"], string(out))
	}
	if len(arr) != 0 {
		t.Errorf("expected empty required, got %v", arr)
	}
}

func TestCorrectNativeMessagesRequest_RequiredEmptyArray(t *testing.T) {
	raw := []byte(`{
		"model": "claude-opus-4.5",
		"max_tokens": 100,
		"messages": [{"role": "user", "content": "hi"}],
		"tools": [{
			"name": "noop",
			"description": "no args",
			"input_schema": {
				"type": "object",
				"properties": {}
			}
		}]
	}`)
	out, err := CorrectNativeMessagesRequest(raw)
	if err != nil {
		t.Fatalf("CorrectNativeMessagesRequest: %v", err)
	}
	if strings.Contains(string(out), `"required":null`) {
		t.Fatalf("output contains required:null: %s", string(out))
	}
	if !strings.Contains(string(out), `"required":[]`) {
		t.Fatalf("output missing required:[], got %s", string(out))
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	tools := req["tools"].([]interface{})
	tool := tools[0].(map[string]interface{})
	schema := tool["input_schema"].(map[string]interface{})
	arr, ok := schema["required"].([]interface{})
	if !ok {
		t.Errorf("expected required array, got %v in %s", schema["required"], string(out))
	}
	if len(arr) != 0 {
		t.Errorf("expected empty required, got %v", arr)
	}
}

// TestResponsesChat_RoundTrip_NamespacedToolSanitized proves the fix for
// strict upstreams (DeepSeek/Qwen) that reject dots in tools[].function.name:
// the request path sanitizes "namespace.name" to "namespace_name", and the
// response path restores the original (namespace, name) via the mapper.
func TestResponsesChat_RoundTrip_NamespacedToolSanitized(t *testing.T) {
	raw := []byte(`{
		"model": "deepseek-chat",
		"input": [
			{"type": "function_call", "call_id": "call_ns", "namespace": "collaboration", "name": "spawn_agent", "arguments": "{}"},
			{"type": "function_call_output", "call_id": "call_ns", "output": "ok"},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "go"}]}
		],
		"tools": [{
			"name": "collaboration",
			"type": "namespace",
			"tools": [{
				"type": "function",
				"name": "spawn_agent",
				"parameters": {"type": "object"}
			}]
		}]
	}`)

	chatBody, mapper, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var chatReq map[string]interface{}
	if err := json.Unmarshal(chatBody, &chatReq); err != nil {
		t.Fatal(err)
	}

	// 1. Outbound tool name must be pattern-safe (no dot) for DeepSeek.
	tools, _ := chatReq["tools"].([]interface{})
	if len(tools) != 1 {
		t.Fatalf("tools len = %d, want 1", len(tools))
	}
	tool, _ := tools[0].(map[string]interface{})
	fn, _ := tool["function"].(map[string]interface{})
	gotName, _ := fn["name"].(string)
	if gotName != "collaboration_spawn_agent" {
		t.Fatalf("outbound tool name = %q, want collaboration_spawn_agent (dot rejected by DeepSeek)", gotName)
	}
	if strings.Contains(gotName, ".") {
		t.Fatalf("outbound tool name %q still contains a dot", gotName)
	}

	// 2. History tool_call name must match the same sanitized form.
	msgs, _ := chatReq["messages"].([]interface{})
	histCall, _ := msgs[0].(map[string]interface{})
	histCalls, _ := histCall["tool_calls"].([]interface{})
	histFn, _ := histCalls[0].(map[string]interface{})["function"].(map[string]interface{})
	if histFn["name"] != "collaboration_spawn_agent" {
		t.Fatalf("history tool_call name = %v, want collaboration_spawn_agent", histFn["name"])
	}

	// 3. DeepSeek echoes back the sanitized name; response path restores namespace.
	chatResp := []byte(`{
		"id": "chatcmpl-rt",
		"choices": [{
			"message": {
				"role": "assistant",
				"content": "",
				"tool_calls": [{
					"id": "call_ns",
					"type": "function",
					"function": {"name": "collaboration_spawn_agent", "arguments": "{\"x\":1}"}
				}]
			},
			"finish_reason": "tool_calls"
		}],
		"usage": {"prompt_tokens": 2, "completion_tokens": 1}
	}`)
	res, err := ChatCompletionToResponses(chatResp, "deepseek-chat", mapper)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(res.Body, &resp); err != nil {
		t.Fatal(err)
	}
	out, _ := resp["output"].([]interface{})
	if len(out) != 1 {
		t.Fatalf("output len = %d, want 1", len(out))
	}
	item, _ := out[0].(map[string]interface{})
	if item["type"] != "function_call" {
		t.Fatalf("type = %v, want function_call", item["type"])
	}
	if item["name"] != "spawn_agent" {
		t.Errorf("restored name = %v, want spawn_agent", item["name"])
	}
	if item["namespace"] != "collaboration" {
		t.Errorf("restored namespace = %v, want collaboration", item["namespace"])
	}
}

// TestResponsesChat_RoundTrip_ColonNamespaceTool covers MCP/plugin namespaced
// tools whose namespace contains a colon (e.g. "browser-use:control-browser"),
// which is the actual shape that triggered the DeepSeek 400 in the field.
func TestResponsesChat_RoundTrip_ColonNamespaceTool(t *testing.T) {
	raw := []byte(`{
		"model": "deepseek-chat",
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "go"}]}
		],
		"tools": [{
			"name": "browser-use",
			"type": "namespace",
			"tools": [{
				"type": "function",
				"name": "control-browser",
				"parameters": {"type": "object"}
			}]
		}]
	}`)
	chatBody, mapper, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var chatReq map[string]interface{}
	_ = json.Unmarshal(chatBody, &chatReq)
	tools, _ := chatReq["tools"].([]interface{})
	fn, _ := tools[0].(map[string]interface{})["function"].(map[string]interface{})
	gotName, _ := fn["name"].(string)
	if strings.ContainsAny(gotName, ".:") {
		t.Fatalf("outbound tool name %q contains illegal separator", gotName)
	}

	chatResp := []byte(`{
		"id": "chatcmpl-c",
		"choices": [{
			"message": {"role": "assistant", "content": "", "tool_calls": [{
				"id": "c1", "type": "function",
				"function": {"name": "browser-use_control-browser", "arguments": "{}"}
			}]},
			"finish_reason": "tool_calls"
		}]
	}`)
	res, err := ChatCompletionToResponses(chatResp, "deepseek-chat", mapper)
	if err != nil {
		t.Fatal(err)
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(res.Body, &resp)
	out, _ := resp["output"].([]interface{})
	item, _ := out[0].(map[string]interface{})
	if item["name"] != "control-browser" || item["namespace"] != "browser-use" {
		t.Fatalf("restored = name=%v namespace=%v, want control-browser / browser-use", item["name"], item["namespace"])
	}
}

// TestResponsesRequestToChat_ReasoningPassedBack covers the fix for DeepSeek
// thinking models requiring reasoning_content in multi-turn history. Codex
// sends the previous reasoning item back in `input`; the translator must NOT
// drop it — it must fold the reasoning summary into the next assistant
// message's `reasoning_content` field so DeepSeek accepts the turn.
func TestResponsesRequestToChat_ReasoningPassedBack(t *testing.T) {
	raw := []byte(`{
		"model": "deepseek-chat",
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
			{"type": "reasoning", "id": "rs_1", "summary": [{"type": "summary_text", "text": "thinking hard"}]},
			{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "again"}]}
		]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	msgs, _ := req["messages"].([]interface{})
	// Expect: system(none here) + user + assistant(with reasoning_content) + user
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d, want 3: %+v", len(msgs), msgs)
	}
	assistant, _ := msgs[1].(map[string]interface{})
	if assistant["role"] != "assistant" {
		t.Fatalf("msg[1] role = %v, want assistant", assistant["role"])
	}
	rc, ok := assistant["reasoning_content"].(string)
	if !ok || rc == "" {
		t.Fatalf("assistant message missing reasoning_content; DeepSeek thinking mode rejects this. msg=%v", assistant)
	}
	if rc != "thinking hard" {
		t.Errorf("reasoning_content = %q, want %q", rc, "thinking hard")
	}
}

// Reasoning followed by a function_call assistant turn must also carry the
// reasoning_content into the assembled assistant tool_call message.
func TestResponsesRequestToChat_ReasoningBeforeToolCall(t *testing.T) {
	raw := []byte(`{
		"model": "deepseek-chat",
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "run it"}]},
			{"type": "reasoning", "id": "rs_1", "summary": [{"type": "summary_text", "text": "plan: call js"}]},
			{"type": "function_call", "call_id": "call_1", "name": "js", "arguments": "{\"code\":\"1\"}"},
			{"type": "function_call_output", "call_id": "call_1", "output": "1"},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "ok"}]}
		]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	_ = json.Unmarshal(out, &req)
	msgs, _ := req["messages"].([]interface{})
	// Find the assistant message with tool_calls
	var asstWithTools map[string]interface{}
	for _, m := range msgs {
		mm, _ := m.(map[string]interface{})
		if mm != nil && mm["role"] == "assistant" {
			if _, ok := mm["tool_calls"]; ok {
				asstWithTools = mm
				break
			}
		}
	}
	if asstWithTools == nil {
		t.Fatalf("no assistant message with tool_calls found: %+v", msgs)
	}
	rc, ok := asstWithTools["reasoning_content"].(string)
	if !ok || rc != "plan: call js" {
		t.Fatalf("tool_call assistant missing reasoning_content = %q; got msg=%v", rc, asstWithTools)
	}
}

// When a conversation operates in thinking mode (e.g. reasoning.effort is set, or
// previous reasoning items exist in history), any assistant message WITHOUT a prior
// reasoning item (e.g. from session compaction or third-party history) falls back
// to "" so upstream thinking models accept the turn without 400 rejection.
// This is purely protocol-driven and works regardless of the model's brand/name.
func TestResponsesRequestToChat_ThinkingModeFallbackEmptyReasoning(t *testing.T) {
	raw := []byte(`{
		"model": "my-custom-reasoner",
		"reasoning": {"effort": "high"},
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
			{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hi"}]},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "how are you?"}]}
		]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	_ = json.Unmarshal(out, &req)
	msgs, _ := req["messages"].([]interface{})
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d, want 3", len(msgs))
	}
	assistant, _ := msgs[1].(map[string]interface{})
	rc, exists := assistant["reasoning_content"]
	if !exists {
		t.Fatalf("thinking mode assistant message must have reasoning_content fallback to empty string; got %+v", assistant)
	}
	if rc != "" {
		t.Errorf("expected empty string fallback, got %v", rc)
	}
}

// Non-thinking mode conversations (e.g. standard gpt-4 or any model without reasoning
// parameters or history) MUST NOT receive reasoning_content: "" to prevent 400 Bad Request
// on strict upstream schemas.
func TestResponsesRequestToChat_NonThinkingNoEmptyReasoningFallback(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-4",
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
			{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hi"}]},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "how are you?"}]}
		]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	_ = json.Unmarshal(out, &req)
	msgs, _ := req["messages"].([]interface{})
	assistant, _ := msgs[1].(map[string]interface{})
	if _, exists := assistant["reasoning_content"]; exists {
		t.Fatalf("non-thinking assistant message should NOT have reasoning_content: %+v", assistant)
	}
}

// DeepSeek rejects an assistant tool_calls message unless every id is answered
// by the tool messages that follow it immediately. These histories are shapes
// Responses clients actually replay; the translator must not split or drop the pair.
func TestResponsesRequestToChat_ToolCallsStayPaired(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string // assistant tool_call ids, in order, each answered by the next tool messages
	}{
		{
			name: "output id only",
			raw: `{
				"model": "deepseek-v4-flash",
				"input": [
					{"type": "function_call", "id": "call_1", "name": "read", "arguments": "{}"},
					{"type": "function_call_output", "id": "call_1", "output": "ok"},
					{"type": "message", "role": "user", "content": "next"}
				]
			}`,
			want: []string{"call_1"},
		},
		{
			name: "message splits a parallel round",
			raw: `{
				"model": "deepseek-v4-flash",
				"input": [
					{"type": "function_call", "call_id": "call_a", "name": "read", "arguments": "{}"},
					{"type": "message", "role": "assistant", "content": "partial"},
					{"type": "function_call", "call_id": "call_b", "name": "bash", "arguments": "{}"},
					{"type": "function_call_output", "call_id": "call_a", "output": "file"},
					{"type": "function_call_output", "call_id": "call_b", "output": "done"},
					{"type": "message", "role": "user", "content": "next"}
				]
			}`,
			want: []string{"call_a", "call_b"},
		},
		{
			name: "output before call",
			raw: `{
				"model": "deepseek-v4-flash",
				"input": [
					{"type": "function_call_output", "call_id": "call_1", "output": "ok"},
					{"type": "function_call", "call_id": "call_1", "name": "read", "arguments": "{}"},
					{"type": "message", "role": "user", "content": "next"}
				]
			}`,
			want: []string{"call_1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, _, err := ResponsesRequestToChat([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			var req map[string]interface{}
			if err := json.Unmarshal(out, &req); err != nil {
				t.Fatal(err)
			}
			assertToolCallsPaired(t, req["messages"], tc.want)
		})
	}
}

func assertToolCallsPaired(t *testing.T, messages interface{}, wantIDs []string) {
	t.Helper()
	msgs, _ := messages.([]interface{})
	var assistant map[string]interface{}
	var assistantAt int
	for i, m := range msgs {
		mm, _ := m.(map[string]interface{})
		if mm == nil || mm["role"] != "assistant" {
			continue
		}
		if _, ok := mm["tool_calls"]; ok {
			if assistant != nil {
				t.Fatalf("tool calls split across assistant messages: %+v", msgs)
			}
			assistant = mm
			assistantAt = i
		}
	}
	if assistant == nil {
		t.Fatalf("no assistant tool_calls message: %+v", msgs)
	}
	calls, _ := assistant["tool_calls"].([]interface{})
	if len(calls) != len(wantIDs) {
		t.Fatalf("tool_calls len = %d, want %d: %+v", len(calls), len(wantIDs), assistant)
	}
	var got []string
	for _, c := range calls {
		cm, _ := c.(map[string]interface{})
		id, _ := cm["id"].(string)
		got = append(got, id)
	}
	for i, id := range wantIDs {
		if got[i] != id {
			t.Fatalf("tool_call ids = %v, want %v", got, wantIDs)
		}
	}
	for j, id := range wantIDs {
		idx := assistantAt + 1 + j
		if idx >= len(msgs) {
			t.Fatalf("missing tool message for %s: %+v", id, msgs)
		}
		tool, _ := msgs[idx].(map[string]interface{})
		if tool["role"] != "tool" || tool["tool_call_id"] != id {
			t.Fatalf("message after tool_calls[%d] = %+v, want tool %s", j, tool, id)
		}
	}
}

// Flexible extraction: summary as string or content blocks
func TestResponsesRequestToChat_ReasoningFlexibleFormats(t *testing.T) {
	raw := []byte(`{
		"model": "deepseek-chat",
		"input": [
			{"type": "reasoning", "id": "rs_1", "summary": "plain string thinking"},
			{"type": "message", "role": "assistant", "content": "reply"}
		]
	}`)
	out, _, err := ResponsesRequestToChat(raw)
	if err != nil {
		t.Fatal(err)
	}
	var req map[string]interface{}
	_ = json.Unmarshal(out, &req)
	msgs, _ := req["messages"].([]interface{})
	assistant, _ := msgs[0].(map[string]interface{})
	if assistant["reasoning_content"] != "plain string thinking" {
		t.Errorf("expected plain string thinking, got %v", assistant["reasoning_content"])
	}
}
