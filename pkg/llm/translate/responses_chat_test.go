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
	out, err := ResponsesRequestToChat(raw)
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
	out, err := ResponsesRequestToChat(raw)
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
	out, err := ResponsesRequestToChat(raw)
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
	out, err := ResponsesRequestToChat(raw)
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
	if fn["name"] != "collaboration.spawn_agent" {
		t.Fatalf("tool name = %v, want collaboration.spawn_agent", fn["name"])
	}

	msgs, _ := req["messages"].([]interface{})
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d, want 3", len(msgs))
	}
	assistant, _ := msgs[0].(map[string]interface{})
	calls, _ := assistant["tool_calls"].([]interface{})
	call, _ := calls[0].(map[string]interface{})
	callFn, _ := call["function"].(map[string]interface{})
	if callFn["name"] != "collaboration.spawn_agent" {
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
	out, err := ResponsesRequestToChat(raw)
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
	res, err := ChatCompletionToResponses(chat, "gpt-4")
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
	res, err := ChatCompletionToResponses(chat, "m")
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
	res, err := ChatCompletionToResponses(chat, "m")
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
	res, err := ChatCompletionToResponses(chat, "gpt-4")
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
			"type": "namespace",
			"tools": [{
				"type": "function",
				"name": "get_weather",
				"parameters": {"type": "object"}
			}]
		}]
	}`)
	body, orig, final, summary, err := CorrectNativeResponsesRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if orig != 1 || final != 1 {
		t.Errorf("counts orig=%d final=%d", orig, final)
	}
	if len(summary) != 1 || summary[0] != "function:get_weather" {
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
