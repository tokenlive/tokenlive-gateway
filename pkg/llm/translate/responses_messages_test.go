package translate

import (
	"encoding/json"
	"strings"
	"testing"
)

func toMap(t *testing.T, body []byte) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, body)
	}
	return m
}

func TestResponsesRequestToMessages_Basic(t *testing.T) {
	raw := []byte(`{
		"model": "claude-alias",
		"instructions": "You are helpful.",
		"input": "Hello",
		"temperature": 0.5
	}`)

	res, err := ResponsesRequestToMessages(raw, "claude-sonnet-4-20250514")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)

	if req["model"] != "claude-sonnet-4-20250514" {
		t.Errorf("model = %v", req["model"])
	}
	if req["system"] != "You are helpful." {
		t.Errorf("system = %v", req["system"])
	}
	if req["max_tokens"].(float64) != DefaultMessagesMaxTokens {
		t.Errorf("max_tokens = %v, want %d", req["max_tokens"], DefaultMessagesMaxTokens)
	}
	if req["temperature"].(float64) != 0.5 {
		t.Errorf("temperature = %v", req["temperature"])
	}
	msgs := req["messages"].([]interface{})
	if len(msgs) != 1 {
		t.Fatalf("messages len = %d", len(msgs))
	}
	m0 := msgs[0].(map[string]interface{})
	if m0["role"] != "user" {
		t.Errorf("role = %v", m0["role"])
	}
	content := m0["content"].([]interface{})
	if content[0].(map[string]interface{})["text"] != "Hello" {
		t.Errorf("content = %v", content)
	}
	if res.ThinkingEnabled {
		t.Error("thinking should not be enabled")
	}
}

func TestResponsesRequestToMessages_DeveloperToSystem(t *testing.T) {
	raw := []byte(`{
		"model": "m",
		"instructions": "inst",
		"input": [
			{"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "dev rules"}]},
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}
		]
	}`)

	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)

	if req["system"] != "inst\ndev rules" {
		t.Errorf("system = %v", req["system"])
	}
	msgs := req["messages"].([]interface{})
	if len(msgs) != 1 || msgs[0].(map[string]interface{})["role"] != "user" {
		t.Errorf("messages = %v", msgs)
	}
}

func TestResponsesRequestToMessages_ToolCallRoundTrip(t *testing.T) {
	raw := []byte(`{
		"model": "m",
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "weather?"}]},
			{"type": "function_call", "call_id": "call_abc", "name": "get_weather", "arguments": "{\"city\":\"BJ\"}"},
			{"type": "function_call_output", "call_id": "call_abc", "output": "sunny"}
		]
	}`)

	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)
	msgs := req["messages"].([]interface{})

	// user, assistant(tool_use), user(tool_result)
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d, want 3", len(msgs))
	}
	if msgs[0].(map[string]interface{})["role"] != "user" {
		t.Errorf("msgs[0] role = %v", msgs[0].(map[string]interface{})["role"])
	}
	if msgs[1].(map[string]interface{})["role"] != "assistant" {
		t.Errorf("msgs[1] role = %v", msgs[1].(map[string]interface{})["role"])
	}
	if msgs[2].(map[string]interface{})["role"] != "user" {
		t.Errorf("msgs[2] role = %v", msgs[2].(map[string]interface{})["role"])
	}
}

func TestResponsesRequestToMessages_ToolUseBlockFields(t *testing.T) {
	raw := []byte(`{
		"model": "m",
		"input": [
			{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "checking"}]},
			{"type": "function_call", "call_id": "call_abc", "name": "get_weather", "arguments": "{\"city\":\"BJ\"}"},
			{"type": "function_call_output", "call_id": "call_abc", "output": "sunny"}
		]
	}`)

	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)
	msgs := req["messages"].([]interface{})

	// assistant(text+tool_use) then user(tool_result); a placeholder user prepended.
	if msgs[0].(map[string]interface{})["role"] != "user" {
		t.Fatalf("first role = %v, want user placeholder", msgs[0].(map[string]interface{})["role"])
	}
	assistant := msgs[1].(map[string]interface{})
	blocks := assistant["content"].([]interface{})
	var toolUse map[string]interface{}
	for _, b := range blocks {
		bm := b.(map[string]interface{})
		if bm["type"] == "tool_use" {
			toolUse = bm
		}
	}
	if toolUse == nil {
		t.Fatalf("no tool_use block in %v", blocks)
	}
	if toolUse["id"] != "toolu_abc" || toolUse["name"] != "get_weather" {
		t.Errorf("tool_use = %v", toolUse)
	}
	input := toolUse["input"].(map[string]interface{})
	if input["city"] != "BJ" {
		t.Errorf("input = %v", input)
	}

	user := msgs[2].(map[string]interface{})
	ublocks := user["content"].([]interface{})
	tr := ublocks[0].(map[string]interface{})
	if tr["type"] != "tool_result" || tr["tool_use_id"] != "toolu_abc" || tr["content"] != "sunny" {
		t.Errorf("tool_result = %v", tr)
	}
}

func TestResponsesRequestToMessages_ReasoningRoundTrip(t *testing.T) {
	raw := []byte(`{
		"model": "m",
		"input": [
			{"type": "message", "role": "user", "content": "q"},
			{"type": "reasoning", "summary": [{"type": "summary_text", "text": "thought"}], "encrypted_content": "sig123"},
			{"type": "reasoning", "summary": [{"type": "summary_text", "text": "no sig, dropped"}]},
			{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "a"}]}
		]
	}`)

	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)
	msgs := req["messages"].([]interface{})

	if len(msgs) != 2 {
		t.Fatalf("messages len = %d", len(msgs))
	}
	assistant := msgs[1].(map[string]interface{})
	blocks := assistant["content"].([]interface{})
	if len(blocks) != 2 {
		t.Fatalf("assistant blocks = %d, want thinking+text", len(blocks))
	}
	first := blocks[0].(map[string]interface{})
	if first["type"] != "thinking" || first["thinking"] != "thought" || first["signature"] != "sig123" {
		t.Errorf("thinking block = %v", first)
	}
}

func TestResponsesRequestToMessages_ToolsAndChoice(t *testing.T) {
	raw := []byte(`{
		"model": "m",
		"input": "hi",
		"tools": [
			{"type": "function", "name": "f1", "description": "d", "parameters": {"type": "object", "properties": {"x": {"type": "string"}}}, "strict": true},
			{"type": "web_search"}
		],
		"tool_choice": {"type": "function", "name": "f1"},
		"parallel_tool_calls": false
	}`)

	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)

	tools := req["tools"].([]interface{})
	if len(tools) != 1 {
		t.Fatalf("tools len = %d (builtin should be dropped)", len(tools))
	}
	tool := tools[0].(map[string]interface{})
	if tool["name"] != "f1" || tool["description"] != "d" {
		t.Errorf("tool = %v", tool)
	}
	if _, ok := tool["input_schema"].(map[string]interface{}); !ok {
		t.Errorf("input_schema missing in %v", tool)
	}
	if _, ok := tool["strict"]; ok {
		t.Error("strict should be dropped")
	}

	tc := req["tool_choice"].(map[string]interface{})
	if tc["type"] != "tool" || tc["name"] != "f1" {
		t.Errorf("tool_choice = %v", tc)
	}

	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "built-in tool") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected builtin-drop warning, got %v", res.Warnings)
	}
}

func TestResponsesRequestToMessages_ParallelToolCallsFalse(t *testing.T) {
	raw := []byte(`{"model": "m", "input": "hi", "tool_choice": "auto", "parallel_tool_calls": false}`)
	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)
	tc := req["tool_choice"].(map[string]interface{})
	if tc["type"] != "auto" || tc["disable_parallel_tool_use"] != true {
		t.Errorf("tool_choice = %v", tc)
	}
}

func TestResponsesRequestToMessages_ThinkingDefaults(t *testing.T) {
	raw := []byte(`{
		"model": "m",
		"input": "hi",
		"reasoning": {"effort": "medium"},
		"temperature": 0.9,
		"tool_choice": "required"
	}`)

	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	if !res.ThinkingEnabled {
		t.Fatal("thinking should be enabled")
	}
	req := toMap(t, res.Body)

	thinking := req["thinking"].(map[string]interface{})
	if thinking["type"] != "enabled" || thinking["budget_tokens"].(float64) != 4096 {
		t.Errorf("thinking = %v", thinking)
	}
	if req["max_tokens"].(float64) != 4096+DefaultMessagesMaxTokens {
		t.Errorf("max_tokens = %v, want budget+default", req["max_tokens"])
	}
	if _, ok := req["temperature"]; ok {
		t.Error("temperature should be stripped with thinking")
	}
	tc := req["tool_choice"].(map[string]interface{})
	if tc["type"] != "auto" {
		t.Errorf("tool_choice = %v, want downgraded auto", tc)
	}
}

func TestResponsesRequestToMessages_ThinkingClampAndDisable(t *testing.T) {
	// budget clamps to max_output_tokens - MinThinkingBudget
	raw := []byte(`{"model": "m", "input": "hi", "reasoning": {"effort": "high"}, "max_output_tokens": 8000}`)
	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)
	thinking := req["thinking"].(map[string]interface{})
	if thinking["budget_tokens"].(float64) != 8000-MinThinkingBudget {
		t.Errorf("budget = %v, want clamped %d", thinking["budget_tokens"], 8000-MinThinkingBudget)
	}
	if req["max_tokens"].(float64) != 8000 {
		t.Errorf("max_tokens = %v", req["max_tokens"])
	}

	// max too small to fit even minimum budget + output: thinking disabled
	raw2 := []byte(`{"model": "m", "input": "hi", "reasoning": {"effort": "high"}, "max_output_tokens": 1500}`)
	res2, err := ResponsesRequestToMessages(raw2, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	if res2.ThinkingEnabled {
		t.Error("thinking should be disabled when budget cannot fit")
	}
	req2 := toMap(t, res2.Body)
	if _, ok := req2["thinking"]; ok {
		t.Error("thinking field should be absent")
	}
	if req2["max_tokens"].(float64) != 1500 {
		t.Errorf("max_tokens = %v", req2["max_tokens"])
	}
	found := false
	for _, w := range res2.Warnings {
		if strings.Contains(w, "thinking disabled") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected thinking-disabled warning, got %v", res2.Warnings)
	}
}

func TestResponsesRequestToMessages_MetadataUserID(t *testing.T) {
	raw := []byte(`{"model": "m", "input": "hi", "metadata": {"user_id": "u1", "other": "x"}, "store": false, "previous_response_id": "resp_1"}`)
	res, err := ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages: %v", err)
	}
	req := toMap(t, res.Body)
	md := req["metadata"].(map[string]interface{})
	if md["user_id"] != "u1" || len(md) != 1 {
		t.Errorf("metadata = %v", md)
	}
	for _, k := range []string{"store", "previous_response_id", "include"} {
		if _, ok := req[k]; ok {
			t.Errorf("%s should be dropped", k)
		}
	}
}

func TestMessagesResponseToResponses_Blocks(t *testing.T) {
	anthropic := []byte(`{
		"id": "msg_01XYZ",
		"type": "message",
		"role": "assistant",
		"model": "claude-sonnet-4-20250514",
		"content": [
			{"type": "thinking", "thinking": "hmm", "signature": "sig"},
			{"type": "text", "text": "answer"},
			{"type": "tool_use", "id": "toolu_01ABC", "name": "get_weather", "input": {"city": "BJ"}}
		],
		"stop_reason": "tool_use",
		"usage": {"input_tokens": 10, "output_tokens": 20, "cache_read_input_tokens": 5, "cache_creation_input_tokens": 3}
	}`)

	res, err := MessagesResponseToResponses(anthropic, "claude-alias")
	if err != nil {
		t.Fatalf("MessagesResponseToResponses: %v", err)
	}
	resp := toMap(t, res.Body)

	if resp["id"] != "resp_01XYZ" {
		t.Errorf("id = %v", resp["id"])
	}
	if resp["model"] != "claude-alias" {
		t.Errorf("model = %v, want client-facing param", resp["model"])
	}
	if resp["status"] != "completed" {
		t.Errorf("status = %v", resp["status"])
	}

	output := resp["output"].([]interface{})
	if len(output) != 3 {
		t.Fatalf("output len = %d", len(output))
	}
	reasoning := output[0].(map[string]interface{})
	if reasoning["type"] != "reasoning" || reasoning["encrypted_content"] != "sig" {
		t.Errorf("reasoning = %v", reasoning)
	}
	summary := reasoning["summary"].([]interface{})
	if summary[0].(map[string]interface{})["text"] != "hmm" {
		t.Errorf("summary = %v", summary)
	}
	msg := output[1].(map[string]interface{})
	if msg["type"] != "message" || msg["status"] != "completed" {
		t.Errorf("message = %v", msg)
	}
	fc := output[2].(map[string]interface{})
	if fc["type"] != "function_call" || fc["call_id"] != "toolu_01ABC" || fc["name"] != "get_weather" {
		t.Errorf("function_call = %v", fc)
	}
	if fc["arguments"] != `{"city":"BJ"}` {
		t.Errorf("arguments = %v", fc["arguments"])
	}

	usage := resp["usage"].(map[string]interface{})
	// input normalized: 10 + 5 + 3
	if usage["input_tokens"].(float64) != 18 {
		t.Errorf("input_tokens = %v", usage["input_tokens"])
	}
	if usage["output_tokens"].(float64) != 20 {
		t.Errorf("output_tokens = %v", usage["output_tokens"])
	}
	details := usage["input_tokens_details"].(map[string]interface{})
	if details["cached_tokens"].(float64) != 5 {
		t.Errorf("cached_tokens = %v", details)
	}

	if res.Usage.InputTokens != 18 || res.Usage.OutputTokens != 20 {
		t.Errorf("result usage = %+v", res.Usage)
	}
	if res.CachedTokens != 5 || res.CacheCreationTokens != 3 {
		t.Errorf("cache tokens = %d/%d", res.CachedTokens, res.CacheCreationTokens)
	}
}

func TestMessagesResponseToResponses_MaxTokensIncomplete(t *testing.T) {
	anthropic := []byte(`{
		"id": "msg_1",
		"content": [{"type": "text", "text": "partial"}],
		"stop_reason": "max_tokens",
		"usage": {"input_tokens": 1, "output_tokens": 2}
	}`)
	res, err := MessagesResponseToResponses(anthropic, "m")
	if err != nil {
		t.Fatalf("MessagesResponseToResponses: %v", err)
	}
	resp := toMap(t, res.Body)
	if resp["status"] != "incomplete" {
		t.Errorf("status = %v", resp["status"])
	}
	details := resp["incomplete_details"].(map[string]interface{})
	if details["reason"] != "max_output_tokens" {
		t.Errorf("incomplete_details = %v", details)
	}
}

func TestMessagesResponseToResponses_ErrorPassthrough(t *testing.T) {
	anthropic := []byte(`{"type": "error", "error": {"type": "invalid_request_error", "message": "bad"}}`)
	res, err := MessagesResponseToResponses(anthropic, "m")
	if err != nil {
		t.Fatalf("MessagesResponseToResponses: %v", err)
	}
	resp := toMap(t, res.Body)
	errObj := resp["error"].(map[string]interface{})
	if errObj["message"] != "bad" || errObj["type"] != "invalid_request_error" {
		t.Errorf("error = %v", errObj)
	}
}

func TestMessagesErrorToResponses(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantType string
		wantMsg  string
		wantOK   bool
	}{
		{"overloaded mapped", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, "server_error", "Overloaded", true},
		{"api error mapped", `{"type":"error","error":{"type":"api_error","message":"boom"}}`, "server_error", "boom", true},
		{"rate limit kept", `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`, "rate_limit_error", "slow down", true},
		{"invalid kept", `{"type":"error","error":{"type":"invalid_request_error","message":"bad param"}}`, "invalid_request_error", "bad param", true},
		{"not an error", `{"id":"msg_1","content":[]}`, "", "", false},
		{"garbage", `not json`, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, ok := MessagesErrorToResponses([]byte(tc.body))
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			m := toMap(t, out)
			errObj := m["error"].(map[string]interface{})
			if errObj["type"] != tc.wantType || errObj["message"] != tc.wantMsg {
				t.Errorf("error = %v", errObj)
			}
		})
	}
}

func TestResponsesRequestToMessages_MaxOutputTokens_DefaultingAndClamping(t *testing.T) {
	// Case 1: No max_output_tokens -> defaults to passed maxOutputTokens (4096)
	raw1 := []byte(`{"model":"m","instructions":"hi","input":"hello"}`)
	res1, err := ResponsesRequestToMessages(raw1, "claude-sonnet-4-20250514", 4096)
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages case 1: %v", err)
	}
	req1 := toMap(t, res1.Body)
	if req1["max_tokens"].(float64) != 4096 {
		t.Errorf("max_tokens = %v, want 4096", req1["max_tokens"])
	}

	// Case 2: Client sends max_output_tokens: 32000 > maxOutputTokens: 8192 -> clamped to 8192
	raw2 := []byte(`{"model":"m","instructions":"hi","input":"hello","max_output_tokens":32000}`)
	res2, err := ResponsesRequestToMessages(raw2, "claude-sonnet-4-20250514", 8192)
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages case 2: %v", err)
	}
	req2 := toMap(t, res2.Body)
	if req2["max_tokens"].(float64) != 8192 {
		t.Errorf("max_tokens = %v, want clamped 8192", req2["max_tokens"])
	}

	// Case 3: Client sends max_output_tokens: 1000 < maxOutputTokens: 8192 -> keeps 1000
	raw3 := []byte(`{"model":"m","instructions":"hi","input":"hello","max_output_tokens":1000}`)
	res3, err := ResponsesRequestToMessages(raw3, "claude-sonnet-4-20250514", 8192)
	if err != nil {
		t.Fatalf("ResponsesRequestToMessages case 3: %v", err)
	}
	req3 := toMap(t, res3.Body)
	if req3["max_tokens"].(float64) != 1000 {
		t.Errorf("max_tokens = %v, want 1000", req3["max_tokens"])
	}
}
