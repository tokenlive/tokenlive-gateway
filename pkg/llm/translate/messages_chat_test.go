package translate

import (
	"encoding/json"
	"testing"
)

func TestMessagesRequestToChat_Official(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-4",
		"system": "You are a helpful assistant",
		"messages": [{"role": "user", "content": "hi"}],
		"max_tokens": 100,
		"temperature": 0.7,
		"thinking": {"type": "adaptive", "budget_tokens": 1024}
	}`)

	out, err := MessagesRequestToChat(raw, MessagesToChatOptions{OfficialOrTest: true})
	if err != nil {
		t.Fatalf("MessagesRequestToChat: %v", err)
	}

	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}

	msgs := req["messages"].([]interface{})
	if len(msgs) != 2 {
		t.Fatalf("messages len = %d", len(msgs))
	}
	first := msgs[0].(map[string]interface{})
	if first["role"] != "system" || first["content"] != "You are a helpful assistant" {
		t.Errorf("system msg = %v", first)
	}
	if _, ok := req["max_tokens"]; ok {
		t.Error("max_tokens should be deleted")
	}
	if req["max_completion_tokens"].(float64) != 100 {
		t.Errorf("max_completion_tokens = %v", req["max_completion_tokens"])
	}
	thinking := req["thinking"].(map[string]interface{})
	if thinking["type"] != "auto" {
		t.Errorf("thinking.type = %v", thinking["type"])
	}
}

func TestChatRequestToMessages_ToolMerge(t *testing.T) {
	oaiReq := map[string]interface{}{
		"messages": []interface{}{
			map[string]interface{}{"role": "system", "content": "System system."},
			map[string]interface{}{"role": "user", "content": "Hello!"},
			map[string]interface{}{"role": "assistant", "content": "Hi there!", "tool_calls": []interface{}{
				map[string]interface{}{
					"id":   "call_1",
					"type": "function",
					"function": map[string]interface{}{
						"name":      "get_weather",
						"arguments": `{"city":"Beijing"}`,
					},
				},
			}},
			map[string]interface{}{"role": "tool", "tool_call_id": "call_1", "content": "Sunny 25C"},
			map[string]interface{}{"role": "tool", "tool_call_id": "call_2", "content": "Windy 3级"},
		},
		"tools": []interface{}{
			map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name":        "get_weather",
					"description": "Get weather info",
					"parameters":  map[string]interface{}{"type": "object"},
				},
			},
		},
		"max_tokens":  2000.0,
		"temperature": 0.7,
	}
	raw, _ := json.Marshal(oaiReq)

	out, err := ChatRequestToMessages(raw, "claude-3-5-sonnet")
	if err != nil {
		t.Fatalf("ChatRequestToMessages: %v", err)
	}

	var aReq map[string]interface{}
	_ = json.Unmarshal(out, &aReq)

	if aReq["model"] != "claude-3-5-sonnet" {
		t.Errorf("model = %v", aReq["model"])
	}
	if aReq["system"] != "System system." {
		t.Errorf("system = %v", aReq["system"])
	}
	msgs := aReq["messages"].([]interface{})
	if len(msgs) != 3 {
		t.Fatalf("messages len = %d", len(msgs))
	}
	merged := msgs[2].(map[string]interface{})
	blocks := merged["content"].([]interface{})
	if len(blocks) != 2 {
		t.Fatalf("tool_result blocks = %d", len(blocks))
	}
}

func TestChatCompletionToMessages_Tools(t *testing.T) {
	chat := []byte(`{
		"id": "chatcmpl-abc",
		"choices": [{
			"message": {
				"role": "assistant",
				"content": "",
				"tool_calls": [{
					"id": "call_xyz",
					"type": "function",
					"function": {"name": "get_weather", "arguments": "{\"city\":\"BJ\"}"}
				}]
			},
			"finish_reason": "tool_calls"
		}],
		"usage": {"prompt_tokens": 10, "completion_tokens": 5}
	}`)

	res, err := ChatCompletionToMessages(chat, "gpt-4")
	if err != nil {
		t.Fatalf("ChatCompletionToMessages: %v", err)
	}
	if res.Usage.InputTokens != 10 || res.Usage.OutputTokens != 5 {
		t.Errorf("usage = %+v", res.Usage)
	}

	var msg map[string]interface{}
	_ = json.Unmarshal(res.Body, &msg)
	if msg["stop_reason"] != "tool_use" {
		t.Errorf("stop_reason = %v", msg["stop_reason"])
	}
	content := msg["content"].([]interface{})
	found := false
	for _, b := range content {
		bm := b.(map[string]interface{})
		if bm["type"] == "tool_use" {
			found = true
			if bm["id"] != "toolu_xyz" {
				t.Errorf("tool id = %v", bm["id"])
			}
			if bm["name"] != "get_weather" {
				t.Errorf("name = %v", bm["name"])
			}
		}
	}
	if !found {
		t.Fatal("expected tool_use block")
	}
}

func TestMessagesToChatCompletion_Tools(t *testing.T) {
	anthropic := []byte(`{
		"id": "msg_123",
		"type": "message",
		"role": "assistant",
		"model": "claude-3-5-sonnet",
		"content": [
			{"type": "text", "text": "Let me check."},
			{"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Beijing"}}
		],
		"stop_reason": "tool_use",
		"usage": {"input_tokens": 15, "output_tokens": 8}
	}`)

	res, err := MessagesToChatCompletion(anthropic, "claude-3-5-sonnet")
	if err != nil {
		t.Fatalf("MessagesToChatCompletion: %v", err)
	}

	var oResp map[string]interface{}
	_ = json.Unmarshal(res.Body, &oResp)

	choices := oResp["choices"].([]interface{})
	choice := choices[0].(map[string]interface{})
	if choice["finish_reason"] != "tool_calls" {
		t.Errorf("finish_reason = %v", choice["finish_reason"])
	}
	msg := choice["message"].(map[string]interface{})
	if msg["content"] != "Let me check." {
		t.Errorf("content = %v", msg["content"])
	}
	tcs := msg["tool_calls"].([]interface{})
	if len(tcs) != 1 {
		t.Fatalf("tool_calls len = %d", len(tcs))
	}
	tc := tcs[0].(map[string]interface{})
	fn := tc["function"].(map[string]interface{})
	if fn["name"] != "get_weather" {
		t.Errorf("name = %v", fn["name"])
	}
	if res.Usage.InputTokens != 15 || res.Usage.OutputTokens != 8 {
		t.Errorf("usage = %+v", res.Usage)
	}
}

func TestMessagesToChatCompletion_TextOnly(t *testing.T) {
	anthropic := []byte(`{
		"id": "msg_123",
		"content": [{"type": "text", "text": "Hello, world!"}],
		"stop_reason": "end_turn",
		"usage": {"input_tokens": 15, "output_tokens": 8}
	}`)

	res, err := MessagesToChatCompletion(anthropic, "claude-3-5-sonnet")
	if err != nil {
		t.Fatal(err)
	}
	var oResp map[string]interface{}
	_ = json.Unmarshal(res.Body, &oResp)
	choice := oResp["choices"].([]interface{})[0].(map[string]interface{})
	if choice["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %v", choice["finish_reason"])
	}
	msg := choice["message"].(map[string]interface{})
	if msg["content"] != "Hello, world!" {
		t.Errorf("content = %v", msg["content"])
	}
}
