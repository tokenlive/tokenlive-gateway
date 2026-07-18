package translate

import (
	"encoding/json"
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
	if item["type"] != "function_call" || item["name"] != "fn" {
		t.Errorf("item = %v", item)
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
	if c0["type"] != "text" {
		t.Errorf("content type = %v", c0["type"])
	}
}
