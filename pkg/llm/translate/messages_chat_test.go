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

func TestMessagesToChatCompletion_CacheTokens(t *testing.T) {
	anthropic := []byte(`{
		"id": "msg_123",
		"content": [{"type": "text", "text": "hi"}],
		"stop_reason": "end_turn",
		"usage": {"input_tokens": 10, "output_tokens": 7, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 50}
	}`)

	res, err := MessagesToChatCompletion(anthropic, "claude-3-5-sonnet")
	if err != nil {
		t.Fatal(err)
	}
	// Normalized total input = 10 + 100 + 50.
	if res.Usage.InputTokens != 160 {
		t.Errorf("InputTokens = %d, want 160", res.Usage.InputTokens)
	}
	if res.Usage.CachedTokens != 100 {
		t.Errorf("CachedTokens = %d, want 100", res.Usage.CachedTokens)
	}
	if res.Usage.CacheCreationTokens != 50 {
		t.Errorf("CacheCreationTokens = %d, want 50", res.Usage.CacheCreationTokens)
	}

	var oResp map[string]interface{}
	_ = json.Unmarshal(res.Body, &oResp)
	usage := oResp["usage"].(map[string]interface{})
	if usage["prompt_tokens"].(float64) != 160 {
		t.Errorf("prompt_tokens = %v, want 160", usage["prompt_tokens"])
	}
	details := usage["prompt_tokens_details"].(map[string]interface{})
	if details["cached_tokens"].(float64) != 100 {
		t.Errorf("cached_tokens = %v, want 100", details["cached_tokens"])
	}
}

func TestMessagesRequestToChat_ImageBlock(t *testing.T) {
	raw := []byte(`{
		"model": "glm-4v",
		"messages": [{
			"role": "user",
			"content": [
				{"type": "text", "text": "What is this?"},
				{"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "AAAA"}}
			]
		}]
	}`)

	// OfficialOrTest true so degradeMessagesToTextOnly does not run.
	out, err := MessagesRequestToChat(raw, MessagesToChatOptions{OfficialOrTest: true})
	if err != nil {
		t.Fatalf("MessagesRequestToChat: %v", err)
	}

	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	msgs := req["messages"].([]interface{})
	user := msgs[len(msgs)-1].(map[string]interface{})
	parts, ok := user["content"].([]interface{})
	if !ok {
		t.Fatalf("expected multimodal content array, got %T", user["content"])
	}
	var sawText, sawImage bool
	for _, p := range parts {
		pm := p.(map[string]interface{})
		switch pm["type"] {
		case "text":
			sawText = true
		case "image_url":
			sawImage = true
			iu := pm["image_url"].(map[string]interface{})
			if iu["url"] != "data:image/jpeg;base64,AAAA" {
				t.Errorf("image url = %v", iu["url"])
			}
		}
	}
	if !sawText || !sawImage {
		t.Errorf("expected both text and image parts, sawText=%v sawImage=%v", sawText, sawImage)
	}
}

func TestMessagesRequestToChat_ImageBlockCompat(t *testing.T) {
	// Compat path (OfficialOrTest=false) routes through degradeMessagesToTextOnly.
	// The image message must survive rather than being flattened to an empty string.
	raw := []byte(`{
		"model": "glm-4v",
		"messages": [{
			"role": "user",
			"content": [
				{"type": "text", "text": "describe"},
				{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "BBBB"}}
			]
		}]
	}`)

	out, err := MessagesRequestToChat(raw, MessagesToChatOptions{OfficialOrTest: false})
	if err != nil {
		t.Fatalf("MessagesRequestToChat: %v", err)
	}
	var req map[string]interface{}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	msgs := req["messages"].([]interface{})
	user := msgs[len(msgs)-1].(map[string]interface{})
	parts, ok := user["content"].([]interface{})
	if !ok {
		t.Fatalf("compat path dropped image: content is %T (%v)", user["content"], user["content"])
	}
	var sawImage bool
	for _, p := range parts {
		if pm, ok := p.(map[string]interface{}); ok && pm["type"] == "image_url" {
			sawImage = true
		}
	}
	if !sawImage {
		t.Error("expected image_url part to survive compat degrade")
	}
}

func TestMessagesToChatCompletion_Thinking(t *testing.T) {
	anthropic := []byte(`{
		"id": "msg_123",
		"content": [
			{"type": "thinking", "thinking": "Let me reason."},
			{"type": "text", "text": "The answer is 42."}
		],
		"stop_reason": "end_turn",
		"usage": {"input_tokens": 10, "output_tokens": 20}
	}`)

	res, err := MessagesToChatCompletion(anthropic, "claude-3-5-sonnet")
	if err != nil {
		t.Fatal(err)
	}
	var oResp map[string]interface{}
	_ = json.Unmarshal(res.Body, &oResp)
	msg := oResp["choices"].([]interface{})[0].(map[string]interface{})["message"].(map[string]interface{})
	if msg["reasoning_content"] != "Let me reason." {
		t.Errorf("reasoning_content = %v", msg["reasoning_content"])
	}
	if msg["content"] != "The answer is 42." {
		t.Errorf("content = %v", msg["content"])
	}
}

func TestChatCompletionToMessages_Reasoning(t *testing.T) {
	chat := []byte(`{
		"id": "chatcmpl-abc",
		"choices": [{
			"message": {
				"role": "assistant",
				"content": "Final answer.",
				"reasoning_content": "Thinking step by step."
			},
			"finish_reason": "stop"
		}],
		"usage": {"prompt_tokens": 5, "completion_tokens": 7}
	}`)

	res, err := ChatCompletionToMessages(chat, "gpt-4")
	if err != nil {
		t.Fatal(err)
	}
	var msg map[string]interface{}
	_ = json.Unmarshal(res.Body, &msg)
	content := msg["content"].([]interface{})
	var sawThinking, sawText bool
	for _, b := range content {
		bm := b.(map[string]interface{})
		switch bm["type"] {
		case "thinking":
			sawThinking = true
			if bm["thinking"] != "Thinking step by step." {
				t.Errorf("thinking = %v", bm["thinking"])
			}
		case "text":
			sawText = true
		}
	}
	if !sawThinking || !sawText {
		t.Errorf("expected thinking+text blocks, sawThinking=%v sawText=%v", sawThinking, sawText)
	}
}
