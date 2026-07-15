package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// TestJoyCode_TranslateOpenAIToAnthropic 测试 OpenAI 到 Anthropic 的请求体翻译，包括连续 tool 消息的合并
func TestJoyCode_TranslateOpenAIToAnthropic(t *testing.T) {
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

	rawBytes, err := json.Marshal(oaiReq)
	if err != nil {
		t.Fatalf("marshal oaiReq failed: %v", err)
	}

	anthropicBytes, err := translateOpenAIToAnthropic(rawBytes, "claude-3-5-sonnet")
	if err != nil {
		t.Fatalf("translateOpenAIToAnthropic failed: %v", err)
	}

	var aReq map[string]interface{}
	if err := json.Unmarshal(anthropicBytes, &aReq); err != nil {
		t.Fatalf("unmarshal anthropicBytes failed: %v", err)
	}

	// 1. 校验 Model
	if aReq["model"] != "claude-3-5-sonnet" {
		t.Errorf("expected model claude-3-5-sonnet, got %v", aReq["model"])
	}

	// 2. 校验 System
	if aReq["system"] != "System system." {
		t.Errorf("expected system System system., got %v", aReq["system"])
	}

	// 3. 校验 Messages
	msgs, ok := aReq["messages"].([]interface{})
	if !ok {
		t.Fatalf("messages is not array")
	}

	// OpenAI 消息被提取了 system 剩余 4 条：user, assistant (含 tool_calls), tool_1, tool_2
	// 在翻译中：
	// - user: -> role user (1条)
	// - assistant: -> role assistant (1条)
	// - tool_1, tool_2: -> 连续的 tool_result 块被合并为一条 user 消息！
	// 所以总 messages 数应该为 3 条！
	if len(msgs) != 3 {
		t.Fatalf("expected messages length 3, got %d", len(msgs))
	}

	// 验证 assistant 里面的 tool_use 转换
	assistantMsg := msgs[1].(map[string]interface{})
	if assistantMsg["role"] != "assistant" {
		t.Errorf("expected role assistant, got %v", assistantMsg["role"])
	}
	contentArr := assistantMsg["content"].([]interface{})
	if len(contentArr) != 2 {
		t.Fatalf("expected assistant content blocks size 2 (text + tool_use), got %d", len(contentArr))
	}
	toolUseBlock := contentArr[1].(map[string]interface{})
	if toolUseBlock["type"] != "tool_use" || toolUseBlock["id"] != "call_1" || toolUseBlock["name"] != "get_weather" {
		t.Errorf("invalid tool_use block conversion: %+v", toolUseBlock)
	}

	// 验证连续 tool_result 的合并
	mergedUserMsg := msgs[2].(map[string]interface{})
	if mergedUserMsg["role"] != "user" {
		t.Errorf("expected role user for merged tool results, got %v", mergedUserMsg["role"])
	}
	toolResultsArr := mergedUserMsg["content"].([]interface{})
	if len(toolResultsArr) != 2 {
		t.Fatalf("expected 2 tool_result blocks in merged user content, got %d", len(toolResultsArr))
	}
	tr1 := toolResultsArr[0].(map[string]interface{})
	tr2 := toolResultsArr[1].(map[string]interface{})
	if tr1["type"] != "tool_result" || tr1["tool_use_id"] != "call_1" || tr1["content"] != "Sunny 25C" {
		t.Errorf("invalid tool_result 1 conversion: %+v", tr1)
	}
	if tr2["type"] != "tool_result" || tr2["tool_use_id"] != "call_2" || tr2["content"] != "Windy 3级" {
		t.Errorf("invalid tool_result 2 conversion: %+v", tr2)
	}
}

// TestJoyCode_TranslateAnthropicToOpenAINonStream 测试 Anthropic 非流式响应翻译回 OpenAI
func TestJoyCode_TranslateAnthropicToOpenAINonStream(t *testing.T) {
	anthropicResp := map[string]interface{}{
		"id":    "msg_123",
		"type":  "message",
		"role":  "assistant",
		"model": "claude-3-5-sonnet",
		"content": []interface{}{
			map[string]interface{}{"type": "text", "text": "Hello, world!"},
		},
		"stop_reason": "end_turn",
		"usage": map[string]interface{}{
			"input_tokens":  15.0,
			"output_tokens": 8.0,
		},
	}

	rawBytes, err := json.Marshal(anthropicResp)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	openaiBytes, err := translateAnthropicToOpenAINonStream(rawBytes, "claude-3-5-sonnet")
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}

	var oResp map[string]interface{}
	if err := json.Unmarshal(openaiBytes, &oResp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if oResp["id"] != "msg_123" {
		t.Errorf("expected id msg_123, got %v", oResp["id"])
	}
	choices := oResp["choices"].([]interface{})
	if len(choices) != 1 {
		t.Fatalf("expected choices size 1, got %d", len(choices))
	}
	firstChoice := choices[0].(map[string]interface{})
	msg := firstChoice["message"].(map[string]interface{})
	if msg["content"] != "Hello, world!" {
		t.Errorf("expected content Hello, world!, got %v", msg["content"])
	}

	usage := oResp["usage"].(map[string]interface{})
	if usage["prompt_tokens"].(float64) != 15.0 || usage["completion_tokens"].(float64) != 8.0 || usage["total_tokens"].(float64) != 23.0 {
		t.Errorf("invalid usage stats: %+v", usage)
	}
}

// TestJoyCode_HandleAnthropicStreamToOpenAI 测试流式翻译状态机 (Anthropic -> OpenAI SSE)
func TestJoyCode_HandleAnthropicStreamToOpenAI(t *testing.T) {
	sseContent := `
data: {"type": "message_start", "message": {"id": "msg_001", "usage": {"input_tokens": 10}}}

data: {"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "Let me think."}}

data: {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Answer is 42."}}

data: {"type": "message_delta", "usage": {"output_tokens": 20}}

data: {"type": "message_stop"}

data: [DONE]
`

	respBody := io.NopCloser(strings.NewReader(sseContent))
	httpResp := &http.Response{
		Body: respBody,
	}

	recorder := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            context.Background(),
		ResponseWriter: recorder,
		Model:          "claude-3-5-sonnet",
		IsStream:       true,
	}

	p := &JoyCodeProvider{
		name: "test-joycode",
	}

	err := p.handleAnthropicStreamToOpenAI(gctx, httpResp)
	if err != nil {
		t.Fatalf("handleAnthropicStreamToOpenAI failed: %v", err)
	}

	resultBody := recorder.Body.String()
	lines := strings.Split(resultBody, "\n")

	var openaiEvents []map[string]interface{}
	hasDone := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "data: [DONE]" {
			hasDone = true
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataStr := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			var val map[string]interface{}
			if err := json.Unmarshal([]byte(dataStr), &val); err == nil {
				openaiEvents = append(openaiEvents, val)
			}
		}
	}

	// 验证 Token 累计抓取
	if gctx.InputTokens != 10 {
		t.Errorf("expected InputTokens 10, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 20 {
		t.Errorf("expected OutputTokens 20, got %d", gctx.OutputTokens)
	}

	// 应当有 3 个翻译事件：1. thinking_delta, 2. text_delta, 3. message_delta (finish_reason)
	if len(openaiEvents) != 3 {
		t.Fatalf("expected 3 OpenAI events translated, got %d: %+v", len(openaiEvents), openaiEvents)
	}

	// 验证 thinking_delta
	ev1Choices := openaiEvents[0]["choices"].([]interface{})
	ev1Delta := ev1Choices[0].(map[string]interface{})["delta"].(map[string]interface{})
	if ev1Delta["reasoning_content"] != "Let me think." {
		t.Errorf("expected reasoning_content, got %v", ev1Delta["reasoning_content"])
	}

	// 验证 text_delta
	ev2Choices := openaiEvents[1]["choices"].([]interface{})
	ev2Delta := ev2Choices[0].(map[string]interface{})["delta"].(map[string]interface{})
	if ev2Delta["content"] != "Answer is 42." {
		t.Errorf("expected content, got %v", ev2Delta["content"])
	}

	// 验证 message_delta (finish_reason)
	ev3Choices := openaiEvents[2]["choices"].([]interface{})
	ev3Choice := ev3Choices[0].(map[string]interface{})
	if ev3Choice["finish_reason"] != "stop" {
		t.Errorf("expected finish_reason stop, got %v", ev3Choice["finish_reason"])
	}

	if !hasDone {
		t.Errorf("expected stream to end with [DONE]")
	}
}
