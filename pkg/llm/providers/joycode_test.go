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

// TestJoyCode_HandleAnthropicStreamToOpenAI 测试流式翻译 (Anthropic -> OpenAI SSE)，含 text/thinking
func TestJoyCode_HandleAnthropicStreamToOpenAI(t *testing.T) {
	sseContent := "data: {\"type\": \"message_start\", \"message\": {\"id\": \"msg_001\", \"usage\": {\"input_tokens\": 10}}}\n\n" +
		"data: {\"type\": \"content_block_delta\", \"index\": 0, \"delta\": {\"type\": \"thinking_delta\", \"thinking\": \"Let me think.\"}}\n\n" +
		"data: {\"type\": \"content_block_delta\", \"index\": 0, \"delta\": {\"type\": \"text_delta\", \"text\": \"Answer is 42.\"}}\n\n" +
		"data: {\"type\": \"message_delta\", \"delta\": {\"stop_reason\": \"end_turn\"}, \"usage\": {\"output_tokens\": 20}}\n\n" +
		"data: {\"type\": \"message_stop\"}\n\n"

	respBody := io.NopCloser(strings.NewReader(sseContent))
	httpResp := &http.Response{Body: respBody}

	recorder := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            context.Background(),
		ResponseWriter: recorder,
		Model:          "claude-3-5-sonnet",
		IsStream:       true,
	}

	p := &JoyCodeProvider{name: "test-joycode"}
	if err := p.handleAnthropicStreamToOpenAI(gctx, httpResp); err != nil {
		t.Fatalf("handleAnthropicStreamToOpenAI failed: %v", err)
	}

	openaiEvents, hasDone := parseSSEOpenAIEvents(t, recorder.Body.String())

	if gctx.InputTokens != 10 {
		t.Errorf("expected InputTokens 10, got %d", gctx.InputTokens)
	}
	if gctx.OutputTokens != 20 {
		t.Errorf("expected OutputTokens 20, got %d", gctx.OutputTokens)
	}
	if len(openaiEvents) != 3 {
		t.Fatalf("expected 3 OpenAI events translated, got %d: %+v", len(openaiEvents), openaiEvents)
	}

	ev1Delta := openaiEvents[0]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	if ev1Delta["reasoning_content"] != "Let me think." {
		t.Errorf("expected reasoning_content, got %v", ev1Delta["reasoning_content"])
	}
	ev2Delta := openaiEvents[1]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	if ev2Delta["content"] != "Answer is 42." {
		t.Errorf("expected content, got %v", ev2Delta["content"])
	}
	ev3Choice := openaiEvents[2]["choices"].([]interface{})[0].(map[string]interface{})
	if ev3Choice["finish_reason"] != "stop" {
		t.Errorf("expected finish_reason stop, got %v", ev3Choice["finish_reason"])
	}
	if !hasDone {
		t.Errorf("expected stream to end with [DONE]")
	}
}

// TestJoyCode_HandleAnthropicStreamToOpenAI_Tools 验证 tool_use 流翻译为 OpenAI tool_calls
func TestJoyCode_HandleAnthropicStreamToOpenAI_Tools(t *testing.T) {
	sseContent := "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_tool\",\"usage\":{\"input_tokens\":5}}}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"get_weather\",\"input\":{}}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\"}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"BJ\\\"}\"}}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":8}}\n\n" +
		"data: {\"type\":\"message_stop\"}\n\n"

	recorder := httptest.NewRecorder()
	gctx := &core.GatewayContext{
		Ctx:            context.Background(),
		ResponseWriter: recorder,
		Model:          "claude-3-5-sonnet",
		IsStream:       true,
	}
	p := &JoyCodeProvider{name: "test-joycode"}
	if err := p.handleAnthropicStreamToOpenAI(gctx, &http.Response{Body: io.NopCloser(strings.NewReader(sseContent))}); err != nil {
		t.Fatalf("stream tools: %v", err)
	}

	events, hasDone := parseSSEOpenAIEvents(t, recorder.Body.String())
	if !hasDone {
		t.Fatal("expected [DONE]")
	}
	if len(events) < 4 {
		t.Fatalf("expected >=4 events, got %d: %+v", len(events), events)
	}

	// first: tool start
	d0 := events[0]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	tc := d0["tool_calls"].([]interface{})[0].(map[string]interface{})
	if tc["id"] != "toolu_1" {
		t.Errorf("id = %v", tc["id"])
	}
	if tc["function"].(map[string]interface{})["name"] != "get_weather" {
		t.Errorf("name = %v", tc["function"])
	}

	// finish
	last := events[len(events)-1]["choices"].([]interface{})[0].(map[string]interface{})
	if last["finish_reason"] != "tool_calls" {
		t.Errorf("finish_reason = %v", last["finish_reason"])
	}
	if gctx.OutputTokens != 8 {
		t.Errorf("OutputTokens = %d", gctx.OutputTokens)
	}
}

func parseSSEOpenAIEvents(t *testing.T, body string) (events []map[string]interface{}, hasDone bool) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
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
				events = append(events, val)
			}
		}
	}
	return events, hasDone
}

func TestJoyCode_SignGatewayURL(t *testing.T) {
	// 1. Missing environment variables should return an error
	t.Setenv("JOYCODE_APPID", "")
	t.Setenv("JOYCODE_SIGN_KEY", "")
	_, err := signJoyCodeGatewayURL("https://joycode-api-saas.jd.com", "chat_completions")
	if err == nil {
		t.Fatal("expected error when JOYCODE_APPID and JOYCODE_SIGN_KEY are missing")
	}

	// 2. Setting environment variables should succeed and return a signed URL
	t.Setenv("JOYCODE_APPID", "joycode_ide")
	t.Setenv("JOYCODE_SIGN_KEY", "0691a3f0b37b4a85aeb63ad0fc7db3ed")

	urlStr, err := signJoyCodeGatewayURL("https://joycode-api-saas.jd.com", "chat_completions")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(urlStr, "appid=joycode_ide") {
		t.Errorf("expected URL to contain appid=joycode_ide, got: %s", urlStr)
	}
	if !strings.Contains(urlStr, "sign=") {
		t.Errorf("expected URL to contain sign, got: %s", urlStr)
	}
}

func TestJoyCodeMessages_ProbeNonStream(t *testing.T) {
	p := &JoyCodeProvider{
		name:    "test-joycode",
		baseURL: "http://localhost:1234",
		apiKey:  "test-key",
	}

	reqBody := `{"model": "claude-3-5-sonnet", "messages": [{"role": "user", "content": "."}], "max_tokens": 1}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "claude-3-5-sonnet"
	gctx.IsStream = false

	invoker := &joycodeMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(gctx.UpstreamBody, &resp); err != nil {
		t.Fatalf("failed to unmarshal probe response: %v", err)
	}
	if resp["type"] != "message" || resp["model"] != "claude-3-5-sonnet" {
		t.Errorf("unexpected probe response: %v", resp)
	}
}

func TestJoyCodeMessages_ProbeStream(t *testing.T) {
	p := &JoyCodeProvider{
		name:    "test-joycode",
		baseURL: "http://localhost:1234",
		apiKey:  "test-key",
	}

	reqBody := `{"model": "claude-3-5-sonnet", "messages": [{"role": "user", "content": "."}], "max_tokens": 1, "stream": true}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "claude-3-5-sonnet"
	gctx.IsStream = true

	invoker := &joycodeMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	body := w.Body.String()
	if !strings.Contains(body, "event: message_start") || !strings.Contains(body, "event: message_stop") {
		t.Errorf("expected stream events, got %s", body)
	}
}

func TestJoyCode_CleanThinkingInHistory(t *testing.T) {
	reqBody := `{
		"model": "claude-3-5-sonnet",
		"messages": [
			{
				"role": "user",
				"content": "Hello!"
			},
			{
				"role": "assistant",
				"content": [
					{
						"type": "thinking",
						"thinking": "Thinking process...",
						"signature": "some_sig"
					},
					{
						"type": "text",
						"text": "Hello, how can I help you?"
					}
				]
			}
		]
	}`

	cleaned := cleanThinkingInHistory([]byte(reqBody))

	var m map[string]interface{}
	if err := json.Unmarshal(cleaned, &m); err != nil {
		t.Fatalf("failed to unmarshal cleaned body: %v", err)
	}

	msgs := m["messages"].([]interface{})
	assistantMsg := msgs[1].(map[string]interface{})
	contentArr := assistantMsg["content"].([]interface{})

	if len(contentArr) != 1 {
		t.Fatalf("expected content block size 1, got %d", len(contentArr))
	}

	block := contentArr[0].(map[string]interface{})
	if block["type"] != "text" || block["text"] != "Hello, how can I help you?" {
		t.Errorf("unexpected content block left: %+v", block)
	}
}

func TestJoyCodeMessages_GLM5_NonAnthropicModelStream(t *testing.T) {
	// Mock upstream JoyCode OpenAI endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/api/saas/openai/v2/chat/completions") {
			t.Errorf("expected OpenAI endpoint path, got: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"id\":\"chatcmpl-glm\",\"model\":\"glm-5.1\",\"choices\":[{\"delta\":{\"reasoning_content\":\"Thinking GLM5...\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"id\":\"chatcmpl-glm\",\"model\":\"glm-5.1\",\"choices\":[{\"delta\":{\"content\":\"Hello from GLM5.1\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	p := &JoyCodeProvider{
		name:    "test-joycode",
		baseURL: server.URL,
		apiKey:  "test-key",
		client:  server.Client(),
	}

	reqBody := `{"model": "glm-5.1", "messages": [{"role": "user", "content": "Hi"}], "max_tokens": 100, "stream": true}`
	req := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	gctx := core.AcquireContext(w, req)
	defer core.ReleaseContext(gctx)

	gctx.RequestType = core.RequestTypeMessages
	gctx.RawBody = []byte(reqBody)
	gctx.Model = "glm-5.1"
	gctx.IsStream = true

	invoker := &joycodeMessagesInvoker{}
	err := invoker.Invoke(gctx, p)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	out := w.Body.String()
	if !strings.Contains(out, "event: message_start") {
		t.Errorf("expected message_start event, got: %s", out)
	}
	if !strings.Contains(out, "Thinking GLM5...") {
		t.Errorf("expected thinking content, got: %s", out)
	}
	if !strings.Contains(out, "Hello from GLM5.1") {
		t.Errorf("expected text content, got: %s", out)
	}
	if !strings.Contains(out, "event: message_stop") {
		t.Errorf("expected message_stop event, got: %s", out)
	}
}
