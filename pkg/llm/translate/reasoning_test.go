package translate_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
)

func reasoningJSON(t *testing.T, body []byte) map[string]interface{} {
	t.Helper()
	var value map[string]interface{}
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestReasoningAliasesStayPathSpecific(t *testing.T) {
	for _, tc := range []struct {
		name, fields, responses, messages, messagesStream string
	}{
		{"precedence", `"reasoning_content":"rc","thinking":"think","reasoning":"reason","thought":"thought"`, "rc", "rc", "rc"},
		{"empty primary", `"reasoning_content":"","thinking":"think","reasoning":"reason","thought":"thought"`, "reason", "", "think"},
		{"reasoning fallback", `"reasoning":"reason","thought":"thought"`, "reason", "", "reason"},
		{"thought only", `"thought":"thought"`, "", "", "thought"},
		{"thinking only", `"thinking":"think"`, "", "", "think"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"id":"chatcmpl-alias","choices":[{"message":{"content":"answer",` + tc.fields + `}}]}`)
			resp, err := translate.ChatCompletionToResponses(body, "m", nil)
			if err != nil {
				t.Fatal(err)
			}
			output := reasoningJSON(t, resp.Body)["output"].([]interface{})
			got := ""
			if item := output[0].(map[string]interface{}); item["type"] == "reasoning" {
				got = item["summary"].([]interface{})[0].(map[string]interface{})["text"].(string)
			}
			if got != tc.responses {
				t.Fatalf("Responses reasoning = %q, want %q", got, tc.responses)
			}
			msgs, err := translate.ChatCompletionToMessages(body, "m")
			if err != nil {
				t.Fatal(err)
			}
			got = ""
			if block := reasoningJSON(t, msgs.Body)["content"].([]interface{})[0].(map[string]interface{}); block["type"] == "thinking" {
				got = block["thinking"].(string)
			}
			if got != tc.messages {
				t.Fatalf("Messages reasoning = %q, want %q", got, tc.messages)
			}
			chunk := `{"id":"chatcmpl-alias","choices":[{"delta":{` + tc.fields + `}}]}`
			rs := translate.NewChatToResponsesStream("m", nil, translate.TokenUsage{})
			events, _ := rs.FeedJSON(chunk)
			got = ""
			for _, ev := range events {
				if ev.Event == "response.reasoning_summary_text.delta" {
					got += reasoningJSON(t, ev.Data)["delta"].(string)
				}
			}
			if got != tc.responses {
				t.Fatalf("Responses stream reasoning = %q, want %q", got, tc.responses)
			}
			ms := translate.NewChatToMessagesStream("m", translate.TokenUsage{})
			messageEvents, _ := ms.FeedJSON(chunk)
			got = ""
			for _, ev := range messageEvents {
				if ev.Event == "content_block_delta" {
					got += reasoningJSON(t, ev.Data)["delta"].(map[string]interface{})["thinking"].(string)
				}
			}
			if got != tc.messagesStream {
				t.Fatalf("Messages stream reasoning = %q, want %q", got, tc.messagesStream)
			}
		})
	}
}

func TestReasoningHistoryLastNonemptyAndSummaryFallbacks(t *testing.T) {
	for _, tc := range []struct{ name, item, want string }{
		{"mixed summary", `{"summary":["a",{"text":"b"},"",{"text":"c"}],"text":"ignored"}`, "abc"},
		{"string summary", `{"summary":"summary","reasoning_content":"ignored"}`, "summary"},
		{"plaintext fallback", `{"summary":[],"reasoning_content":"rc","text":"ignored"}`, "rc"},
		{"content fallback", `{"content":["a",{"text":"b"}]}`, "ab"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := reasoningJSON(t, []byte(tc.item))
			item["type"] = "reasoning"
			input := []interface{}{
				map[string]interface{}{"type": "reasoning", "summary": "old"}, item,
				map[string]interface{}{"type": "reasoning", "summary": []interface{}{}},
				map[string]interface{}{"role": "user", "content": "question"},
				map[string]interface{}{"type": "function_call", "call_id": "call_1", "name": "tool", "arguments": "{}"},
				map[string]interface{}{"type": "function_call_output", "call_id": "call_1", "output": "ok"},
				map[string]interface{}{"role": "user", "content": "next question"},
				map[string]interface{}{"role": "assistant", "content": "answer"},
			}
			raw, _ := json.Marshal(map[string]interface{}{"input": input})
			body, _, err := translate.ResponsesRequestToChat(raw)
			if err != nil {
				t.Fatal(err)
			}
			messages := reasoningJSON(t, body)["messages"].([]interface{})
			if len(messages) != 5 {
				t.Fatalf("messages = %v", messages)
			}
			if _, exists := messages[0].(map[string]interface{})["reasoning_content"]; exists {
				t.Fatal("user received reasoning")
			}
			if got := messages[1].(map[string]interface{})["reasoning_content"]; got != tc.want {
				t.Fatalf("pending reasoning = %v, want %q", got, tc.want)
			}
			if got := messages[4].(map[string]interface{})["reasoning_content"]; got != "" {
				t.Fatalf("consumed reasoning = %v", got)
			}
		})
	}
}

func TestReasoningMessagesReplayRetainsSignedShape(t *testing.T) {
	raw := []byte(`{"input":[{"role":"user","content":"hi"},{"role":"assistant","content":"answer"},{"type":"reasoning","encrypted_content":"sig","summary":["ignored",{"text":"a"},{"text":"b"}]},{"type":"reasoning","summary":[{"text":"unsigned"}]}]}`)
	res, err := translate.ResponsesRequestToMessages(raw, "m")
	if err != nil {
		t.Fatal(err)
	}
	messages := reasoningJSON(t, res.Body)["messages"].([]interface{})
	blocks := messages[1].(map[string]interface{})["content"].([]interface{})
	want := []interface{}{map[string]interface{}{"type": "thinking", "thinking": "a\nb", "signature": "sig"}, map[string]interface{}{"type": "text", "text": "answer"}}
	if !reflect.DeepEqual(blocks, want) {
		t.Fatalf("replayed blocks = %v", blocks)
	}
}

func TestReasoningMessagesOutputEmptySummaryAndSignatures(t *testing.T) {
	for _, tc := range []struct {
		kind, text, encrypted           string
		nonstreamSummary, streamSummary int
	}{
		{"thinking", "", "sig", 1, 0}, {"thinking", "plain", "sig", 1, 1}, {"redacted_thinking", "", "blob", 0, 0},
	} {
		t.Run(tc.kind+tc.text, func(t *testing.T) {
			block := map[string]interface{}{"type": tc.kind, "thinking": tc.text, "signature": tc.encrypted}
			if tc.kind == "redacted_thinking" {
				block = map[string]interface{}{"type": tc.kind, "data": tc.encrypted}
			}
			raw, _ := json.Marshal(map[string]interface{}{"id": "msg_shape", "content": []interface{}{block}})
			res, err := translate.MessagesResponseToResponses(raw, "m")
			if err != nil {
				t.Fatal(err)
			}
			item := reasoningJSON(t, res.Body)["output"].([]interface{})[0].(map[string]interface{})
			if len(item) != 4 || item["encrypted_content"] != tc.encrypted || len(item["summary"].([]interface{})) != tc.nonstreamSummary {
				t.Fatalf("nonstream item = %v", item)
			}
			chat, err := translate.MessagesToChatCompletion(raw, "m")
			if err != nil {
				t.Fatal(err)
			}
			message := reasoningJSON(t, chat.Body)["choices"].([]interface{})[0].(map[string]interface{})["message"].(map[string]interface{})
			if rc, exists := message["reasoning_content"]; exists != (tc.text != "") || (exists && rc != tc.text) {
				t.Fatalf("Chat plaintext = %v", message)
			}
			stream := translate.NewMessagesToResponsesStream("m")
			stream.FeedJSON(`{"type":"message_start","message":{"id":"msg_shape"}}`)
			start, _ := json.Marshal(map[string]interface{}{"type": "content_block_start", "index": 0, "content_block": block})
			stream.FeedJSON(string(start))
			if tc.kind == "thinking" {
				delta, _ := json.Marshal(map[string]interface{}{"type": "content_block_delta", "index": 0, "delta": map[string]interface{}{"type": "thinking_delta", "thinking": tc.text}})
				stream.FeedJSON(string(delta))
				stream.FeedJSON(`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}`)
			}
			events, _ := stream.FeedJSON(`{"type":"content_block_stop","index":0}`)
			item = reasoningJSON(t, events[len(events)-1].Data)["item"].(map[string]interface{})
			if len(item) != 4 || item["encrypted_content"] != tc.encrypted || len(item["summary"].([]interface{})) != tc.streamSummary {
				t.Fatalf("stream item = %v", item)
			}
		})
	}
}

func TestReasoningBudgetBoundariesPreserveCurrentResolver(t *testing.T) {
	for _, tc := range []struct {
		name, effort               string
		client, limit, max, budget int
		disabledWarning            bool
	}{
		{"minimal", "minimal", 0, 0, 9216, 1024, false},
		{"low", "low", 0, 0, 9216, 1024, false},
		{"medium", "medium", 0, 0, 12288, 4096, false},
		{"high", "high", 0, 0, 24576, 16384, false},
		{"fits minimum", "high", 2048, 0, 2048, 1024, false},
		{"below minimum", "high", 2047, 0, 2047, 0, true},
		{"endpoint with client", "high", 8000, 4096, 4096, 3072, false},
		{"endpoint without client keeps budget", "high", 0, 1500, 1500, 16384, false},
		{"unknown", "unknown", 0, 0, 8192, 0, false},
		{"none", "none", 10000, 4096, 4096, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]interface{}{"input": "hi", "reasoning": map[string]interface{}{"effort": tc.effort}}
			if tc.client != 0 {
				payload["max_output_tokens"] = tc.client
			}
			raw, _ := json.Marshal(payload)
			res, err := translate.ResponsesRequestToMessages(raw, "m", tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			body := reasoningJSON(t, res.Body)
			if body["max_tokens"] != float64(tc.max) || res.ThinkingEnabled != (tc.budget != 0) {
				t.Fatalf("budget result = %s, enabled %v", res.Body, res.ThinkingEnabled)
			}
			if tc.budget != 0 && body["thinking"].(map[string]interface{})["budget_tokens"] != float64(tc.budget) {
				t.Fatalf("thinking = %v", body["thinking"])
			}
			if got := strings.Contains(strings.Join(res.Warnings, "\n"), "thinking disabled"); got != tc.disabledWarning {
				t.Fatalf("warnings = %v", res.Warnings)
			}
		})
	}
}
