package translate

import (
	"encoding/json"
	"testing"
)

func feedResponses(t *testing.T, s *MessagesToResponsesStream, frames []string) (events []map[string]interface{}, metas []StreamChunkMeta) {
	t.Helper()
	for _, f := range frames {
		out, meta := s.FeedJSON(f)
		metas = append(metas, meta)
		for _, oe := range out {
			var m map[string]interface{}
			if err := json.Unmarshal(oe.Data, &m); err != nil {
				t.Fatalf("unmarshal event %s: %v data=%s", oe.Event, err, oe.Data)
			}
			if m["type"] != oe.Event {
				t.Fatalf("event field %s != payload type %v", oe.Event, m["type"])
			}
			events = append(events, m)
		}
	}
	return events, metas
}

func eventTypes(events []map[string]interface{}) []string {
	var types []string
	for _, e := range events {
		types = append(types, e["type"].(string))
	}
	return types
}

func TestMessagesToResponsesStream_TextFlow(t *testing.T) {
	s := NewMessagesToResponsesStream("claude-alias")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_01ABC","model":"claude-sonnet-4-20250514","usage":{"input_tokens":10,"cache_read_input_tokens":4,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}`,
		`{"type":"message_stop"}`,
	}
	events, metas := feedResponses(t, s, frames)

	want := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.delta",
		"response.output_text.delta",
		"response.output_text.done",
		"response.content_part.done",
		"response.output_item.done",
		"response.done",
		"response.completed",
	}
	got := eventTypes(events)
	if len(got) != len(want) {
		t.Fatalf("events = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events[%d] = %s, want %s\ngot: %v", i, got[i], want[i], got)
		}
	}

	if s.ResponseID() != "resp_01ABC" {
		t.Errorf("respID = %v", s.ResponseID())
	}

	// ids derived from raw upstream id, not double-prefixed
	itemAdded := events[2]
	if itemAdded["item"].(map[string]interface{})["id"] != "msg_01ABC_0" {
		t.Errorf("item id = %v", itemAdded["item"])
	}

	textDone := events[6]
	if textDone["text"] != "Hello world" {
		t.Errorf("text done = %v", textDone["text"])
	}

	completed := events[10]
	respObj := completed["response"].(map[string]interface{})
	if respObj["status"] != "completed" {
		t.Errorf("status = %v", respObj["status"])
	}
	usage := respObj["usage"].(map[string]interface{})
	if usage["input_tokens"].(float64) != 14 { // 10 + 4 cached
		t.Errorf("input_tokens = %v", usage["input_tokens"])
	}
	if usage["output_tokens"].(float64) != 7 {
		t.Errorf("output_tokens = %v", usage["output_tokens"])
	}
	output := respObj["output"].([]interface{})
	if len(output) != 1 {
		t.Fatalf("output len = %d", len(output))
	}

	// meta: input(14) on message_start, output(7) on message_delta, EmitDone on message_stop
	if metas[0].InputTokens != 14 || metas[0].CachedTokens != 4 {
		t.Errorf("meta[0] = %+v", metas[0])
	}
	if metas[5].OutputTokens != 7 {
		t.Errorf("meta[5] = %+v", metas[5])
	}
	if !metas[6].EmitDone {
		t.Error("message_stop should EmitDone")
	}
	if metas[2].TransmittedChars != len("Hello") {
		t.Errorf("TransmittedChars = %d", metas[2].TransmittedChars)
	}
}

func TestMessagesToResponsesStream_ToolCallFlow(t *testing.T) {
	s := NewMessagesToResponsesStream("m")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_X","usage":{"input_tokens":3}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_01Y","name":"get_weather","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"BJ\"}"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}`,
		`{"type":"message_stop"}`,
	}
	events, _ := feedResponses(t, s, frames)

	want := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.function_call.arguments.delta",
		"response.function_call.arguments.delta",
		"response.function_call.arguments.done",
		"response.output_item.done",
		"response.done",
		"response.completed",
	}
	got := eventTypes(events)
	if len(got) != len(want) {
		t.Fatalf("events = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events[%d] = %s, want %s", i, got[i], want[i])
		}
	}

	added := events[2]["item"].(map[string]interface{})
	if added["type"] != "function_call" || added["call_id"] != "toolu_01Y" || added["name"] != "get_weather" {
		t.Errorf("item added = %v", added)
	}
	argsDone := events[5]
	if argsDone["arguments"] != `{"city":"BJ"}` {
		t.Errorf("arguments = %v", argsDone["arguments"])
	}
	itemDone := events[6]["item"].(map[string]interface{})
	if itemDone["status"] != "completed" || itemDone["arguments"] != `{"city":"BJ"}` {
		t.Errorf("item done = %v", itemDone)
	}
}

func TestMessagesToResponsesStream_ThinkingFlow(t *testing.T) {
	s := NewMessagesToResponsesStream("m")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_T","usage":{"input_tokens":5}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"let me think"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-1"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"done"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":30}}`,
		`{"type":"message_stop"}`,
	}
	events, _ := feedResponses(t, s, frames)

	want := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.reasoning_summary_part.added",
		"response.reasoning_summary_text.delta",
		"response.reasoning_summary_text.done",
		"response.reasoning_summary_part.done",
		"response.output_item.done",
		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.delta",
		"response.output_text.done",
		"response.content_part.done",
		"response.output_item.done",
		"response.done",
		"response.completed",
	}
	got := eventTypes(events)
	if len(got) != len(want) {
		t.Fatalf("events = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events[%d] = %s, want %s", i, got[i], want[i])
		}
	}

	delta := events[4]
	if delta["delta"] != "let me think" || delta["summary_index"].(float64) != 0 {
		t.Errorf("reasoning delta = %v", delta)
	}
	reasoningDone := events[7]["item"].(map[string]interface{})
	if reasoningDone["type"] != "reasoning" || reasoningDone["encrypted_content"] != "sig-1" {
		t.Errorf("reasoning done = %v", reasoningDone)
	}

	// two output items with distinct indexes
	completed := events[15]["response"].(map[string]interface{})
	output := completed["output"].([]interface{})
	if len(output) != 2 {
		t.Fatalf("output len = %d", len(output))
	}
}

func TestMessagesToResponsesStream_MaxTokensIncomplete(t *testing.T) {
	s := NewMessagesToResponsesStream("m")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"cut off"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":100}}`,
		`{"type":"message_stop"}`,
	}
	events, _ := feedResponses(t, s, frames)

	completed := events[len(events)-1]["response"].(map[string]interface{})
	if completed["status"] != "incomplete" {
		t.Errorf("status = %v", completed["status"])
	}
	details := completed["incomplete_details"].(map[string]interface{})
	if details["reason"] != "max_output_tokens" {
		t.Errorf("incomplete_details = %v", details)
	}
}

func TestMessagesToResponsesStream_MidStreamError(t *testing.T) {
	s := NewMessagesToResponsesStream("m")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_E","usage":{"input_tokens":2}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}`,
		`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`,
	}
	events, metas := feedResponses(t, s, frames)

	last := events[len(events)-1]
	failed := events[len(events)-2]
	if failed["type"] != "response.failed" {
		t.Fatalf("second-to-last = %v", failed["type"])
	}
	respObj := failed["response"].(map[string]interface{})
	if respObj["status"] != "failed" {
		t.Errorf("status = %v", respObj["status"])
	}
	errObj := respObj["error"].(map[string]interface{})
	if errObj["message"] != "Overloaded" {
		t.Errorf("error = %v", errObj)
	}
	if last["type"] != "response.completed" {
		t.Errorf("last = %v", last["type"])
	}
	if !metas[len(metas)-1].EmitDone {
		t.Error("error should EmitDone")
	}
	if metas[len(metas)-1].ErrorMessage != "Overloaded" {
		t.Errorf("ErrorMessage = %v", metas[len(metas)-1].ErrorMessage)
	}
}

func TestMessagesToResponsesStream_RedactedThinking(t *testing.T) {
	s := NewMessagesToResponsesStream("m")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_R","usage":{"input_tokens":2}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"encrypted-blob"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`,
		`{"type":"message_stop"}`,
	}
	events, _ := feedResponses(t, s, frames)

	// added + done only; no summary delta events for redacted thinking
	want := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.output_item.done",
		"response.done",
		"response.completed",
	}
	got := eventTypes(events)
	if len(got) != len(want) {
		t.Fatalf("events = %v", got)
	}
	itemDone := events[3]["item"].(map[string]interface{})
	if itemDone["type"] != "reasoning" || itemDone["encrypted_content"] != "encrypted-blob" {
		t.Errorf("redacted done = %v", itemDone)
	}
}
