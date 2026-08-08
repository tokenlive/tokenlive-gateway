package translate

import (
	"encoding/json"
	"testing"
)

func feedAll(t *testing.T, s *MessagesToChatStream, frames []string) (chunks []map[string]interface{}, metas []StreamChunkMeta) {
	t.Helper()
	for _, f := range frames {
		cs, meta := s.FeedJSON(f)
		metas = append(metas, meta)
		for _, c := range cs {
			var m map[string]interface{}
			if err := json.Unmarshal(c, &m); err != nil {
				t.Fatalf("unmarshal chunk: %v body=%s", err, c)
			}
			chunks = append(chunks, m)
		}
	}
	return chunks, metas
}

func TestMessagesToChatStream_TextAndThinking(t *testing.T) {
	s := NewMessagesToChatStream("claude-3-5-sonnet")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_001","model":"claude-3-5-sonnet","usage":{"input_tokens":10}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me think."}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Answer is 42."}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":20}}`,
		`{"type":"message_stop"}`,
	}
	chunks, metas := feedAll(t, s, frames)

	if len(chunks) < 3 {
		t.Fatalf("expected >=3 chunks, got %d", len(chunks))
	}

	// thinking
	d0 := chunks[0]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	if d0["reasoning_content"] != "Let me think." {
		t.Errorf("thinking = %v", d0)
	}
	// text
	d1 := chunks[1]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	if d1["content"] != "Answer is 42." {
		t.Errorf("text = %v", d1)
	}
	// finish
	c2 := chunks[2]["choices"].([]interface{})[0].(map[string]interface{})
	if c2["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %v", c2["finish_reason"])
	}

	var lastMeta StreamChunkMeta
	for _, m := range metas {
		if m.InputTokens > 0 {
			lastMeta.InputTokens = m.InputTokens
		}
		if m.OutputTokens > 0 {
			lastMeta.OutputTokens = m.OutputTokens
		}
		if m.EmitDone {
			lastMeta.EmitDone = true
		}
	}
	if lastMeta.InputTokens != 10 || lastMeta.OutputTokens != 20 {
		t.Errorf("tokens in=%d out=%d", lastMeta.InputTokens, lastMeta.OutputTokens)
	}
	if !lastMeta.EmitDone {
		t.Error("expected EmitDone")
	}
	if chunks[0]["id"] != "chatcmpl-001" {
		t.Errorf("id = %v", chunks[0]["id"])
	}
}

func TestMessagesToChatStream_ToolUse(t *testing.T) {
	s := NewMessagesToChatStream("claude-3-5-sonnet")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_tool","usage":{"input_tokens":5}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"Beijing\"}"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":12}}`,
		`{"type":"message_stop"}`,
	}
	chunks, metas := feedAll(t, s, frames)

	// start tool + 2 arg deltas + finish
	if len(chunks) < 4 {
		t.Fatalf("expected >=4 chunks, got %d: %+v", len(chunks), chunks)
	}

	// content_block_start → tool_calls with id/name
	d0 := chunks[0]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	tcs := d0["tool_calls"].([]interface{})
	tc0 := tcs[0].(map[string]interface{})
	if tc0["id"] != "toolu_1" {
		t.Errorf("id = %v", tc0["id"])
	}
	fn := tc0["function"].(map[string]interface{})
	if fn["name"] != "get_weather" {
		t.Errorf("name = %v", fn["name"])
	}

	// partial json args
	d1 := chunks[1]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	args1 := d1["tool_calls"].([]interface{})[0].(map[string]interface{})["function"].(map[string]interface{})["arguments"]
	if args1 != `{"city":` {
		t.Errorf("args1 = %v", args1)
	}
	d2 := chunks[2]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})
	args2 := d2["tool_calls"].([]interface{})[0].(map[string]interface{})["function"].(map[string]interface{})["arguments"]
	if args2 != `"Beijing"}` {
		t.Errorf("args2 = %v", args2)
	}

	// finish_reason tool_calls
	finish := chunks[3]["choices"].([]interface{})[0].(map[string]interface{})["finish_reason"]
	if finish != "tool_calls" {
		t.Errorf("finish_reason = %v", finish)
	}

	var outTok int
	var done bool
	for _, m := range metas {
		if m.OutputTokens > 0 {
			outTok = m.OutputTokens
		}
		if m.EmitDone {
			done = true
		}
	}
	if outTok != 12 {
		t.Errorf("output tokens = %d", outTok)
	}
	if !done {
		t.Error("expected EmitDone")
	}
}

func TestMessagesToChatStream_MultiTools(t *testing.T) {
	s := NewMessagesToChatStream("m")
	frames := []string{
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_a","name":"a","input":{}}}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_b","name":"b","input":{}}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{}"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`,
	}
	chunks, _ := feedAll(t, s, frames)

	// two starts
	idx0 := chunks[0]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})["tool_calls"].([]interface{})[0].(map[string]interface{})["index"]
	idx1 := chunks[1]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})["tool_calls"].([]interface{})[0].(map[string]interface{})["index"]
	if idx0.(float64) != 0 || idx1.(float64) != 1 {
		t.Errorf("tool indices = %v, %v", idx0, idx1)
	}
	// delta for second tool uses index 1
	argIdx := chunks[2]["choices"].([]interface{})[0].(map[string]interface{})["delta"].(map[string]interface{})["tool_calls"].([]interface{})[0].(map[string]interface{})["index"]
	if argIdx.(float64) != 1 {
		t.Errorf("arg index = %v", argIdx)
	}
}

func TestMessagesToChatStream_CacheTokens(t *testing.T) {
	s := NewMessagesToChatStream("claude-3-5-sonnet")
	frames := []string{
		`{"type":"message_start","message":{"id":"msg_cache","usage":{"input_tokens":10,"cache_read_input_tokens":100,"cache_creation_input_tokens":50}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}`,
		`{"type":"message_stop"}`,
	}
	_, metas := feedAll(t, s, frames)

	var cached, cacheCreated, in, out int
	for _, m := range metas {
		if m.CachedTokens > 0 {
			cached = m.CachedTokens
		}
		if m.CacheCreationTokens > 0 {
			cacheCreated = m.CacheCreationTokens
		}
		if m.InputTokens > 0 {
			in = m.InputTokens
		}
		if m.OutputTokens > 0 {
			out = m.OutputTokens
		}
	}
	if cached != 100 {
		t.Errorf("cached = %d, want 100", cached)
	}
	if cacheCreated != 50 {
		t.Errorf("cacheCreated = %d, want 50", cacheCreated)
	}
	// meta.InputTokens holds the raw (uncached) value; normalization happens at the caller.
	if in != 10 {
		t.Errorf("input = %d, want 10 (raw)", in)
	}
	if out != 7 {
		t.Errorf("output = %d, want 7", out)
	}
}

func TestMessagesToChatStream_Error(t *testing.T) {
	s := NewMessagesToChatStream("m")
	chunks, metas := feedAll(t, s, []string{
		`{"type":"error","error":{"message":"boom","type":"api_error"}}`,
	})
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d", len(chunks))
	}
	if metas[0].ErrorMessage != "boom" {
		t.Errorf("ErrorMessage = %q", metas[0].ErrorMessage)
	}
	errObj := chunks[0]["error"].(map[string]interface{})
	if errObj["message"] != "boom" {
		t.Errorf("error = %v", errObj)
	}
}
