package llm

import (
	"testing"
)

func TestOpenAITokenExtractor_WithUsage(t *testing.T) {
	data := `{"id":"chatcmpl-123","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":20}}`
	pt, ct, cached, cc := OpenAITokenExtractor(data)
	if pt != 10 || ct != 20 || cached != 0 || cc != 0 {
		t.Errorf("expected (10, 20, 0, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestOpenAITokenExtractor_WithCachedUsage(t *testing.T) {
	data := `{"id":"chatcmpl-123","choices":[],"usage":{"prompt_tokens":100,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":40}}}`
	pt, ct, cached, cc := OpenAITokenExtractor(data)
	if pt != 100 || ct != 20 || cached != 40 || cc != 0 {
		t.Errorf("expected (100, 20, 40, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestOpenAITokenExtractor_NoUsage(t *testing.T) {
	data := `{"id":"chatcmpl-123","choices":[{"delta":{"content":"hi"}}]}`
	pt, ct, cached, cc := OpenAITokenExtractor(data)
	if pt != 0 || ct != 0 || cached != 0 || cc != 0 {
		t.Errorf("expected (0, 0, 0, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestOpenAITokenExtractor_InvalidJSON(t *testing.T) {
	pt, ct, cached, cc := OpenAITokenExtractor("not json")
	if pt != 0 || ct != 0 || cached != 0 || cc != 0 {
		t.Errorf("expected (0, 0, 0, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestAnthropicTokenExtractor_MessageStart(t *testing.T) {
	data := `{"type":"message_start","message":{"id":"msg-1","usage":{"input_tokens":50,"output_tokens":0}}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 50 || ct != 0 || cached != 0 || cc != 0 {
		t.Errorf("expected (50, 0, 0, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestAnthropicTokenExtractor_MessageStartWithCaching(t *testing.T) {
	// Anthropic 的 input_tokens 仅含「未命中缓存」部分，提取器需归一化为「总输入」对齐 OpenAI 语义：
	// total = input_tokens(100) + cache_read(60) + cache_creation(40) = 200
	data := `{"type":"message_start","message":{"id":"msg-1","usage":{"input_tokens":100,"output_tokens":0,"cache_read_input_tokens":60,"cache_creation_input_tokens":40}}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 200 || ct != 0 || cached != 60 || cc != 40 {
		t.Errorf("expected (200, 0, 60, 40), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestAnthropicTokenExtractor_MessageDelta(t *testing.T) {
	data := `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":30}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 0 || ct != 30 || cached != 0 || cc != 0 {
		t.Errorf("expected (0, 30, 0, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestAnthropicTokenExtractor_MessageDeltaWithInputAndCaching(t *testing.T) {
	// message_delta 携带输入时同样归一化为总输入：80 + 120 + 10 = 210
	data := `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":80,"output_tokens":40,"cache_read_input_tokens":120,"cache_creation_input_tokens":10}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 210 || ct != 40 || cached != 120 || cc != 10 {
		t.Errorf("expected (210, 40, 120, 10), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestAnthropicTokenExtractor_ContentBlockDelta(t *testing.T) {
	data := `{"type":"content_block_delta","delta":{"type":"text_delta","text":"hello"}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 0 || ct != 0 || cached != 0 || cc != 0 {
		t.Errorf("expected (0, 0, 0, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestAnthropicTokenExtractor_InvalidJSON(t *testing.T) {
	pt, ct, cached, cc := AnthropicTokenExtractor("not json")
	if pt != 0 || ct != 0 || cached != 0 || cc != 0 {
		t.Errorf("expected (0, 0, 0, 0), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

// 非流式整体响应：无 type 字段，顶层 usage。归一化 total = 100 + 60 + 40 = 200。
func TestAnthropicTokenExtractor_NonStreamWholeBody(t *testing.T) {
	data := `{"id":"msg-1","type":"message","role":"assistant","usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":60,"cache_creation_input_tokens":40}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 200 || ct != 20 || cached != 60 || cc != 40 {
		t.Errorf("expected (200, 20, 60, 40), got (%d, %d, %d, %d)", pt, ct, cached, cc)
	}
}

func TestResponsesTokenExtractor_CompletedEvent(t *testing.T) {
	data := `{"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":30,"output_tokens":12,"total_tokens":42,"input_tokens_details":{"cached_tokens":8}}}}`
	in, out, cached, cc := ResponsesTokenExtractor(data)
	if in != 30 || out != 12 || cached != 8 || cc != 0 {
		t.Errorf("expected (30, 12, 8, 0), got (%d, %d, %d, %d)", in, out, cached, cc)
	}
}

func TestResponsesTokenExtractor_DeltaEvent(t *testing.T) {
	data := `{"type":"response.output_text.delta","response_id":"resp_1","delta":"hi"}`
	in, out, cached, cc := ResponsesTokenExtractor(data)
	if in != 0 || out != 0 || cached != 0 || cc != 0 {
		t.Errorf("expected (0, 0, 0, 0), got (%d, %d, %d, %d)", in, out, cached, cc)
	}
}

func TestResponsesTokenExtractor_InvalidJSON(t *testing.T) {
	in, out, cached, cc := ResponsesTokenExtractor("not json")
	if in != 0 || out != 0 || cached != 0 || cc != 0 {
		t.Errorf("expected (0, 0, 0, 0), got (%d, %d, %d, %d)", in, out, cached, cc)
	}
}
