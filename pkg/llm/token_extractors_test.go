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
	data := `{"type":"message_start","message":{"id":"msg-1","usage":{"input_tokens":100,"output_tokens":0,"cache_read_input_tokens":60,"cache_creation_input_tokens":40}}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 100 || ct != 0 || cached != 60 || cc != 40 {
		t.Errorf("expected (100, 0, 60, 40), got (%d, %d, %d, %d)", pt, ct, cached, cc)
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
	data := `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":80,"output_tokens":40,"cache_read_input_tokens":120,"cache_creation_input_tokens":10}}`
	pt, ct, cached, cc := AnthropicTokenExtractor(data)
	if pt != 80 || ct != 40 || cached != 120 || cc != 10 {
		t.Errorf("expected (80, 40, 120, 10), got (%d, %d, %d, %d)", pt, ct, cached, cc)
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
