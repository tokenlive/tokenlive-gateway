package llm

import (
	"testing"
)

func TestSSEParser_ParseSimpleEvent(t *testing.T) {
	p := NewSSEParser()
	events := p.Feed([]byte("data: {\"id\":\"chatcmpl-1\"}\n\n"))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != `{"id":"chatcmpl-1"}` {
		t.Errorf("unexpected data: %s", events[0].Data)
	}
}

func TestSSEParser_ParseMultipleEvents(t *testing.T) {
	p := NewSSEParser()
	input := "data: {\"a\":1}\n\ndata: {\"b\":2}\n\n"
	events := p.Feed([]byte(input))
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestSSEParser_MultiLineData(t *testing.T) {
	p := NewSSEParser()
	input := "data: line1\ndata: line2\n\n"
	events := p.Feed([]byte(input))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	// Multi-line data concatenated with newline
	if events[0].Data != "line1\nline2" {
		t.Errorf("expected 'line1\\nline2', got '%s'", events[0].Data)
	}
}

func TestSSEParser_DoneSignal(t *testing.T) {
	p := NewSSEParser()
	events := p.Feed([]byte("data: [DONE]\n\n"))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !events[0].Done {
		t.Error("expected Done=true for [DONE] event")
	}
}

func TestSSEParser_ExtractUsageTokens(t *testing.T) {
	p := NewSSEParser()
	usageJSON := `{"id":"chatcmpl-1","usage":{"prompt_tokens":10,"completion_tokens":20}}`
	events := p.Feed([]byte("data: " + usageJSON + "\n\n"))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].InputTokens != 10 {
		t.Errorf("expected prompt_tokens=10, got %d", events[0].InputTokens)
	}
	if events[0].OutputTokens != 20 {
		t.Errorf("expected completion_tokens=20, got %d", events[0].OutputTokens)
	}
}

func TestSSEParser_PartialData(t *testing.T) {
	p := NewSSEParser()
	// Feed data in chunks
	events1 := p.Feed([]byte("data: {\"id"))
	if len(events1) != 0 {
		t.Fatalf("expected 0 events from partial feed, got %d", len(events1))
	}
	events2 := p.Feed([]byte("\":\"1\"}\n\n"))
	if len(events2) != 1 {
		t.Fatalf("expected 1 event after completion, got %d", len(events2))
	}
}

func TestSSEParser_SkipEmptyLines(t *testing.T) {
	p := NewSSEParser()
	input := "\n\ndata: test\n\n\n\ndata: test2\n\n"
	events := p.Feed([]byte(input))
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestSSEParser_IgnoreNonDataFields(t *testing.T) {
	p := NewSSEParser()
	input := "event: message\nid: 123\ndata: payload\n\n"
	events := p.Feed([]byte(input))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != "payload" {
		t.Errorf("expected 'payload', got '%s'", events[0].Data)
	}
}

func TestSSEParser_CRLF(t *testing.T) {
	p := NewSSEParser()
	input := "data: {\"id\":\"chatcmpl-1\"}\r\n\r\n"
	events := p.Feed([]byte(input))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data != `{"id":"chatcmpl-1"}` {
		t.Errorf("unexpected data: %s", events[0].Data)
	}

	// 测试跨包分块 CRLF
	p2 := NewSSEParser()
	e1 := p2.Feed([]byte("data: test\r"))
	if len(e1) != 0 {
		t.Fatalf("expected 0 events, got %d", len(e1))
	}
	e2 := p2.Feed([]byte("\n\r\n"))
	if len(e2) != 1 {
		t.Fatalf("expected 1 event, got %d", len(e2))
	}
	if e2[0].Data != "test" {
		t.Errorf("expected 'test', got '%s'", e2[0].Data)
	}
}

func TestSSEParser_CRLFPairSplitAcrossFeedsDoesNotEndEvent(t *testing.T) {
	p := NewSSEParser()

	events := p.Feed([]byte("data: one\r"))
	if len(events) != 0 {
		t.Fatalf("expected no event from partial CRLF line ending, got %d", len(events))
	}

	events = p.Feed([]byte("\ndata: two\r\n\r\n"))
	if len(events) != 1 {
		t.Fatalf("expected one multiline event, got %d: %#v", len(events), events)
	}
	if events[0].Data != "one\ntwo" {
		t.Fatalf("expected joined multiline data, got %q", events[0].Data)
	}
}

func TestSSEParser_PreservesWhitespaceAndIndentation(t *testing.T) {
	p := NewSSEParser()
	// Simulate code patch diff with leading indentation and empty lines
	input := "data:   def hello():\ndata:       print(\"world\")\ndata: \ndata: +    return True\n\n"
	events := p.Feed([]byte(input))
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	expected := "  def hello():\n      print(\"world\")\n\n+    return True"
	if events[0].Data != expected {
		t.Errorf("expected:\n%q\ngot:\n%q", expected, events[0].Data)
	}
}
