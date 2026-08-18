package llm

import (
	"strings"
)

// SSEEvent represents a single parsed SSE event
type SSEEvent struct {
	Data                string
	Done                bool
	InputTokens         int
	OutputTokens        int
	CachedTokens        int
	CacheCreationTokens int
}

// SSEParser incrementally parses SSE frames from a byte stream.
// Feed() may be called with partial data; it buffers incomplete lines
// and returns fully parsed events.
type SSEParser struct {
	buf strings.Builder
}

// NewSSEParser creates a new SSEParser
func NewSSEParser() *SSEParser {
	return &SSEParser{}
}

// Feed processes incoming bytes and returns any complete SSE events found.
func (p *SSEParser) Feed(data []byte) []SSEEvent {
	p.buf.Write(data)

	var events []SSEEvent
	// Normalize \r\n to \n for CRLF cross-packet compatibility
	fullText := strings.ReplaceAll(p.buf.String(), "\r\n", "\n")

	// Process complete blocks (delimited by \n\n)
	for {
		idx := strings.Index(fullText, "\n\n")
		if idx < 0 {
			break
		}
		block := fullText[:idx]
		fullText = fullText[idx+2:]

		if ev, ok := p.parseBlock(block); ok {
			events = append(events, ev)
		}
	}

	// Keep remaining incomplete data
	p.buf.Reset()
	p.buf.WriteString(fullText)

	return events
}

// parseBlock parses a single SSE block (everything before \n\n).
func (p *SSEParser) parseBlock(block string) (SSEEvent, bool) {
	var dataLines []string

	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "data:") {
			cleanLine := strings.TrimPrefix(line, "data:")
			// Standard SSE specification: strip a single leading space if present
			cleanLine = strings.TrimPrefix(cleanLine, " ")
			dataLines = append(dataLines, cleanLine)
		}
		// Ignore event:, id:, retry: fields
	}

	if len(dataLines) == 0 {
		return SSEEvent{}, false
	}

	data := strings.Join(dataLines, "\n")
	ev := SSEEvent{Data: data}

	// Check for [DONE] sentinel
	if strings.TrimSpace(data) == "[DONE]" {
		ev.Done = true
		return ev, true
	}

	// Try to extract usage tokens from JSON
	ev.InputTokens, ev.OutputTokens, ev.CachedTokens, ev.CacheCreationTokens = OpenAITokenExtractor(data)

	return ev, true
}
