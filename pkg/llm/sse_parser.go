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
	buf           strings.Builder
	skipLeadingLF bool
}

// NewSSEParser creates a new SSEParser
func NewSSEParser() *SSEParser {
	return &SSEParser{}
}

// Feed processes incoming bytes and returns any complete SSE events found.
func (p *SSEParser) Feed(data []byte) []SSEEvent {
	if p.skipLeadingLF && len(data) > 0 {
		if data[0] == '\n' {
			data = data[1:]
		}
		p.skipLeadingLF = false
	}

	var events []SSEEvent
	// Normalize CRLF and CR line endings to LF. A trailing CR is already a
	// complete SSE line ending, but the next feed may start with its LF pair.
	if len(data) > 0 && data[len(data)-1] == '\r' {
		p.skipLeadingLF = true
	}
	normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	p.buf.WriteString(normalized)
	fullText := p.buf.String()

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
