package translate

import (
	"fmt"
	"strings"
)

// ToolNameMapper maintains a bidirectional mapping between the original
// (namespace, name) of a Responses tool and a pattern-safe sanitized full name
// suitable for strict OpenAI-compatible upstreams (DeepSeek, Qwen) that enforce
// ^[a-zA-Z0-9_-]+$ on tools[].function.name and reject dots/colons/slashes.
//
// Request path: SanitizeAndRegister converts (namespace, name) to a safe name
// and records it so the response path can restore the original pair.
// Response path: Restore looks up the safe name the upstream echoed back and
// returns the original (namespace, name); unknown names fall back to the
// dot-split heuristic for backward compatibility.
type ToolNameMapper struct {
	forward map[string]string // "namespace\x00name" -> sanitized
	reverse map[string]toolNameEntry
	used    map[string]bool
}

type toolNameEntry struct {
	namespace string
	name      string
}

// NewToolNameMapper creates an empty mapper.
func NewToolNameMapper() *ToolNameMapper {
	return &ToolNameMapper{
		forward: make(map[string]string),
		reverse: make(map[string]toolNameEntry),
		used:    make(map[string]bool),
	}
}

// SanitizeAndRegister produces a pattern-safe full name for (namespace, name)
// and registers the mapping. Calling twice with the same pair returns the same
// sanitized name (idempotent). On collision with a different original pair, a
// numeric suffix is appended so each pair maps to a distinct safe name.
func (m *ToolNameMapper) SanitizeAndRegister(namespace, name string) string {
	key := namespace + "\x00" + name
	if s, ok := m.forward[key]; ok {
		return s
	}

	full := name
	if namespace != "" {
		full = namespace + "." + name
	}
	sanitized := sanitizeToolName(full)

	// Dedup: if this safe name is already taken by a different original pair,
	// append _2, _3, ... until free.
	if m.used[sanitized] {
		base := sanitized
		for i := 2; ; i++ {
			candidate := fmt.Sprintf("%s_%d", base, i)
			if !m.used[candidate] {
				sanitized = candidate
				break
			}
		}
	}

	m.forward[key] = sanitized
	m.reverse[sanitized] = toolNameEntry{namespace: namespace, name: name}
	m.used[sanitized] = true
	return sanitized
}

// Restore returns the original (namespace, name) for a sanitized full name the
// upstream echoed back. Unknown names fall back to splitting on the last dot.
func (m *ToolNameMapper) Restore(sanitized string) (namespace, name string) {
	if e, ok := m.reverse[sanitized]; ok {
		return e.namespace, e.name
	}
	return splitChatToolNamespace(sanitized), chatToolLocalName(sanitized)
}

// sanitizeToolName replaces every character outside [a-zA-Z0-9_-] with '_'.
func sanitizeToolName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
