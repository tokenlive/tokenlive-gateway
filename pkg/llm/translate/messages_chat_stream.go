package translate

import (
	"encoding/json"
	"fmt"
	"time"
)

// StreamChunkMeta is side-channel info (tokens, etc.) during stream translate.
type StreamChunkMeta struct {
	InputTokens      int
	OutputTokens     int
	TransmittedChars int
	// EmitDone true: caller should write data: [DONE]
	EmitDone bool
	// ErrorMessage non-empty: emit OpenAI error chunk (or equivalent)
	ErrorMessage string
}

// MessagesToChatStream translates Anthropic Messages SSE to OpenAI Chat stream chunks.
// Caller splits SSE frames; this FSM only consumes full JSON data payloads.
type MessagesToChatStream struct {
	model   string
	id      string
	created int64

	// anthropic content block index -> openAI tool_calls index
	toolIndex map[int]int
	nextTool  int
	// block index -> "text" | "tool_use" | "thinking"
	blockTypes map[int]string

	sawToolUse bool
	finishSent bool
}

// NewMessagesToChatStream creates the stream translation state machine.
func NewMessagesToChatStream(model string) *MessagesToChatStream {
	return &MessagesToChatStream{
		model:      model,
		id:         "chatcmpl-stream",
		created:    time.Now().Unix(),
		toolIndex:  make(map[int]int),
		blockTypes: make(map[int]string),
	}
}

// FeedJSON consumes one Anthropic SSE data field (full JSON or [DONE]).
// Returns 0..n serialized OpenAI chunk JSON (no "data: " prefix).
func (s *MessagesToChatStream) FeedJSON(data string) (chunks [][]byte, meta StreamChunkMeta) {
	if data == "[DONE]" {
		meta.EmitDone = true
		return nil, meta
	}

	var ev map[string]interface{}
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return nil, meta
	}

	// usage side channel
	if usage, ok := ev["usage"].(map[string]interface{}); ok {
		if it, ok := usage["input_tokens"].(float64); ok {
			meta.InputTokens = int(it)
		}
		if ot, ok := usage["output_tokens"].(float64); ok {
			meta.OutputTokens = int(ot)
		}
	}
	if message, ok := ev["message"].(map[string]interface{}); ok {
		if usage, ok := message["usage"].(map[string]interface{}); ok {
			if it, ok := usage["input_tokens"].(float64); ok {
				meta.InputTokens = int(it)
			}
			if ot, ok := usage["output_tokens"].(float64); ok && meta.OutputTokens == 0 {
				meta.OutputTokens = int(ot)
			}
		}
		if id, ok := message["id"].(string); ok && id != "" {
			s.id = "chatcmpl-" + stripMsgPrefix(id)
		}
		if m, ok := message["model"].(string); ok && m != "" {
			s.model = m
		}
	}

	eventType, _ := ev["type"].(string)
	switch eventType {
	case "message_start":
		// id/model already extracted from message above
		return nil, meta

	case "content_block_start":
		idx := intFrom(ev["index"])
		cb, _ := ev["content_block"].(map[string]interface{})
		if cb == nil {
			return nil, meta
		}
		cbType, _ := cb["type"].(string)
		s.blockTypes[idx] = cbType

		if cbType == "tool_use" {
			s.sawToolUse = true
			toolIdx := s.nextTool
			s.toolIndex[idx] = toolIdx
			s.nextTool++

			id, _ := cb["id"].(string)
			name, _ := cb["name"].(string)
			chunk := s.baseChunk(map[string]interface{}{
				"index": 0,
				"delta": map[string]interface{}{
					"tool_calls": []interface{}{
						map[string]interface{}{
							"index": toolIdx,
							"id":    id,
							"type":  "function",
							"function": map[string]interface{}{
								"name":      name,
								"arguments": "",
							},
						},
					},
				},
			})
			if b, err := json.Marshal(chunk); err == nil {
				chunks = append(chunks, b)
			}
		}
		return chunks, meta

	case "content_block_delta":
		idx := intFrom(ev["index"])
		delta, _ := ev["delta"].(map[string]interface{})
		if delta == nil {
			return nil, meta
		}
		dt, _ := delta["type"].(string)

		switch dt {
		case "thinking_delta":
			thinking, _ := delta["thinking"].(string)
			if thinking == "" {
				return nil, meta
			}
			meta.TransmittedChars = len(thinking)
			chunk := s.baseChunk(map[string]interface{}{
				"index": 0,
				"delta": map[string]interface{}{
					"reasoning_content": thinking,
				},
			})
			if b, err := json.Marshal(chunk); err == nil {
				chunks = append(chunks, b)
			}
		case "text_delta":
			text, _ := delta["text"].(string)
			if text == "" {
				return nil, meta
			}
			meta.TransmittedChars = len(text)
			chunk := s.baseChunk(map[string]interface{}{
				"index": 0,
				"delta": map[string]interface{}{
					"content": text,
				},
			})
			if b, err := json.Marshal(chunk); err == nil {
				chunks = append(chunks, b)
			}
		case "input_json_delta":
			partial, _ := delta["partial_json"].(string)
			if partial == "" {
				return nil, meta
			}
			toolIdx, ok := s.toolIndex[idx]
			if !ok {
				// Fallback index if content_block_start not seen
				toolIdx = s.nextTool
				s.toolIndex[idx] = toolIdx
				s.nextTool++
				s.sawToolUse = true
			}
			meta.TransmittedChars = len(partial)
			chunk := s.baseChunk(map[string]interface{}{
				"index": 0,
				"delta": map[string]interface{}{
					"tool_calls": []interface{}{
						map[string]interface{}{
							"index": toolIdx,
							"function": map[string]interface{}{
								"arguments": partial,
							},
						},
					},
				},
			})
			if b, err := json.Marshal(chunk); err == nil {
				chunks = append(chunks, b)
			}
		}
		return chunks, meta

	case "message_delta":
		// May include usage
		if usage, ok := ev["usage"].(map[string]interface{}); ok {
			if ot, ok := usage["output_tokens"].(float64); ok {
				meta.OutputTokens = int(ot)
			}
			if it, ok := usage["input_tokens"].(float64); ok {
				meta.InputTokens = int(it)
			}
		}
		finish := "stop"
		if delta, ok := ev["delta"].(map[string]interface{}); ok {
			if sr, ok := delta["stop_reason"].(string); ok {
				finish = mapStopReasonToFinish(sr, s.sawToolUse)
			}
		}
		if s.sawToolUse && finish == "stop" {
			finish = "tool_calls"
		}
		if !s.finishSent {
			s.finishSent = true
			chunk := s.baseChunk(map[string]interface{}{
				"index":         0,
				"delta":         map[string]interface{}{},
				"finish_reason": finish,
			})
			if b, err := json.Marshal(chunk); err == nil {
				chunks = append(chunks, b)
			}
		}
		return chunks, meta

	case "message_stop":
		if !s.finishSent {
			s.finishSent = true
			finish := "stop"
			if s.sawToolUse {
				finish = "tool_calls"
			}
			chunk := s.baseChunk(map[string]interface{}{
				"index":         0,
				"delta":         map[string]interface{}{},
				"finish_reason": finish,
			})
			if b, err := json.Marshal(chunk); err == nil {
				chunks = append(chunks, b)
			}
		}
		meta.EmitDone = true
		return chunks, meta

	case "error":
		errMap, _ := ev["error"].(map[string]interface{})
		msg, _ := errMap["message"].(string)
		if msg == "" {
			msg = "upstream anthropic error"
		}
		meta.ErrorMessage = msg
		errChunk := map[string]interface{}{
			"error": map[string]interface{}{
				"message": msg,
				"type":    "anthropic_error",
			},
		}
		if b, err := json.Marshal(errChunk); err == nil {
			chunks = append(chunks, b)
		}
		return chunks, meta
	}

	return chunks, meta
}

func (s *MessagesToChatStream) baseChunk(choice map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"id":      s.id,
		"object":  "chat.completion.chunk",
		"created": s.created,
		"model":   s.model,
		"choices": []interface{}{choice},
	}
}

func mapStopReasonToFinish(sr string, sawTool bool) string {
	switch sr {
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "end_turn", "stop_sequence":
		if sawTool {
			return "tool_calls"
		}
		return "stop"
	default:
		if sawTool {
			return "tool_calls"
		}
		return "stop"
	}
}

func intFrom(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}

func stripMsgPrefix(id string) string {
	if len(id) > 4 && id[:4] == "msg_" {
		return id[4:]
	}
	return id
}

// FormatSSEData wraps as an SSE data line.
func FormatSSEData(jsonPayload []byte) string {
	return fmt.Sprintf("data: %s\n\n", string(jsonPayload))
}
