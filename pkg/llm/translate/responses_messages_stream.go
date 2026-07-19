package translate

import (
	"encoding/json"
	"fmt"
	"time"
)

// ResponsesStreamEvent is one serialized Responses SSE frame.
// Caller writes it as "event: <Event>\ndata: <Data>\n\n".
type ResponsesStreamEvent struct {
	Event string
	Data  []byte
}

// responsesStreamItem tracks one in-flight output item (per Anthropic content block).
type responsesStreamItem struct {
	kind        string // "thinking" | "text" | "tool_use" | "redacted_thinking"
	outputIndex int
	itemID      string
	callID      string // tool_use only
	name        string // tool_use only
	text        string // accumulated text / thinking / arguments
	signature   string // thinking only
	closed      bool
}

// MessagesToResponsesStream translates Anthropic Messages SSE to Responses SSE events.
// Caller splits SSE frames; this FSM only consumes full JSON data payloads.
type MessagesToResponsesStream struct {
	model     string
	respID    string
	idBase    string // raw upstream id without msg_ prefix; base for per-item ids
	createdAt int64
	started   bool

	items       map[int]*responsesStreamItem // anthropic block index -> item
	nextOutput  int
	completed   []map[string]interface{} // finished output items, in order
	stopReason  string
	usageInput  int
	usageOutput int
	usageCached int
	usageCacheCreation int
}

// NewMessagesToResponsesStream creates the stream translation state machine.
// model is the client-facing model name used in emitted response objects.
func NewMessagesToResponsesStream(model string) *MessagesToResponsesStream {
	return &MessagesToResponsesStream{
		model:     model,
		respID:    "resp_mock",
		createdAt: time.Now().Unix(),
		items:     make(map[int]*responsesStreamItem),
	}
}

// ResponseID returns the upstream-derived response id (resp_ prefixed).
func (s *MessagesToResponsesStream) ResponseID() string { return s.respID }

// FeedJSON consumes one Anthropic SSE data field (full JSON or [DONE]).
// Returns 0..n Responses events plus token/progress side-channel info.
func (s *MessagesToResponsesStream) FeedJSON(data string) (events []ResponsesStreamEvent, meta StreamChunkMeta) {
	if data == "[DONE]" {
		meta.EmitDone = true
		return nil, meta
	}

	var ev map[string]interface{}
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return nil, meta
	}

	eventType, _ := ev["type"].(string)
	switch eventType {
	case "message_start":
		if message, ok := ev["message"].(map[string]interface{}); ok {
			if id, ok := message["id"].(string); ok && id != "" {
				s.respID = normalizeResponsesID(id)
				s.idBase = stripMsgPrefix(id)
			}
			if m, ok := message["model"].(string); ok && m != "" && s.model == "" {
				s.model = m
			}
			if usage, ok := message["usage"].(map[string]interface{}); ok {
				s.usageInput = intFrom(usage["input_tokens"])
				s.usageCached = intFrom(usage["cache_read_input_tokens"])
				s.usageCacheCreation = intFrom(usage["cache_creation_input_tokens"])
				meta.InputTokens = s.usageInput + s.usageCached + s.usageCacheCreation
				meta.CachedTokens = s.usageCached
				meta.CacheCreationTokens = s.usageCacheCreation
				if ot := intFrom(usage["output_tokens"]); ot > 0 {
					s.usageOutput = ot
					meta.OutputTokens = ot
				}
			}
		}
		return s.emitStarted(), meta

	case "content_block_start":
		idx := intFrom(ev["index"])
		cb, _ := ev["content_block"].(map[string]interface{})
		if cb == nil {
			return nil, meta
		}
		cbType, _ := cb["type"].(string)
		item := &responsesStreamItem{outputIndex: s.nextOutput}
		s.nextOutput++
		switch cbType {
		case "thinking":
			item.kind = "thinking"
			item.itemID = fmt.Sprintf("rs_%s_%d", s.idBase, item.outputIndex)
		case "redacted_thinking":
			item.kind = "redacted_thinking"
			item.itemID = fmt.Sprintf("rs_%s_%d", s.idBase, item.outputIndex)
			item.signature, _ = cb["data"].(string)
		case "text":
			item.kind = "text"
			item.itemID = fmt.Sprintf("msg_%s_%d", s.idBase, item.outputIndex)
		case "tool_use":
			item.kind = "tool_use"
			item.callID, _ = cb["id"].(string)
			item.name, _ = cb["name"].(string)
			item.itemID = fmt.Sprintf("fc_%s", stripToolUsePrefix(item.callID))
		default:
			// server_tool_use / web_search_tool_result etc.: not convertible, skip.
			s.nextOutput--
			return nil, meta
		}
		s.items[idx] = item
		return s.emitItemAdded(item), meta

	case "content_block_delta":
		idx := intFrom(ev["index"])
		item, ok := s.items[idx]
		if !ok {
			return nil, meta
		}
		delta, _ := ev["delta"].(map[string]interface{})
		if delta == nil {
			return nil, meta
		}
		switch dt, _ := delta["type"].(string); dt {
		case "thinking_delta":
			thinking, _ := delta["thinking"].(string)
			if thinking == "" {
				return nil, meta
			}
			item.text += thinking
			meta.TransmittedChars = len(thinking)
			events = append(events, s.event("response.reasoning_summary_text.delta", map[string]interface{}{
				"type":          "response.reasoning_summary_text.delta",
				"response_id":   s.respID,
				"item_id":       item.itemID,
				"output_index":  item.outputIndex,
				"summary_index": 0,
				"delta":         thinking,
			}))
		case "signature_delta":
			sig, _ := delta["signature"].(string)
			item.signature += sig
		case "text_delta":
			text, _ := delta["text"].(string)
			if text == "" {
				return nil, meta
			}
			item.text += text
			meta.TransmittedChars = len(text)
			events = append(events, s.event("response.output_text.delta", map[string]interface{}{
				"type":          "response.output_text.delta",
				"response_id":   s.respID,
				"item_id":       item.itemID,
				"output_index":  item.outputIndex,
				"content_index": 0,
				"delta":         text,
			}))
		case "input_json_delta":
			partial, _ := delta["partial_json"].(string)
			if partial == "" {
				return nil, meta
			}
			item.text += partial
			meta.TransmittedChars = len(partial)
			events = append(events, s.event("response.function_call.arguments.delta", map[string]interface{}{
				"type":         "response.function_call.arguments.delta",
				"response_id":  s.respID,
				"item_id":      item.itemID,
				"output_index": item.outputIndex,
				"delta":        partial,
			}))
		}
		return events, meta

	case "content_block_stop":
		idx := intFrom(ev["index"])
		item, ok := s.items[idx]
		if !ok {
			return nil, meta
		}
		return s.closeItem(item), meta

	case "message_delta":
		if usage, ok := ev["usage"].(map[string]interface{}); ok {
			if ot := intFrom(usage["output_tokens"]); ot > 0 {
				s.usageOutput = ot
				meta.OutputTokens = ot
			}
		}
		if delta, ok := ev["delta"].(map[string]interface{}); ok {
			if sr, ok := delta["stop_reason"].(string); ok {
				s.stopReason = sr
			}
		}
		return nil, meta

	case "message_stop":
		// Close any items whose content_block_stop never arrived (defensive).
		for _, item := range s.sortedOpenItems() {
			events = append(events, s.closeItem(item)...)
		}
		events = append(events, s.finalEvents("completed")...)
		meta.EmitDone = true
		return events, meta

	case "error":
		errMap, _ := ev["error"].(map[string]interface{})
		msg, _ := errMap["message"].(string)
		if msg == "" {
			msg = "upstream anthropic error"
		}
		meta.ErrorMessage = msg
		events = append(events, s.finalEventsWithError(msg)...)
		meta.EmitDone = true
		return events, meta

	case "ping":
		return nil, meta
	}

	return nil, meta
}

// emitStarted sends response.created + response.in_progress once.
func (s *MessagesToResponsesStream) emitStarted() []ResponsesStreamEvent {
	if s.started {
		return nil
	}
	s.started = true
	response := func(status string) map[string]interface{} {
		return map[string]interface{}{
			"id":         s.respID,
			"object":     "response",
			"created_at": s.createdAt,
			"status":     status,
			"model":      s.model,
			"output":     []interface{}{},
		}
	}
	return []ResponsesStreamEvent{
		s.event("response.created", map[string]interface{}{"type": "response.created", "response": response("in_progress")}),
		s.event("response.in_progress", map[string]interface{}{"type": "response.in_progress", "response": response("in_progress")}),
	}
}

// emitItemAdded sends the item-open event(s) for a new content block.
func (s *MessagesToResponsesStream) emitItemAdded(item *responsesStreamItem) []ResponsesStreamEvent {
	// Late blocks can arrive before message_start in degenerate streams.
	events := s.emitStarted()

	var itemPayload map[string]interface{}
	switch item.kind {
	case "thinking", "redacted_thinking":
		itemPayload = map[string]interface{}{
			"id":      item.itemID,
			"type":    "reasoning",
			"summary": []interface{}{},
		}
	case "text":
		itemPayload = map[string]interface{}{
			"id":      item.itemID,
			"type":    "message",
			"status":  "in_progress",
			"role":    "assistant",
			"content": []interface{}{},
		}
	case "tool_use":
		itemPayload = map[string]interface{}{
			"id":        item.itemID,
			"call_id":   item.callID,
			"type":      "function_call",
			"status":    "in_progress",
			"name":      item.name,
			"arguments": "",
		}
	}
	events = append(events, s.event("response.output_item.added", map[string]interface{}{
		"type":         "response.output_item.added",
		"response_id":  s.respID,
		"output_index": item.outputIndex,
		"item":         itemPayload,
	}))

	switch item.kind {
	case "thinking":
		events = append(events, s.event("response.reasoning_summary_part.added", map[string]interface{}{
			"type":          "response.reasoning_summary_part.added",
			"response_id":   s.respID,
			"item_id":       item.itemID,
			"output_index":  item.outputIndex,
			"summary_index": 0,
			"part":          map[string]interface{}{"type": "summary_text", "text": ""},
		}))
	case "text":
		events = append(events, s.event("response.content_part.added", map[string]interface{}{
			"type":          "response.content_part.added",
			"response_id":   s.respID,
			"item_id":       item.itemID,
			"output_index":  item.outputIndex,
			"content_index": 0,
			"part":          map[string]interface{}{"type": "output_text", "text": "", "annotations": []interface{}{}},
		}))
	}
	return events
}

// closeItem emits the item-done event sequence and records the completed output item.
func (s *MessagesToResponsesStream) closeItem(item *responsesStreamItem) []ResponsesStreamEvent {
	if item.closed {
		return nil
	}
	item.closed = true

	var events []ResponsesStreamEvent
	var doneItem map[string]interface{}

	switch item.kind {
	case "thinking", "redacted_thinking":
		if item.kind == "thinking" {
			events = append(events,
				s.event("response.reasoning_summary_text.done", map[string]interface{}{
					"type":          "response.reasoning_summary_text.done",
					"response_id":   s.respID,
					"item_id":       item.itemID,
					"output_index":  item.outputIndex,
					"summary_index": 0,
					"text":          item.text,
				}),
				s.event("response.reasoning_summary_part.done", map[string]interface{}{
					"type":          "response.reasoning_summary_part.done",
					"response_id":   s.respID,
					"item_id":       item.itemID,
					"output_index":  item.outputIndex,
					"summary_index": 0,
					"part":          map[string]interface{}{"type": "summary_text", "text": item.text},
				}),
			)
		}
		summary := []interface{}{}
		if item.text != "" {
			summary = append(summary, map[string]interface{}{"type": "summary_text", "text": item.text})
		}
		doneItem = map[string]interface{}{
			"id":      item.itemID,
			"type":    "reasoning",
			"summary": summary,
		}
		if item.signature != "" {
			doneItem["encrypted_content"] = item.signature
		}
		events = append(events, s.itemDoneEvent(item, doneItem))

	case "text":
		events = append(events,
			s.event("response.output_text.done", map[string]interface{}{
				"type":          "response.output_text.done",
				"response_id":   s.respID,
				"item_id":       item.itemID,
				"output_index":  item.outputIndex,
				"content_index": 0,
				"text":          item.text,
			}),
			s.event("response.content_part.done", map[string]interface{}{
				"type":          "response.content_part.done",
				"response_id":   s.respID,
				"item_id":       item.itemID,
				"output_index":  item.outputIndex,
				"content_index": 0,
				"part":          map[string]interface{}{"type": "output_text", "text": item.text, "annotations": []interface{}{}},
			}),
		)
		doneItem = map[string]interface{}{
			"id":     item.itemID,
			"type":   "message",
			"status": "completed",
			"role":   "assistant",
			"content": []interface{}{
				map[string]interface{}{"type": "output_text", "text": item.text, "annotations": []interface{}{}},
			},
		}
		events = append(events, s.itemDoneEvent(item, doneItem))

	case "tool_use":
		events = append(events,
			s.event("response.function_call.arguments.done", map[string]interface{}{
				"type":         "response.function_call.arguments.done",
				"response_id":  s.respID,
				"item_id":      item.itemID,
				"output_index": item.outputIndex,
				"arguments":    item.text,
			}),
		)
		doneItem = map[string]interface{}{
			"id":        item.itemID,
			"call_id":   item.callID,
			"type":      "function_call",
			"status":    "completed",
			"name":      item.name,
			"arguments": item.text,
		}
		events = append(events, s.itemDoneEvent(item, doneItem))
	}

	if doneItem != nil {
		s.completed = append(s.completed, doneItem)
	}
	return events
}

// finalEvents emits response.done + response.completed (double-send for old clients).
func (s *MessagesToResponsesStream) finalEvents(status string) []ResponsesStreamEvent {
	response := s.finalResponse(status)
	return []ResponsesStreamEvent{
		s.event("response.done", map[string]interface{}{"type": "response.done", "response": response}),
		s.event("response.completed", map[string]interface{}{"type": "response.completed", "response": response}),
	}
}

// finalEventsWithError emits response.failed + response.completed(status=failed).
func (s *MessagesToResponsesStream) finalEventsWithError(message string) []ResponsesStreamEvent {
	response := s.finalResponse("failed")
	response["error"] = map[string]interface{}{"message": message, "type": "upstream_error"}
	return []ResponsesStreamEvent{
		s.event("response.failed", map[string]interface{}{"type": "response.failed", "response": response}),
		s.event("response.completed", map[string]interface{}{"type": "response.completed", "response": response}),
	}
}

// finalResponse builds the full response object with output items and usage.
func (s *MessagesToResponsesStream) finalResponse(status string) map[string]interface{} {
	output := s.completed
	if output == nil {
		output = []map[string]interface{}{}
	}
	totalInput := s.usageInput + s.usageCached + s.usageCacheCreation
	resp := map[string]interface{}{
		"id":         s.respID,
		"object":     "response",
		"created_at": s.createdAt,
		"status":     status,
		"model":      s.model,
		"output":     output,
		"usage": map[string]interface{}{
			"input_tokens":  totalInput,
			"output_tokens": s.usageOutput,
			"total_tokens":  totalInput + s.usageOutput,
			"input_tokens_details": map[string]interface{}{
				"cached_tokens": s.usageCached,
			},
		},
	}
	if status == "completed" && s.stopReason == "max_tokens" {
		resp["status"] = "incomplete"
		resp["incomplete_details"] = map[string]interface{}{"reason": "max_output_tokens"}
	}
	return resp
}

// sortedOpenItems returns unclosed items ordered by output index.
func (s *MessagesToResponsesStream) sortedOpenItems() []*responsesStreamItem {
	var open []*responsesStreamItem
	for _, item := range s.items {
		if !item.closed {
			open = append(open, item)
		}
	}
	for i := 0; i < len(open); i++ {
		for j := i + 1; j < len(open); j++ {
			if open[j].outputIndex < open[i].outputIndex {
				open[i], open[j] = open[j], open[i]
			}
		}
	}
	return open
}

func (s *MessagesToResponsesStream) itemDoneEvent(item *responsesStreamItem, doneItem map[string]interface{}) ResponsesStreamEvent {
	return s.event("response.output_item.done", map[string]interface{}{
		"type":         "response.output_item.done",
		"response_id":  s.respID,
		"output_index": item.outputIndex,
		"item":         doneItem,
	})
}

// event serializes one Responses SSE frame (payload must contain "type").
func (s *MessagesToResponsesStream) event(eventType string, payload map[string]interface{}) ResponsesStreamEvent {
	data, err := json.Marshal(payload)
	if err != nil {
		return ResponsesStreamEvent{Event: eventType, Data: []byte("{}")}
	}
	return ResponsesStreamEvent{Event: eventType, Data: data}
}
