package translate

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ChatToResponsesStream 是请求级纯翻译 FSM；caller 负责 framing、I/O 和错误预扫描。
type ChatToResponsesStream struct {
	model                string
	mapper               *ToolNameMapper
	usage                TokenUsage
	started              bool
	finished             bool
	sawDone              bool
	finishReason         string
	lastResponseID       string
	lastModelName        string
	fullText             strings.Builder
	fullReasoning        strings.Builder
	messageAdded         bool
	textOutputIndex      int
	currentOutputIndex   int
	reasoningAdded       bool
	reasoningOutputIndex int
	localToolCalls       map[int]*chatResponsesToolCall
}

type chatResponsesToolCall struct {
	ID          string
	CallID      string
	Name        string
	Namespace   string
	Arguments   strings.Builder
	OutputIndex int
	Added       bool
	IsCustom    bool
}

func NewChatToResponsesStream(model string, mapper *ToolNameMapper, initialUsage TokenUsage) *ChatToResponsesStream {
	return &ChatToResponsesStream{model: model, mapper: mapper, usage: initialUsage, textOutputIndex: -1, reasoningOutputIndex: -1, localToolCalls: make(map[int]*chatResponsesToolCall)}
}

// FeedJSON 消费完整 Chat JSON data；finish_reason 不触发提前完成，允许后续 usage。
func (s *ChatToResponsesStream) FeedJSON(data string) (events []ResponsesStreamEvent, meta StreamChunkMeta) {
	if s.finished {
		return nil, meta
	}
	if strings.TrimSpace(data) == "[DONE]" {
		s.sawDone = true
		s.progressMeta(&meta)
		return nil, meta
	}
	// 原 SSEParser 的 usage 解码独立于内容解码，辅助字段格式错不能吞掉有效 delta。
	usage := chatStreamUsage(data)
	meta.InputTokens = usage.InputTokens
	meta.OutputTokens = usage.OutputTokens
	meta.CachedTokens = usage.CachedTokens
	s.applyUsage(usage)
	var finishChunk struct {
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(data), &finishChunk) == nil {
		for _, choice := range finishChunk.Choices {
			if choice.FinishReason != nil {
				s.finishReason = *choice.FinishReason
			}
		}
	}
	var chunk struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Delta struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				Reasoning        string `json:"reasoning"`
				ToolCalls        []struct {
					Index    int    `json:"index"`
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
	}

	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return nil, meta
	}

	if chunk.ID != "" {
		s.lastResponseID = chunk.ID
		meta.ResponseID = chatResponsesID(chunk.ID, "resp_")
	}
	if chunk.Model != "" {
		s.lastModelName = chunk.Model
		meta.ResponseModel = chunk.Model
	}

	respID := chatResponsesID(s.lastResponseID, "resp_")

	msgID := chatResponsesID(s.lastResponseID, "msg_")

	reasoningID := chatResponsesID(s.lastResponseID, "rs_")

	modelName := s.lastModelName
	if modelName == "" {
		modelName = s.model
	}

	if !s.started {
		s.started = true
		now := time.Now().Unix()

		// 1. response.created
		var evCreated responseCreatedEvent
		evCreated.Type = "response.created"
		evCreated.Response.ID = respID
		evCreated.Response.Object = "response"
		evCreated.Response.CreatedAt = now
		evCreated.Response.Status = "in_progress"
		evCreated.Response.Model = modelName
		evCreated.Response.Output = []interface{}{}
		appendChatResponsesEvent(&events, "response.created", evCreated)

		// 2. response.in_progress
		var evInProgress responseInProgressEvent
		evInProgress.Type = "response.in_progress"
		evInProgress.Response.ID = respID
		evInProgress.Response.Object = "response"
		evInProgress.Response.CreatedAt = now
		evInProgress.Response.Status = "in_progress"
		evInProgress.Response.Model = modelName
		evInProgress.Response.Output = []interface{}{}
		appendChatResponsesEvent(&events, "response.in_progress", evInProgress)
		events[len(events)-1].Flush = true
	}

	if len(chunk.Choices) > 0 {
		meta.Flush = true
		choice := chunk.Choices[0]

		// Process reasoning text (thinking process)
		reasoning := chatResponsesReasoning(choice.Delta.ReasoningContent, choice.Delta.Reasoning)
		if reasoning != "" {
			s.sendResponsesReasoningDelta(&events, respID, reasoningID, reasoning)
			s.fullReasoning.WriteString(reasoning)
			meta.TransmittedChars += len(reasoning)
		}

		// Process text
		txt := choice.Delta.Content
		if txt != "" {
			s.sendPlainResponsesText(&events, respID, txt, msgID)
			s.fullText.WriteString(txt)
			meta.TransmittedChars += len(txt)
		}

		// Process tool calls
		if len(choice.Delta.ToolCalls) > 0 {
			for _, tc := range choice.Delta.ToolCalls {
				localTC, exist := s.localToolCalls[tc.Index]
				if !exist {
					localTC = &chatResponsesToolCall{}
					callID := tc.ID
					if callID == "" {
						callID = fmt.Sprintf("call_%s_%d", msgID, tc.Index)
					}
					localTC.CallID = callID
					localTC.ID = EnsureFunctionCallItemID(callID)
					localTC.Name = tc.Function.Name
					localTC.OutputIndex = s.currentOutputIndex
					s.currentOutputIndex++
					s.localToolCalls[tc.Index] = localTC
				}
				if tc.ID != "" && localTC.CallID == "" {
					localTC.CallID = tc.ID
					localTC.ID = EnsureFunctionCallItemID(tc.ID)
				}
				if tc.Function.Name != "" && localTC.Name == "" {
					localTC.Name = tc.Function.Name
				}
				if localTC.Namespace == "" {
					if s.mapper != nil {
						localTC.Namespace, localTC.Name = s.mapper.Restore(localTC.Name)
					} else {
						localTC.Namespace, localTC.Name = splitChatToolName(localTC.Name)
					}
				}
				if localTC.Name == "apply_patch" || (s.mapper != nil && s.mapper.IsCustom(localTC.Name)) {
					localTC.IsCustom = true
					localTC.ID = EnsureCustomToolCallItemID(localTC.CallID)
				}

				// Once we have a tool name and haven't sent the added event yet, send it immediately
				if localTC.Name != "" && !localTC.Added {
					localTC.Added = true
					if localTC.IsCustom {
						var evTCAdded responseOutputItemAddedCustomToolCallEvent
						evTCAdded.Type = "response.output_item.added"
						evTCAdded.ResponseID = respID
						evTCAdded.OutputIndex = localTC.OutputIndex
						evTCAdded.Item.ID = localTC.ID
						evTCAdded.Item.CallID = localTC.CallID
						evTCAdded.Item.Type = "custom_tool_call"
						evTCAdded.Item.Status = "in_progress"
						evTCAdded.Item.Name = localTC.Name
						evTCAdded.Item.Namespace = localTC.Namespace
						evTCAdded.Item.Input = ""
						appendChatResponsesEvent(&events, "response.output_item.added", evTCAdded)
					} else {
						var evTCAdded responseOutputItemAddedFunctionCallEvent
						evTCAdded.Type = "response.output_item.added"
						evTCAdded.ResponseID = respID
						evTCAdded.OutputIndex = localTC.OutputIndex
						evTCAdded.Item.ID = localTC.ID
						evTCAdded.Item.CallID = localTC.CallID
						evTCAdded.Item.Type = "function_call"
						evTCAdded.Item.Status = "in_progress"
						evTCAdded.Item.Name = localTC.Name
						evTCAdded.Item.Namespace = localTC.Namespace
						evTCAdded.Item.Arguments = ""
						appendChatResponsesEvent(&events, "response.output_item.added", evTCAdded)
					}
				}

				argDelta := tc.Function.Arguments
				localTC.Arguments.WriteString(argDelta)

				// Send arguments delta only for standard function calls
				if !localTC.IsCustom {
					var evTCDelta responseFunctionCallArgumentsDeltaEvent
					evTCDelta.Type = "response.function_call.arguments.delta"
					evTCDelta.ResponseID = respID
					evTCDelta.ItemID = localTC.ID
					evTCDelta.CallID = localTC.CallID
					evTCDelta.OutputIndex = localTC.OutputIndex
					evTCDelta.Delta = argDelta
					appendChatResponsesEvent(&events, "response.function_call.arguments.delta", evTCDelta)
				}
			}
		}
	}
	s.progressMeta(&meta)
	return events, meta
}

// Finish 保留 started EOF 补成功，以及 response.done/response.completed 双事件。
// caller-latest 有效 usage 必须覆盖本地值，因为 output interceptor 也可能更新计数。
func (s *ChatToResponsesStream) Finish(latestUsage TokenUsage) (events []ResponsesStreamEvent, meta StreamChunkMeta) {
	if s.finished {
		return nil, meta
	}
	s.finished = true
	s.applyUsage(latestUsage)
	s.progressMeta(&meta)
	if !s.started {
		return nil, meta
	}
	respID := chatResponsesID(s.lastResponseID, "resp_")

	msgID := chatResponsesID(s.lastResponseID, "msg_")

	reasoningID := chatResponsesID(s.lastResponseID, "rs_")

	modelName := s.lastModelName
	if modelName == "" {
		modelName = s.model
	}

	now := time.Now().Unix()

	// Finalize reasoning item
	if s.reasoningAdded {
		finalReasoning := s.fullReasoning.String()
		reasoningDoneItem := completedChatReasoningItem(reasoningID, finalReasoning)
		reasoningEvents := []struct {
			name    string
			payload map[string]interface{}
		}{
			{
				name: "response.reasoning_summary_text.done",
				payload: map[string]interface{}{
					"type":          "response.reasoning_summary_text.done",
					"response_id":   respID,
					"item_id":       reasoningID,
					"output_index":  s.reasoningOutputIndex,
					"summary_index": 0,
					"text":          finalReasoning,
				},
			},
			{
				name: "response.reasoning_summary_part.done",
				payload: map[string]interface{}{
					"type":          "response.reasoning_summary_part.done",
					"response_id":   respID,
					"item_id":       reasoningID,
					"output_index":  s.reasoningOutputIndex,
					"summary_index": 0,
					"part":          reasoningSummaryPart(finalReasoning),
				},
			},
			{
				name: "response.output_item.done",
				payload: map[string]interface{}{
					"type":         "response.output_item.done",
					"response_id":  respID,
					"output_index": s.reasoningOutputIndex,
					"item":         reasoningDoneItem,
				},
			},
		}
		for _, ev := range reasoningEvents {
			appendChatResponsesEvent(&events, ev.name, ev.payload)
		}
	}

	// Finalize text message
	if s.messageAdded {
		finalText := s.fullText.String()

		// 5. response.output_text.done
		var evTextDone responseOutputTextDoneEvent
		evTextDone.Type = "response.output_text.done"
		evTextDone.ResponseID = respID
		evTextDone.ItemID = msgID
		evTextDone.OutputIndex = s.textOutputIndex
		evTextDone.ContentIndex = 0
		evTextDone.Text = finalText
		appendChatResponsesEvent(&events, "response.output_text.done", evTextDone)

		// 6. response.content_part.done
		var evPartDone responseContentPartDoneEvent
		evPartDone.Type = "response.content_part.done"
		evPartDone.ResponseID = respID
		evPartDone.ItemID = msgID
		evPartDone.OutputIndex = s.textOutputIndex
		evPartDone.ContentIndex = 0
		evPartDone.Part.Type = "output_text"
		evPartDone.Part.Text = finalText
		evPartDone.Part.Annotations = []interface{}{}
		appendChatResponsesEvent(&events, "response.content_part.done", evPartDone)

		// 7. response.output_item.done
		var evItemDone responseOutputItemDoneEvent
		evItemDone.Type = "response.output_item.done"
		evItemDone.ResponseID = respID
		evItemDone.OutputIndex = s.textOutputIndex
		evItemDone.Item.ID = msgID
		evItemDone.Item.Type = "message"
		evItemDone.Item.Status = "completed"
		evItemDone.Item.Role = "assistant"
		evItemDone.Item.Content = []interface{}{
			map[string]interface{}{
				"type":        "output_text",
				"text":        finalText,
				"annotations": []interface{}{},
			},
		}
		appendChatResponsesEvent(&events, "response.output_item.done", evItemDone)
	}

	// Finalize tool calls
	var outputs []interface{}
	if s.reasoningAdded {
		outputs = append(outputs, completedChatReasoningItem(reasoningID, s.fullReasoning.String()))
	}
	if s.messageAdded {
		outputs = append(outputs, map[string]interface{}{
			"id":     msgID,
			"type":   "message",
			"status": "completed",
			"role":   "assistant",
			"content": []interface{}{
				map[string]interface{}{
					"type":        "output_text",
					"text":        s.fullText.String(),
					"annotations": []interface{}{},
				},
			},
		})
	}

	// Iterate tool call completion events in index order
	var indices []int
	for idx := range s.localToolCalls {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	for _, idx := range indices {
		tc := s.localToolCalls[idx]
		finalArgs := tc.Arguments.String()

		if tc.IsCustom {
			rawPatch := ExtractPatchInput(finalArgs)
			var evTCItemDone responseOutputItemDoneCustomToolCallEvent
			evTCItemDone.Type = "response.output_item.done"
			evTCItemDone.ResponseID = respID
			evTCItemDone.OutputIndex = tc.OutputIndex
			evTCItemDone.Item.ID = tc.ID
			evTCItemDone.Item.CallID = tc.CallID
			evTCItemDone.Item.Type = "custom_tool_call"
			evTCItemDone.Item.Status = "completed"
			evTCItemDone.Item.Name = tc.Name
			evTCItemDone.Item.Namespace = tc.Namespace
			evTCItemDone.Item.Input = rawPatch
			appendChatResponsesEvent(&events, "response.output_item.done", evTCItemDone)

			outputs = append(outputs, map[string]interface{}{
				"id":        tc.ID,
				"call_id":   tc.CallID,
				"type":      "custom_tool_call",
				"status":    "completed",
				"name":      tc.Name,
				"namespace": tc.Namespace,
				"input":     rawPatch,
			})
		} else {
			// Send arguments done
			var evTCDone responseFunctionCallArgumentsDoneEvent
			evTCDone.Type = "response.function_call.arguments.done"
			evTCDone.ResponseID = respID
			evTCDone.ItemID = tc.ID
			evTCDone.CallID = tc.CallID
			evTCDone.OutputIndex = tc.OutputIndex
			evTCDone.Arguments = finalArgs
			appendChatResponsesEvent(&events, "response.function_call.arguments.done", evTCDone)

			// Send output_item.done
			var evTCItemDone responseOutputItemDoneFunctionCallEvent
			evTCItemDone.Type = "response.output_item.done"
			evTCItemDone.ResponseID = respID
			evTCItemDone.OutputIndex = tc.OutputIndex
			evTCItemDone.Item.ID = tc.ID
			evTCItemDone.Item.CallID = tc.CallID
			evTCItemDone.Item.Type = "function_call"
			evTCItemDone.Item.Status = "completed"
			evTCItemDone.Item.Name = tc.Name
			evTCItemDone.Item.Namespace = tc.Namespace
			evTCItemDone.Item.Arguments = finalArgs
			appendChatResponsesEvent(&events, "response.output_item.done", evTCItemDone)

			outputs = append(outputs, map[string]interface{}{
				"id":        tc.ID,
				"call_id":   tc.CallID,
				"type":      "function_call",
				"status":    "completed",
				"name":      tc.Name,
				"namespace": tc.Namespace,
				"arguments": finalArgs,
			})
		}
	}

	// 8. response.done
	var evCompleted responseDoneEvent
	evCompleted.Type = "response.done"
	evCompleted.Response.ID = respID
	evCompleted.Response.Object = "response"
	evCompleted.Response.CreatedAt = now - 1
	evCompleted.Response.Status = "completed"
	evCompleted.Response.Model = modelName
	evCompleted.Response.Output = outputs
	evCompleted.Response.Usage.InputTokens = s.usage.InputTokens
	evCompleted.Response.Usage.OutputTokens = s.usage.OutputTokens
	evCompleted.Response.Usage.TotalTokens = s.usage.InputTokens + s.usage.OutputTokens
	appendChatResponsesEvent(&events, "response.done", evCompleted)

	// Also send response.completed for old client compatibility to avoid indefinite waiting timeout
	evCompleted.Type = "response.completed"
	appendChatResponsesEvent(&events, "response.completed", evCompleted)

	meta.Completed, meta.EmitDone = true, true
	meta.InputTokens, meta.OutputTokens = s.usage.InputTokens, s.usage.OutputTokens
	meta.CachedTokens, meta.CacheCreationTokens = s.usage.CachedTokens, s.usage.CacheCreationTokens
	return events, meta
}

func (s *ChatToResponsesStream) progressMeta(meta *StreamChunkMeta) {
	meta.SawDone = s.sawDone
	meta.FinishReason = s.finishReason
	meta.TextChars = s.fullText.Len()
	meta.ThinkingChars = s.fullReasoning.Len()
	meta.HasToolUse = len(s.localToolCalls) > 0
}

func (s *ChatToResponsesStream) applyUsage(usage TokenUsage) {
	if usage.InputTokens > 0 {
		s.usage.InputTokens = usage.InputTokens
	}
	if usage.OutputTokens > 0 {
		s.usage.OutputTokens = usage.OutputTokens
	}
	if usage.CachedTokens > 0 {
		s.usage.CachedTokens = usage.CachedTokens
	}
	if usage.CacheCreationTokens > 0 {
		s.usage.CacheCreationTokens = usage.CacheCreationTokens
	}
}

func appendChatResponsesEvent(events *[]ResponsesStreamEvent, event string, payload interface{}) {
	data, _ := json.Marshal(payload) // 本模块只封装可序列化的固定 shape。
	*events = append(*events, ResponsesStreamEvent{Event: event, Data: data})
}

func (s *ChatToResponsesStream) sendPlainResponsesText(events *[]ResponsesStreamEvent, respID, text, msgID string) {
	if !s.messageAdded {
		s.messageAdded = true
		s.textOutputIndex = s.currentOutputIndex
		var item responseOutputItemAddedEvent
		item.Type = "response.output_item.added"
		item.ResponseID = respID
		item.OutputIndex = s.textOutputIndex
		item.Item.ID = msgID
		item.Item.Type = "message"
		item.Item.Status = "in_progress"
		item.Item.Role = "assistant"
		item.Item.Content = []interface{}{}
		appendChatResponsesEvent(events, item.Type, item)
		var part responseContentPartAddedEvent
		part.Type = "response.content_part.added"
		part.ResponseID = respID
		part.ItemID = msgID
		part.OutputIndex = s.textOutputIndex
		part.Part.Type = "output_text"
		part.Part.Annotations = []interface{}{}
		appendChatResponsesEvent(events, part.Type, part)
		s.currentOutputIndex++
	}
	var delta responseOutputTextDeltaEvent
	delta.Type = "response.output_text.delta"
	delta.ResponseID = respID
	delta.ItemID = msgID
	delta.OutputIndex = s.textOutputIndex
	delta.Delta = text
	appendChatResponsesEvent(events, delta.Type, delta)
	(*events)[len(*events)-1].TransmittedChars = len(text)
}

func (s *ChatToResponsesStream) sendResponsesReasoningDelta(events *[]ResponsesStreamEvent, respID, reasoningID, delta string) {
	if !s.reasoningAdded {
		s.reasoningAdded = true
		s.reasoningOutputIndex = s.currentOutputIndex
		s.currentOutputIndex++
		appendChatResponsesEvent(events, "response.output_item.added", map[string]interface{}{
			"type": "response.output_item.added", "response_id": respID, "output_index": s.reasoningOutputIndex,
			"item": responsesReasoningItem(reasoningID, []interface{}{}, ""),
		})
		appendChatResponsesEvent(events, "response.reasoning_summary_part.added", map[string]interface{}{
			"type": "response.reasoning_summary_part.added", "response_id": respID, "item_id": reasoningID,
			"output_index": s.reasoningOutputIndex, "summary_index": 0,
			"part": reasoningSummaryPart(""),
		})
	}
	appendChatResponsesEvent(events, "response.reasoning_summary_text.delta", map[string]interface{}{
		"type": "response.reasoning_summary_text.delta", "response_id": respID, "item_id": reasoningID,
		"output_index": s.reasoningOutputIndex, "summary_index": 0, "delta": delta,
	})
	(*events)[len(*events)-1].TransmittedChars = len(delta)
}

type responseCreatedEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
	} `json:"response"`
}

type responseInProgressEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
	} `json:"response"`
}

type responseOutputItemAddedEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID      string        `json:"id"`
		Type    string        `json:"type"`
		Status  string        `json:"status"`
		Role    string        `json:"role"`
		Content []interface{} `json:"content"`
	} `json:"item"`
}

type responseContentPartAddedEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Part         struct {
		Type        string        `json:"type"`
		Text        string        `json:"text"`
		Annotations []interface{} `json:"annotations"`
	} `json:"part"`
}

type responseOutputTextDeltaEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

type responseOutputTextDoneEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Text         string `json:"text"`
}

type responseContentPartDoneEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Part         struct {
		Type        string        `json:"type"`
		Text        string        `json:"text"`
		Annotations []interface{} `json:"annotations"`
	} `json:"part"`
}

type responseOutputItemDoneEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID      string        `json:"id"`
		Type    string        `json:"type"`
		Status  string        `json:"status"`
		Role    string        `json:"role"`
		Content []interface{} `json:"content"`
	} `json:"item"`
}

type responseDoneEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
		Usage     struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
	} `json:"response"`
}

type responseOutputItemAddedFunctionCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
		Namespace string `json:"namespace,omitempty"`
	} `json:"item"`
}

type responseFunctionCallArgumentsDeltaEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	ItemID      string `json:"item_id"`
	CallID      string `json:"call_id"`
	OutputIndex int    `json:"output_index"`
	Delta       string `json:"delta"`
}

type responseFunctionCallArgumentsDoneEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	ItemID      string `json:"item_id"`
	CallID      string `json:"call_id"`
	OutputIndex int    `json:"output_index"`
	Arguments   string `json:"arguments"`
}

type responseOutputItemDoneFunctionCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
		Namespace string `json:"namespace,omitempty"`
	} `json:"item"`
}

type responseOutputItemAddedCustomToolCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Input     string `json:"input"`
		Namespace string `json:"namespace,omitempty"`
	} `json:"item"`
}

type responseOutputItemDoneCustomToolCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		CallID    string `json:"call_id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Input     string `json:"input"`
		Namespace string `json:"namespace,omitempty"`
	} `json:"item"`
}
