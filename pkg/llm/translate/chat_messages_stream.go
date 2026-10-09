package translate

import (
	"encoding/json"
	"sort"
)

// MessagesStreamEvent 是一帧序列化后的 Messages SSE 事件，由 caller 写入 SSE framing。
type MessagesStreamEvent struct {
	Event            string
	Data             []byte
	TransmittedChars int  // caller 成功写出后累加；仅 text/thinking delta 计入
	Flush            bool // 保留原 Provider 的刷新点，不执行 I/O
}

type messageStartEvent struct {
	Type    string `json:"type"`
	Message struct {
		ID           string      `json:"id"`
		Type         string      `json:"type"`
		Role         string      `json:"role"`
		Content      []string    `json:"content"`
		Model        string      `json:"model"`
		StopReason   *string     `json:"stop_reason"`
		StopSequence interface{} `json:"stop_sequence"`
		Usage        struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

type contentBlockStartEvent struct {
	Type         string `json:"type"`
	Index        int    `json:"index"`
	ContentBlock struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content_block"`
}

type contentBlockDeltaEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	Delta struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"delta"`
}

type contentBlockStopEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
}

type messageDeltaEvent struct {
	Type  string `json:"type"`
	Delta struct {
		StopReason   string      `json:"stop_reason"`
		StopSequence interface{} `json:"stop_sequence"`
	} `json:"delta"`
	Usage struct {
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

type messageStopEvent struct {
	Type string `json:"type"`
}

// ChatToMessagesStream 是请求级 Chat→Messages FSM，不处理 framing、I/O 或 GatewayContext。
type ChatToMessagesStream struct {
	model               string
	usage               TokenUsage
	started             bool
	finished            bool
	lastMessageID       string
	sawDone             bool
	finishReason        string
	textChars           int
	thinkingChars       int
	hasToolUse          bool
	activeBlocks        map[int]bool
	oaiToAnthropicIndex map[int]int
	nextBlockIndex      int
	thinkingBlockIndex  int
	textBlockIndex      int
}

// NewChatToMessagesStream 使用 caller 的客户端模型名和已有 usage 初始化。
func NewChatToMessagesStream(model string, initialUsage TokenUsage) *ChatToMessagesStream {
	return &ChatToMessagesStream{
		model: model, usage: initialUsage,
		activeBlocks: make(map[int]bool), oaiToAnthropicIndex: make(map[int]int),
		thinkingBlockIndex: -1, textBlockIndex: -1,
	}
}

// FeedJSON 接收一帧完整 Chat JSON 或 [DONE]，完成信号只记录，终止事件留给 EOF 的 Finish。
func (s *ChatToMessagesStream) FeedJSON(data string) (events []MessagesStreamEvent, meta StreamChunkMeta) {
	if s.finished {
		return nil, s.diagnostics()
	}
	if data == "[DONE]" {
		s.sawDone = true
		return nil, s.diagnostics()
	}

	// 与原 SSE parser 一样，usage 先于内容解析和 message_start 更新；零值不清已有值。
	usage := chatStreamUsage(data)
	meta.InputTokens = usage.InputTokens
	meta.OutputTokens = usage.OutputTokens
	meta.CachedTokens = usage.CachedTokens
	if meta.InputTokens > 0 {
		s.usage.InputTokens = meta.InputTokens
	}
	if meta.OutputTokens > 0 {
		s.usage.OutputTokens = meta.OutputTokens
	}
	if meta.CachedTokens > 0 {
		s.usage.CachedTokens = meta.CachedTokens
	}

	var chunk struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Delta struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
				Thinking         string `json:"thinking"`
				Reasoning        string `json:"reasoning"`
				Thought          string `json:"thought"`
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
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(data), &chunk); err == nil {
		if chunk.ID != "" {
			s.lastMessageID = chunk.ID
		}
		if len(chunk.Choices) > 0 {
			choice := chunk.Choices[0]
			thinkingText := chatMessagesStreamReasoning(choice.Delta.ReasoningContent, choice.Delta.Thinking, choice.Delta.Reasoning, choice.Delta.Thought)
			if thinkingText != "" {
				s.startMessage(&events)
				idx := s.ensureBlock("thinking", &events)
				s.emitThinkingDelta(idx, thinkingText, &events)
				meta.TransmittedChars += len(thinkingText)
			}
			if choice.Delta.Content != "" {
				s.startMessage(&events)
				idx := s.ensureBlock("text", &events)
				s.emitTextDelta(idx, choice.Delta.Content, &events)
				meta.TransmittedChars += len(choice.Delta.Content)
			}
			if len(choice.Delta.ToolCalls) > 0 {
				s.startMessage(&events)
				for _, tc := range choice.Delta.ToolCalls {
					eventStart := len(events)
					idx, mapped := s.oaiToAnthropicIndex[tc.Index]
					if !mapped {
						idx = s.nextBlockIndex
						s.oaiToAnthropicIndex[tc.Index] = idx
						s.nextBlockIndex++
					}
					if !s.activeBlocks[idx] {
						s.activeBlocks[idx] = true
						var blockStartEv struct {
							Type         string `json:"type"`
							Index        int    `json:"index"`
							ContentBlock struct {
								Type  string                 `json:"type"`
								ID    string                 `json:"id"`
								Name  string                 `json:"name"`
								Input map[string]interface{} `json:"input"`
							} `json:"content_block"`
						}
						blockStartEv.Type = "content_block_start"
						blockStartEv.Index = idx
						blockStartEv.ContentBlock.Type = "tool_use"
						blockStartEv.ContentBlock.ID = NormalizeToolUseID(tc.ID)
						blockStartEv.ContentBlock.Name = tc.Function.Name
						blockStartEv.ContentBlock.Input = make(map[string]interface{})
						appendMessagesEvent(&events, "content_block_start", blockStartEv)
					}
					s.hasToolUse = true
					if tc.Function.Arguments != "" {
						var deltaEv struct {
							Type  string `json:"type"`
							Index int    `json:"index"`
							Delta struct {
								Type        string `json:"type"`
								PartialJSON string `json:"partial_json"`
							} `json:"delta"`
						}
						deltaEv.Type = "content_block_delta"
						deltaEv.Index = idx
						deltaEv.Delta.Type = "input_json_delta"
						deltaEv.Delta.PartialJSON = tc.Function.Arguments
						appendMessagesEvent(&events, "content_block_delta", deltaEv)
					}
					if len(events) > eventStart {
						events[len(events)-1].Flush = true
					} else {
						meta.Flush = true
					}
				}
			}
			if choice.FinishReason != nil && *choice.FinishReason != "" {
				s.finishReason = *choice.FinishReason
			}
		}
	}
	diag := s.diagnostics()
	diag.InputTokens = meta.InputTokens
	diag.OutputTokens = meta.OutputTokens
	diag.CachedTokens = meta.CachedTokens
	diag.TransmittedChars = meta.TransmittedChars
	diag.Flush = meta.Flush
	return events, diag
}

// Finish 只在正常 EOF 调用；Completed 表示生成了成功终止事件，不代表 caller 已写出。
func (s *ChatToMessagesStream) Finish(latestUsage TokenUsage) (events []MessagesStreamEvent, meta StreamChunkMeta) {
	if s.finished {
		return nil, s.diagnostics()
	}
	s.finished = true
	s.usage = latestUsage
	if s.finishReason != "" || s.sawDone {
		stopReason := mapOpenAIFinishReason(s.finishReason)
		s.finalizeSuccess(stopReason, &events)
		meta = s.diagnostics()
		meta.Completed = true
		meta.StopReason = stopReason
	} else {
		meta = s.diagnostics()
		if s.started {
			s.closeOpenBlocks(&events)
			if len(events) > 0 {
				events[len(events)-1].Flush = true
			}
			meta.ErrorMessage = "upstream stream closed prematurely without completion event"
		} else {
			meta.ErrorMessage = "empty upstream stream: no content or completion signal received"
		}
	}
	meta.InputTokens = latestUsage.InputTokens
	meta.OutputTokens = latestUsage.OutputTokens
	meta.CachedTokens = latestUsage.CachedTokens
	meta.CacheCreationTokens = latestUsage.CacheCreationTokens
	return events, meta
}

func (s *ChatToMessagesStream) diagnostics() StreamChunkMeta {
	return StreamChunkMeta{
		ResponseID: s.lastMessageID, ResponseModel: s.model,
		FinishReason: s.finishReason, SawDone: s.sawDone,
		TextChars: s.textChars, ThinkingChars: s.thinkingChars, HasToolUse: s.hasToolUse,
	}
}

func appendMessagesEvent(events *[]MessagesStreamEvent, eventType string, data interface{}) {
	// 事件仅含固定 struct、字符串和空 map，不含不可序列化值。
	jsonData, _ := json.Marshal(data)
	*events = append(*events, MessagesStreamEvent{Event: eventType, Data: jsonData})
}

func (s *ChatToMessagesStream) startMessage(events *[]MessagesStreamEvent) {
	if s.started {
		return
	}
	s.started = true
	var startEv messageStartEvent
	startEv.Type = "message_start"
	startEv.Message.ID = NormalizeAnthropicID(s.lastMessageID)
	startEv.Message.Type = "message"
	startEv.Message.Role = "assistant"
	startEv.Message.Content = []string{}
	startEv.Message.Model = s.model
	startEv.Message.Usage.InputTokens = s.usage.InputTokens
	startEv.Message.Usage.OutputTokens = s.usage.OutputTokens
	appendMessagesEvent(events, "message_start", startEv)
	(*events)[len(*events)-1].Flush = true
}

func (s *ChatToMessagesStream) ensureBlock(blockType string, events *[]MessagesStreamEvent) int {
	if blockType == "thinking" && s.thinkingBlockIndex >= 0 {
		return s.thinkingBlockIndex
	}
	if blockType == "text" && s.textBlockIndex >= 0 {
		return s.textBlockIndex
	}
	idx := s.nextBlockIndex
	s.nextBlockIndex++
	s.activeBlocks[idx] = true
	if blockType == "thinking" {
		s.thinkingBlockIndex = idx
		var blockStartEv struct {
			Type         string `json:"type"`
			Index        int    `json:"index"`
			ContentBlock struct {
				Type     string `json:"type"`
				Thinking string `json:"thinking"`
			} `json:"content_block"`
		}
		blockStartEv.Type = "content_block_start"
		blockStartEv.Index = idx
		blockStartEv.ContentBlock.Type = "thinking"
		blockStartEv.ContentBlock.Thinking = ""
		appendMessagesEvent(events, "content_block_start", blockStartEv)
	} else {
		s.textBlockIndex = idx
		var blockStartEv contentBlockStartEvent
		blockStartEv.Type = "content_block_start"
		blockStartEv.Index = idx
		blockStartEv.ContentBlock.Type = "text"
		blockStartEv.ContentBlock.Text = ""
		appendMessagesEvent(events, "content_block_start", blockStartEv)
	}
	return idx
}

func (s *ChatToMessagesStream) emitTextDelta(idx int, text string, events *[]MessagesStreamEvent) {
	var deltaEv contentBlockDeltaEvent
	deltaEv.Type = "content_block_delta"
	deltaEv.Index = idx
	deltaEv.Delta.Type = "text_delta"
	deltaEv.Delta.Text = text
	appendMessagesEvent(events, "content_block_delta", deltaEv)
	(*events)[len(*events)-1].TransmittedChars = len(text)
	(*events)[len(*events)-1].Flush = true
	s.textChars += len(text)
}

func (s *ChatToMessagesStream) emitThinkingDelta(idx int, text string, events *[]MessagesStreamEvent) {
	var deltaEv struct {
		Type  string `json:"type"`
		Index int    `json:"index"`
		Delta struct {
			Type     string `json:"type"`
			Thinking string `json:"thinking"`
		} `json:"delta"`
	}
	deltaEv.Type = "content_block_delta"
	deltaEv.Index = idx
	deltaEv.Delta.Type = "thinking_delta"
	deltaEv.Delta.Thinking = text
	appendMessagesEvent(events, "content_block_delta", deltaEv)
	(*events)[len(*events)-1].TransmittedChars = len(text)
	(*events)[len(*events)-1].Flush = true
	s.thinkingChars += len(text)
}

func (s *ChatToMessagesStream) closeOpenBlocks(events *[]MessagesStreamEvent) {
	var activeIndices []int
	for idx := range s.activeBlocks {
		activeIndices = append(activeIndices, idx)
	}
	sort.Ints(activeIndices)
	for _, idx := range activeIndices {
		appendMessagesEvent(events, "content_block_stop", contentBlockStopEvent{Type: "content_block_stop", Index: idx})
	}
	s.activeBlocks = make(map[int]bool)
}

func (s *ChatToMessagesStream) finalizeSuccess(stopReason string, events *[]MessagesStreamEvent) {
	s.startMessage(events)
	if len(s.activeBlocks) == 0 {
		// Anthropic 客户端要求终止前至少出现一个 content block。
		s.ensureBlock("text", events)
	}
	s.closeOpenBlocks(events)
	var msgDeltaEv messageDeltaEvent
	msgDeltaEv.Type = "message_delta"
	msgDeltaEv.Delta.StopReason = stopReason
	msgDeltaEv.Usage.OutputTokens = s.usage.OutputTokens
	appendMessagesEvent(events, "message_delta", msgDeltaEv)
	appendMessagesEvent(events, "message_stop", messageStopEvent{Type: "message_stop"})
	(*events)[len(*events)-1].Flush = true
}

func mapOpenAIFinishReason(finishReason string) string {
	switch finishReason {
	case "length":
		return "max_tokens"
	case "tool_calls", "function_call":
		return "tool_use"
	case "content_filter", "stop", "":
		return "end_turn"
	default:
		return "end_turn"
	}
}
