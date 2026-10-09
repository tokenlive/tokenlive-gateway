package translate_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
)

func messagesEventNames(events []translate.MessagesStreamEvent) []string {
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, event.Event)
	}
	return names
}

func messagesEventObject(t *testing.T, event translate.MessagesStreamEvent) map[string]interface{} {
	t.Helper()
	var object map[string]interface{}
	require.NoError(t, json.Unmarshal(event.Data, &object))
	return object
}

func TestChatToMessagesStream_EmptyWithoutCompletionSignal(t *testing.T) {
	stream := translate.NewChatToMessagesStream("client-model", translate.TokenUsage{})
	events, meta := stream.Finish(translate.TokenUsage{})
	require.Empty(t, events)
	require.False(t, meta.Completed)
	require.Equal(t, "empty upstream stream: no content or completion signal received", meta.ErrorMessage)
}

func TestChatToMessagesStream_ReasoningContent(t *testing.T) {
	stream := translate.NewChatToMessagesStream("client-model", translate.TokenUsage{})
	events, meta := stream.FeedJSON(`{"id":"chatcmpl-123","model":"upstream-model","choices":[{"delta":{"reasoning_content":"Thinking deeply..."}}]}`)
	require.Equal(t, []string{"message_start", "content_block_start", "content_block_delta"}, messagesEventNames(events))
	assert.JSONEq(t, `{"type":"message_start","message":{"id":"msg_123","type":"message","role":"assistant","content":[],"model":"client-model","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":0,"output_tokens":0}}}`, string(events[0].Data))
	assert.JSONEq(t, `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`, string(events[1].Data))
	assert.JSONEq(t, `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Thinking deeply..."}}`, string(events[2].Data))
	assert.Equal(t, 18, meta.TransmittedChars)

	events, _ = stream.FeedJSON(`{"choices":[{"delta":{"content":"Hello world!"}}]}`)
	require.Equal(t, []string{"content_block_start", "content_block_delta"}, messagesEventNames(events))
	assert.JSONEq(t, `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`, string(events[0].Data))
	assert.JSONEq(t, `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Hello world!"}}`, string(events[1].Data))

	events, meta = stream.FeedJSON(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	assert.Empty(t, events)
	assert.False(t, meta.Completed)
	events, meta = stream.FeedJSON("[DONE]")
	assert.Empty(t, events)
	assert.False(t, meta.Completed)
	assert.True(t, meta.SawDone)

	events, meta = stream.Finish(translate.TokenUsage{OutputTokens: 9})
	require.Equal(t, []string{"content_block_stop", "content_block_stop", "message_delta", "message_stop"}, messagesEventNames(events))
	assert.JSONEq(t, `{"type":"content_block_stop","index":0}`, string(events[0].Data))
	assert.JSONEq(t, `{"type":"content_block_stop","index":1}`, string(events[1].Data))
	assert.JSONEq(t, `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":9}}`, string(events[2].Data))
	assert.JSONEq(t, `{"type":"message_stop"}`, string(events[3].Data))
	assert.True(t, meta.Completed)
	assert.Equal(t, "stop", meta.FinishReason)
	assert.Equal(t, "end_turn", meta.StopReason)
	assert.Equal(t, 12, meta.TextChars)
	assert.Equal(t, 18, meta.ThinkingChars)
	assert.Zero(t, meta.TransmittedChars)
}

func TestChatToMessagesStream_ReasoningPriority(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delta string
		want  string
	}{
		{"reasoning_content", `{"reasoning_content":"first","thinking":"second","reasoning":"third","thought":"fourth"}`, "first"},
		{"thinking", `{"reasoning_content":"","thinking":"second","reasoning":"third","thought":"fourth"}`, "second"},
		{"reasoning", `{"thinking":"","reasoning":"third","thought":"fourth"}`, "third"},
		{"thought", `{"reasoning":"","thought":"fourth"}`, "fourth"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{})
			events, meta := stream.FeedJSON(`{"choices":[{"delta":` + tc.delta + `}]}`)
			require.Len(t, events, 3)
			delta := messagesEventObject(t, events[2])["delta"].(map[string]interface{})
			assert.Equal(t, "thinking_delta", delta["type"])
			assert.Equal(t, tc.want, delta["thinking"])
			assert.Equal(t, len(tc.want), meta.TransmittedChars)
		})
	}
}

func TestChatToMessagesStream_FinishReasonMapping(t *testing.T) {
	for _, tc := range []struct{ finish, stop string }{
		{"stop", "end_turn"}, {"length", "max_tokens"}, {"tool_calls", "tool_use"},
		{"function_call", "tool_use"}, {"content_filter", "end_turn"}, {"unknown", "end_turn"}, {"", "end_turn"},
	} {
		t.Run(tc.finish, func(t *testing.T) {
			stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{})
			events, meta := stream.FeedJSON(fmt.Sprintf(`{"choices":[{"delta":{},"finish_reason":%q}]}`, tc.finish))
			assert.Empty(t, events)
			assert.False(t, meta.Completed)
			if tc.finish == "" {
				stream.FeedJSON("[DONE]")
			}
			events, meta = stream.Finish(translate.TokenUsage{})
			require.Equal(t, []string{"message_start", "content_block_start", "content_block_stop", "message_delta", "message_stop"}, messagesEventNames(events))
			assert.JSONEq(t, `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`, string(events[1].Data))
			assert.Equal(t, tc.stop, messagesEventObject(t, events[3])["delta"].(map[string]interface{})["stop_reason"])
			assert.Equal(t, tc.finish, meta.FinishReason)
			assert.Equal(t, tc.stop, meta.StopReason)
			assert.True(t, meta.Completed)
		})
	}
}

func TestChatToMessagesStream_InitialUsageUpdatedBeforeStart(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prior      string
		firstUsage string
		want       string
	}{
		{"seed", "", "", `{"input_tokens":11,"output_tokens":3}`},
		{"same_frame", "", `,"usage":{"prompt_tokens":20,"completion_tokens":7}`, `{"input_tokens":20,"output_tokens":7}`},
		{"usage_before_content_zero_guard", `{"id":"chatcmpl-before","choices":[],"usage":{"prompt_tokens":20,"completion_tokens":7}}`, `,"usage":{"prompt_tokens":0,"completion_tokens":0}`, `{"input_tokens":20,"output_tokens":7}`},
		{"independent_positive_guard", "", `,"usage":{"prompt_tokens":20,"completion_tokens":0}`, `{"input_tokens":20,"output_tokens":3}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{InputTokens: 11, OutputTokens: 3})
			if tc.prior != "" {
				events, _ := stream.FeedJSON(tc.prior)
				assert.Empty(t, events)
			}
			events, _ := stream.FeedJSON(`{"choices":[{"delta":{"content":"hello"}}]` + tc.firstUsage + `}`)
			require.Len(t, events, 3)
			message := messagesEventObject(t, events[0])["message"].(map[string]interface{})
			usage, err := json.Marshal(message["usage"])
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, string(usage))
			if tc.prior != "" {
				assert.Equal(t, "msg_before", message["id"])
			}
		})
	}
}

func TestChatToMessagesStream_UsageTailAfterCompletionSignals(t *testing.T) {
	stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{InputTokens: 10, OutputTokens: 2, CachedTokens: 4, CacheCreationTokens: 3})
	stream.FeedJSON(`{"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}]}`)
	stream.FeedJSON("[DONE]")
	events, meta := stream.FeedJSON(`{"choices":[],"usage":{"prompt_tokens":30,"completion_tokens":17,"prompt_tokens_details":{"cached_tokens":8}}}`)
	assert.Empty(t, events)
	assert.False(t, meta.Completed)
	assert.Equal(t, 30, meta.InputTokens)
	assert.Equal(t, 17, meta.OutputTokens)
	assert.Equal(t, 8, meta.CachedTokens)

	events, meta = stream.FeedJSON(`{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":0}}}`)
	assert.Empty(t, events)
	assert.Zero(t, meta.InputTokens)
	assert.Zero(t, meta.OutputTokens)
	// Finish uses the latest effective caller values, not a stale finish-frame snapshot.
	events, meta = stream.Finish(translate.TokenUsage{InputTokens: 31, OutputTokens: 19, CachedTokens: 9, CacheCreationTokens: 3})
	require.Len(t, events, 3)
	assert.JSONEq(t, `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":19}}`, string(events[1].Data))
	assert.True(t, meta.Completed)
	assert.True(t, meta.SawDone)
}

func TestChatToMessagesStream_ToolArgumentsAndBlockIndices(t *testing.T) {
	stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{})
	events, meta := stream.FeedJSON(`{"choices":[{"delta":{"tool_calls":[{"index":4,"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":"}},{"index":1,"id":"toolu_second","function":{"name":"other","arguments":"{}"}}]}}]}`)
	require.Equal(t, []string{"message_start", "content_block_start", "content_block_delta", "content_block_start", "content_block_delta"}, messagesEventNames(events))
	assert.JSONEq(t, `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"lookup","input":{}}}`, string(events[1].Data))
	assert.JSONEq(t, `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}`, string(events[2].Data))
	assert.JSONEq(t, `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_second","name":"other","input":{}}}`, string(events[3].Data))
	assert.Zero(t, meta.TransmittedChars)
	assert.True(t, meta.HasToolUse)

	events, meta = stream.FeedJSON(`{"choices":[{"delta":{"reasoning_content":"why","content":"ok","tool_calls":[{"index":4,"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]}`)
	require.Len(t, events, 5)
	assert.Equal(t, float64(2), messagesEventObject(t, events[0])["index"])
	assert.Equal(t, float64(3), messagesEventObject(t, events[2])["index"])
	assert.JSONEq(t, `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"1}"}}`, string(events[4].Data))
	assert.Equal(t, 5, meta.TransmittedChars)

	events, meta = stream.Finish(translate.TokenUsage{})
	require.Equal(t, []string{"content_block_stop", "content_block_stop", "content_block_stop", "content_block_stop", "message_delta", "message_stop"}, messagesEventNames(events))
	for idx := 0; idx < 4; idx++ {
		assert.Equal(t, float64(idx), messagesEventObject(t, events[idx])["index"])
	}
	assert.Equal(t, "tool_use", meta.StopReason)
	assert.Equal(t, 2, meta.TextChars)
	assert.Equal(t, 3, meta.ThinkingChars)
	assert.True(t, meta.HasToolUse)
}

func TestChatToMessagesStream_UnicodeByteLengthAndFrameIncrements(t *testing.T) {
	stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{})
	events, meta := stream.FeedJSON(`{"choices":[{"delta":{"reasoning_content":"思考","content":"你🙂"}}]}`)
	require.Len(t, events, 5)
	assert.Equal(t, 0, events[0].TransmittedChars)
	assert.Equal(t, 6, events[2].TransmittedChars)
	assert.Equal(t, 7, events[4].TransmittedChars)
	assert.True(t, events[0].Flush)
	assert.True(t, events[2].Flush)
	assert.True(t, events[4].Flush)
	assert.Equal(t, 13, meta.TransmittedChars)
	assert.Equal(t, 7, meta.TextChars)
	assert.Equal(t, 6, meta.ThinkingChars)
	events, meta = stream.FeedJSON(`{"choices":[{"delta":{"reasoning_content":"!","content":"好"},"finish_reason":"stop"}]}`)
	require.Len(t, events, 2)
	assert.Equal(t, 4, meta.TransmittedChars)
	assert.Equal(t, 10, meta.TextChars)
	assert.Equal(t, 7, meta.ThinkingChars)
	_, meta = stream.Finish(translate.TokenUsage{})
	assert.Zero(t, meta.TransmittedChars)
	assert.Equal(t, 10, meta.TextChars)
	assert.Equal(t, 7, meta.ThinkingChars)
}

func TestChatToMessagesStream_PrematureEOFClosesOnlyPartialBlocks(t *testing.T) {
	stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{})
	stream.FeedJSON(`{"choices":[{"delta":{"reasoning_content":"thinking...","content":"partial answer"}}]}`)
	events, meta := stream.Finish(translate.TokenUsage{})
	require.Equal(t, []string{"content_block_stop", "content_block_stop"}, messagesEventNames(events))
	assert.JSONEq(t, `{"type":"content_block_stop","index":0}`, string(events[0].Data))
	assert.JSONEq(t, `{"type":"content_block_stop","index":1}`, string(events[1].Data))
	assert.False(t, meta.Completed)
	assert.Equal(t, "upstream stream closed prematurely without completion event", meta.ErrorMessage)
	events, meta = stream.Finish(translate.TokenUsage{})
	assert.Empty(t, events)
	assert.False(t, meta.Completed)
}

func TestChatToMessagesStream_DoneOnlyAndRepeatedFinish(t *testing.T) {
	stream := translate.NewChatToMessagesStream("caller-model", translate.TokenUsage{InputTokens: 7, OutputTokens: 2})
	events, meta := stream.FeedJSON("[DONE]")
	assert.Empty(t, events)
	assert.False(t, meta.Completed)
	events, meta = stream.Finish(translate.TokenUsage{InputTokens: 8, OutputTokens: 4})
	require.Len(t, events, 5)
	assert.JSONEq(t, `{"type":"message_start","message":{"id":"msg_mockprobe1234567890","type":"message","role":"assistant","content":[],"model":"caller-model","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":8,"output_tokens":4}}}`, string(events[0].Data))
	assert.True(t, meta.Completed)
	assert.True(t, meta.SawDone)
	events, meta = stream.Finish(translate.TokenUsage{OutputTokens: 99})
	assert.Empty(t, events)
	assert.False(t, meta.Completed)
}

func TestChatToMessagesStream_IgnoresMalformedAndRoleOnlyChunks(t *testing.T) {
	stream := translate.NewChatToMessagesStream("model", translate.TokenUsage{})
	for _, data := range []string{`not json`, `{"choices":[{"delta":{"content":42}}]}`, `{"id":"chatcmpl-role","choices":[{"delta":{"role":"assistant"}}]}`} {
		events, meta := stream.FeedJSON(data)
		assert.Empty(t, events)
		assert.False(t, meta.Completed)
	}
	stream.FeedJSON("[DONE]")
	events, _ := stream.Finish(translate.TokenUsage{})
	assert.Equal(t, "msg_role", messagesEventObject(t, events[0])["message"].(map[string]interface{})["id"])
}
