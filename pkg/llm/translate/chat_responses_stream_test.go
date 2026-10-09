package translate_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
)

func chatResponsesEventNames(events []translate.ResponsesStreamEvent) []string {
	names := make([]string, len(events))
	for i, event := range events {
		names[i] = event.Event
	}
	return names
}

func chatResponsesPayload(t *testing.T, event translate.ResponsesStreamEvent) map[string]interface{} {
	t.Helper()
	var payload map[string]interface{}
	if err := json.Unmarshal(event.Data, &payload); err != nil {
		t.Fatalf("invalid %s payload: %v", event.Event, err)
	}
	if payload["type"] != event.Event {
		t.Fatalf("event/type mismatch: %s / %v", event.Event, payload["type"])
	}
	return payload
}

// 丢失 EOF 补完成、双 terminal 事件或 caller-latest usage 均应使此测试失败。
func TestChatToResponsesStream_ReasoningSeparation(t *testing.T) {
	stream := translate.NewChatToResponsesStream("fallback", nil, translate.TokenUsage{})
	var all []translate.ResponsesStreamEvent
	for _, frame := range []string{
		`{"id":"chatcmpl-reason-stream","model":"gpt-5.4","choices":[{"delta":{"reasoning_content":"thinking","reasoning":"ignored"}}]}`,
		`{"choices":[{"delta":{"reasoning":"过程","content":"answer"}}]}`,
	} {
		events, meta := stream.FeedJSON(frame)
		all = append(all, events...)
		if meta.TransmittedChars == 0 {
			t.Fatal("reasoning/text bytes must be reported")
		}
	}
	events, meta := stream.Finish(translate.TokenUsage{})
	all = append(all, events...)
	if meta.ThinkingChars != 14 || meta.TextChars != 6 {
		t.Fatalf("terminal counters = %+v", meta)
	}
	var deltas, outputText []string
	for _, event := range all {
		payload := chatResponsesPayload(t, event)
		switch event.Event {
		case "response.reasoning_summary_text.delta":
			deltas = append(deltas, payload["delta"].(string))
		case "response.output_text.delta":
			outputText = append(outputText, payload["delta"].(string))
		case "response.completed":
			output := payload["response"].(map[string]interface{})["output"].([]interface{})
			if len(output) != 2 {
				t.Fatalf("reasoning/text output = %v", output)
			}
			reason := output[0].(map[string]interface{})
			if reason["id"] != "rs_reason-stream" || reason["type"] != "reasoning" || reason["summary"].([]interface{})[0].(map[string]interface{})["text"] != "thinking过程" {
				t.Fatalf("reasoning output = %v", reason)
			}
		}
	}
	if !reflect.DeepEqual(deltas, []string{"thinking", "过程"}) || !reflect.DeepEqual(outputText, []string{"answer"}) {
		t.Fatalf("deltas = %v / %v", deltas, outputText)
	}
}

func TestChatToResponsesStream_NamespaceToolCall(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		t.Run(map[bool]string{false: "dotted", true: "mapper"}[mapped], func(t *testing.T) {
			var mapper *translate.ToolNameMapper
			name := "collaboration.spawn_agent"
			if mapped {
				mapper = translate.NewToolNameMapper()
				name = mapper.SanitizeAndRegister("collaboration", "spawn_agent")
			}
			stream := translate.NewChatToResponsesStream("glm-5.3", mapper, translate.TokenUsage{})
			frame := `{"id":"chatcmpl-ns-stream","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ns","function":{"name":"` + name + `","arguments":"{}"}}]}}]}`
			events, meta := stream.FeedJSON(frame)
			if !meta.HasToolUse || meta.TransmittedChars != 0 {
				t.Fatalf("tool meta = %+v", meta)
			}
			want := []string{"response.created", "response.in_progress", "response.output_item.added", "response.function_call.arguments.delta"}
			if !reflect.DeepEqual(chatResponsesEventNames(events), want) {
				t.Fatalf("events = %v", chatResponsesEventNames(events))
			}
			item := chatResponsesPayload(t, events[2])["item"].(map[string]interface{})
			if item["name"] != "spawn_agent" || item["namespace"] != "collaboration" || item["id"] != "fc_ns" || item["call_id"] != "call_ns" {
				t.Fatalf("tool item = %v", item)
			}
			events, _ = stream.Finish(translate.TokenUsage{})
			item = chatResponsesPayload(t, events[len(events)-1])["response"].(map[string]interface{})["output"].([]interface{})[0].(map[string]interface{})
			if item["arguments"] != "{}" || item["namespace"] != "collaboration" {
				t.Fatalf("final tool = %v", item)
			}
		})
	}
}

func TestChatToResponsesStream_ToolCallsAccumulate(t *testing.T) {
	stream := translate.NewChatToResponsesStream("gpt-5.4", nil, translate.TokenUsage{})
	frames := []string{
		`{"id":"chatcmpl-toolCallStream","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc123","function":{"name":"js","arguments":""}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"code\""}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"log\"}"}}]},"finish_reason":"tool_calls"}]}`,
	}
	var deltas []string
	for _, frame := range frames {
		events, meta := stream.FeedJSON(frame)
		if meta.TransmittedChars != 0 {
			t.Fatal("tool arguments must not add transmitted billing bytes")
		}
		for _, event := range events {
			if event.Event == "response.function_call.arguments.delta" {
				payload := chatResponsesPayload(t, event)
				if payload["item_id"] != "fc_abc123" || payload["call_id"] != "call_abc123" {
					t.Fatalf("argument ids = %v", payload)
				}
				deltas = append(deltas, payload["delta"].(string))
			}
		}
	}
	if !reflect.DeepEqual(deltas, []string{"", `{"code"`, `:"log"}`}) {
		t.Fatalf("argument deltas = %v", deltas)
	}
	events, meta := stream.Finish(translate.TokenUsage{})
	if meta.FinishReason != "tool_calls" || !meta.HasToolUse {
		t.Fatalf("terminal meta = %+v", meta)
	}
	if !reflect.DeepEqual(chatResponsesEventNames(events), []string{"response.function_call.arguments.done", "response.output_item.done", "response.done", "response.completed"}) {
		t.Fatalf("terminal events = %v", chatResponsesEventNames(events))
	}
	if chatResponsesPayload(t, events[0])["arguments"] != `{"code":"log"}` {
		t.Fatalf("arguments not accumulated: %s", events[0].Data)
	}
}

func TestChatToResponsesStream_ApplyPatchCustomToolCall(t *testing.T) {
	for _, name := range []string{"apply_patch", "edit_code"} {
		t.Run(name, func(t *testing.T) {
			mapper := translate.NewToolNameMapper()
			mapper.RegisterCustom(name)
			stream := translate.NewChatToResponsesStream("kimi-k3", mapper, translate.TokenUsage{})
			frames := []string{
				`{"id":"chatcmpl-stream-patch","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_patch_stream_1","function":{"name":"` + name + `","arguments":""}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"patch\": \"*** Begin Patch\\n"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"+console.log(\\\"hello\\\");\\n*** End Patch\"}"}}]},"finish_reason":"tool_calls"}]}`,
			}
			var all []translate.ResponsesStreamEvent
			for _, frame := range frames {
				events, _ := stream.FeedJSON(frame)
				all = append(all, events...)
			}
			events, _ := stream.Finish(translate.TokenUsage{})
			all = append(all, events...)
			found := false
			for _, event := range all {
				if strings.HasPrefix(event.Event, "response.function_call.arguments.") {
					t.Fatal("custom tools must not emit function argument events")
				}
				payload := chatResponsesPayload(t, event)
				if event.Event == "response.output_item.added" || event.Event == "response.output_item.done" {
					item := payload["item"].(map[string]interface{})
					if item["type"] != "custom_tool_call" || item["id"] != "ctc_patch_stream_1" || item["call_id"] != "call_patch_stream_1" {
						t.Fatalf("custom item = %v", item)
					}
					if event.Event == "response.output_item.done" {
						found = true
						if item["input"] != "*** Begin Patch\n+console.log(\"hello\");\n*** End Patch" {
							t.Fatalf("patch input = %v", item["input"])
						}
					}
				}
			}
			if !found {
				t.Fatal("missing custom item done")
			}
		})
	}
}

func TestChatToResponsesStream_TextToolOrderingAndFallbackCallID(t *testing.T) {
	stream := translate.NewChatToResponsesStream("test", nil, translate.TokenUsage{})
	first, _ := stream.FeedJSON(`{"id":"chatcmpl-order","choices":[{"delta":{"tool_calls":[{"index":2,"function":{"arguments":"{"}}]}}]}`)
	if chatResponsesPayload(t, first[2])["call_id"] != "call_msg_order_2" {
		t.Fatalf("fallback call id = %s", first[2].Data)
	}
	second, meta := stream.FeedJSON(`{"choices":[{"delta":{"reasoning":"think","content":"text","tool_calls":[{"index":0,"id":"toolu_low","function":{"name":"low","arguments":"{}"}},{"index":2,"id":"call_late","function":{"name":"high","arguments":"}"}}]}}]}`)
	if meta.TransmittedChars != 9 || !meta.HasToolUse {
		t.Fatalf("mixed meta = %+v", meta)
	}
	var addedTypes []string
	var indices []int
	for _, event := range second {
		if event.Event == "response.output_item.added" {
			payload := chatResponsesPayload(t, event)
			addedTypes = append(addedTypes, payload["item"].(map[string]interface{})["type"].(string))
			indices = append(indices, int(payload["output_index"].(float64)))
		}
	}
	if !reflect.DeepEqual(addedTypes, []string{"reasoning", "message", "function_call", "function_call"}) || !reflect.DeepEqual(indices, []int{1, 2, 3, 0}) {
		t.Fatalf("added ordering = %v / %v", addedTypes, indices)
	}
	events, _ := stream.Finish(translate.TokenUsage{})
	output := chatResponsesPayload(t, events[len(events)-1])["response"].(map[string]interface{})["output"].([]interface{})
	var ids []string
	for _, value := range output {
		ids = append(ids, value.(map[string]interface{})["id"].(string))
	}
	// 旧最终 output 固定 reasoning→text→按 tool index 排序，不改为 output_index 排序。
	if !reflect.DeepEqual(ids, []string{"rs_order", "msg_order", "fc_low", "fc_msg_order_2"}) {
		t.Fatalf("final ordering = %v", ids)
	}
	last := output[3].(map[string]interface{})
	if last["call_id"] != "call_msg_order_2" || last["arguments"] != "{}" {
		t.Fatalf("late id must retain original fallback: %v", last)
	}
}

func TestChatToResponsesStream_LifecycleAndUsageGuard(t *testing.T) {
	for _, tc := range []struct {
		name      string
		frames    []string
		completed bool
		sawDone   bool
		reason    string
	}{
		{"empty", nil, false, false, ""},
		{"invalid", []string{"not json"}, false, false, ""},
		{"done-only", []string{"[DONE]"}, false, true, ""},
		{"empty-object-eof", []string{"{}"}, true, false, ""},
		{"usage-only-eof", []string{`{"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3}}`}, true, false, ""},
		{"finish-done", []string{`{"choices":[{"delta":{},"finish_reason":"stop"}]}`, "[DONE]"}, true, true, "stop"},
		{"finish-late-usage-done", []string{`{"choices":[{"delta":{},"finish_reason":"stop"}]}`, `{"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":2}}}`, `{"usage":{"prompt_tokens":0,"completion_tokens":-1,"prompt_tokens_details":{"cached_tokens":0}}}`, "[DONE]"}, true, true, "stop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := translate.NewChatToResponsesStream("fallback", nil, translate.TokenUsage{InputTokens: 7, OutputTokens: 3, CachedTokens: 2, CacheCreationTokens: 4})
			for _, frame := range tc.frames {
				events, meta := stream.FeedJSON(frame)
				if meta.Completed || meta.EmitDone {
					t.Fatalf("Feed finalized too early: %v / %+v", chatResponsesEventNames(events), meta)
				}
			}
			events, meta := stream.Finish(translate.TokenUsage{})
			if meta.Completed != tc.completed || meta.EmitDone != tc.completed || meta.SawDone != tc.sawDone || meta.FinishReason != tc.reason {
				t.Fatalf("Finish meta = %+v", meta)
			}
			if tc.completed {
				if !reflect.DeepEqual(chatResponsesEventNames(events), []string{"response.done", "response.completed"}) {
					t.Fatalf("terminal events = %v", chatResponsesEventNames(events))
				}
				for _, event := range events {
					response := chatResponsesPayload(t, event)["response"].(map[string]interface{})
					usage := response["usage"].(map[string]interface{})
					if usage["input_tokens"] != float64(7) || usage["output_tokens"] != float64(3) || response["id"] != "resp_mock" || response["model"] != "fallback" || response["output"] != nil {
						t.Fatalf("empty/usage output = %v", response)
					}
				}
				if meta.CachedTokens != 2 || meta.CacheCreationTokens != 4 {
					t.Fatalf("cache zero guard = %+v", meta)
				}
			} else if len(events) != 0 {
				t.Fatalf("unstarted completion = %v", events)
			}
			if events, meta = stream.Finish(translate.TokenUsage{OutputTokens: 99}); len(events) != 0 || meta.Completed || meta.EmitDone {
				t.Fatalf("repeated Finish = %v / %+v", events, meta)
			}
		})
	}
}

func TestChatToResponsesStream_UsageDecodeDoesNotDiscardContent(t *testing.T) {
	for _, extra := range []string{`"usage":{"completion_tokens":"bad"}`, `"choices":[{"delta":{"content":"Hi"},"finish_reason":17}]`} {
		frame := `{"id":"chatcmpl-decode","choices":[{"delta":{"content":"Hi"}}],` + extra + `}`
		if strings.HasPrefix(extra, `"choices"`) {
			frame = `{"id":"chatcmpl-decode",` + extra + `}`
		}
		stream := translate.NewChatToResponsesStream("test", nil, translate.TokenUsage{OutputTokens: 5})
		events, meta := stream.FeedJSON(frame)
		if len(events) != 5 || meta.TransmittedChars != 2 {
			t.Fatalf("auxiliary decode error discarded text: %v / %+v", chatResponsesEventNames(events), meta)
		}
		events, _ = stream.Finish(translate.TokenUsage{})
		usage := chatResponsesPayload(t, events[len(events)-1])["response"].(map[string]interface{})["usage"].(map[string]interface{})
		if usage["output_tokens"] != float64(5) {
			t.Fatalf("bad usage overwrote real value: %v", usage)
		}
	}
}

func TestChatToResponsesStream_TextEOFUsesLatestUsage(t *testing.T) {
	stream := translate.NewChatToResponsesStream("fallback", nil, translate.TokenUsage{InputTokens: 3, OutputTokens: 1})
	events, meta := stream.FeedJSON(`{"id":"chatcmpl-stream123","model":"upstream","choices":[{"delta":{"content":"Hi"},"finish_reason":null}],"usage":{"prompt_tokens":37,"completion_tokens":2}}`)
	want := []string{"response.created", "response.in_progress", "response.output_item.added", "response.content_part.added", "response.output_text.delta"}
	if !reflect.DeepEqual(chatResponsesEventNames(events), want) {
		t.Fatalf("Feed events = %v, want %v", chatResponsesEventNames(events), want)
	}
	if meta.ResponseID != "resp_stream123" || meta.ResponseModel != "upstream" || meta.TransmittedChars != 2 || meta.Completed || meta.EmitDone {
		t.Fatalf("Feed meta = %+v", meta)
	}
	if meta.InputTokens != 37 || meta.OutputTokens != 2 {
		t.Fatalf("Feed usage = %+v", meta)
	}
	created := chatResponsesPayload(t, events[0])["response"].(map[string]interface{})
	if created["id"] != "resp_stream123" || created["model"] != "upstream" || created["status"] != "in_progress" || created["created_at"].(float64) <= 0 {
		t.Fatalf("created response = %v", created)
	}
	events, meta = stream.Finish(translate.TokenUsage{InputTokens: 41, OutputTokens: 9})
	want = []string{"response.output_text.done", "response.content_part.done", "response.output_item.done", "response.done", "response.completed"}
	if !reflect.DeepEqual(chatResponsesEventNames(events), want) || !meta.Completed || !meta.EmitDone || meta.TransmittedChars != 0 {
		t.Fatalf("Finish events = %v, meta = %+v", chatResponsesEventNames(events), meta)
	}
	for _, event := range events[3:] {
		response := chatResponsesPayload(t, event)["response"].(map[string]interface{})
		usage := response["usage"].(map[string]interface{})
		if usage["input_tokens"] != float64(41) || usage["output_tokens"] != float64(9) || usage["total_tokens"] != float64(50) {
			t.Fatalf("terminal usage = %v", usage)
		}
		item := response["output"].([]interface{})[0].(map[string]interface{})
		if item["id"] != "msg_stream123" || item["content"].([]interface{})[0].(map[string]interface{})["text"] != "Hi" {
			t.Fatalf("terminal output = %v", item)
		}
	}
	if events, meta = stream.Finish(translate.TokenUsage{}); len(events) != 0 || meta.Completed || meta.EmitDone {
		t.Fatalf("repeated Finish = %v, %+v", events, meta)
	}
}
