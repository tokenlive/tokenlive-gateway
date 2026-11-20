package llm_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
	"github.com/tokenlive/tokenlive-gateway/pkg/usage"
)

func TestAnthropicUsageConsistentAcrossPassthroughAndTranslations(t *testing.T) {
	native := &core.GatewayContext{ResponseWriter: httptest.NewRecorder(), StartTime: time.Now()}
	writer := llm.NewSSEInterceptWriter(native, llm.WithTokenExtractor(usage.Anthropic))
	chat, responses := translate.NewMessagesToChatStream("model"), translate.NewMessagesToResponsesStream("model")
	chatContext, responsesContext := &core.GatewayContext{}, &core.GatewayContext{}
	for _, step := range []struct {
		frame string
		want  usage.Tokens
	}{
		{`{"type":"message_start","message":{"usage":{"input_tokens":100,"cache_read_input_tokens":40,"cache_creation_input_tokens":10}}}`, usage.Tokens{InputTokens: 150, CachedTokens: 40, CacheCreationTokens: 10}},
		{`{"type":"message_delta","usage":{"cache_read_input_tokens":60,"output_tokens":20}}`, usage.Tokens{InputTokens: 170, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 10}},
		{`{"type":"message_delta","usage":{"input_tokens":120}}`, usage.Tokens{InputTokens: 190, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 10}},
		{`{"type":"message_delta","usage":{"output_tokens":20}}`, usage.Tokens{InputTokens: 190, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 10}},
		{`{"type":"message_delta","usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0}}`, usage.Tokens{InputTokens: 190, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 10}},
		{"not json", usage.Tokens{InputTokens: 190, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 10}},
	} {
		// Deliberately split every SSE frame so the test exercises the transport adapter too.
		frame := []byte("data: " + step.frame + "\n\n")
		for _, fragment := range [][]byte{frame[:9], frame[9:]} {
			if _, err := writer.Write(fragment); err != nil {
				t.Fatal(err)
			}
		}
		_, chatMeta := chat.FeedJSON(step.frame)
		chatContext.MergeUsage(chatMeta.TokenUsage)
		_, responsesMeta := responses.FeedJSON(step.frame)
		responsesContext.MergeUsage(responsesMeta.TokenUsage)
		for name, got := range map[string]usage.Tokens{"passthrough": native.Usage(), "MessagesToChat": chatContext.Usage(), "MessagesToResponses": responsesContext.Usage()} {
			if got != step.want {
				t.Fatalf("%s after %s: got %+v, want %+v", name, step.frame, got, step.want)
			}
		}
	}

	// Whole JSON responses must produce the same totals as the stream above.
	body := []byte(`{"id":"msg_1","role":"assistant","content":[],"usage":{"input_tokens":120,"output_tokens":20,"cache_read_input_tokens":60,"cache_creation_input_tokens":10}}`)
	chatReply, err := translate.MessagesToChatCompletion(body, "model")
	if err != nil {
		t.Fatal(err)
	}
	responsesReply, err := translate.MessagesResponseToResponses(body, "model")
	if err != nil {
		t.Fatal(err)
	}
	if chatReply.Usage != native.Usage() || responsesReply.Usage != native.Usage() {
		t.Fatalf("whole replies disagree with stream: chat=%+v responses=%+v stream=%+v", chatReply.Usage, responsesReply.Usage, native.Usage())
	}
}
