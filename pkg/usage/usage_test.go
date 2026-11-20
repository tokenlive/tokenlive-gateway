package usage_test

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/usage"
)

func TestObserveCumulativeSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name   string
		decode usage.Decoder
		frames []string
	}{
		{"OpenAI", usage.OpenAI, []string{
			`{"usage":{"prompt_tokens":140,"prompt_tokens_details":{"cached_tokens":40}}}`,
			`{"usage":{"prompt_tokens":160,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":60}}}`,
		}},
		{"Responses", usage.Responses, []string{
			`{"usage":{"input_tokens":140,"input_tokens_details":{"cached_tokens":40}}}`,
			`{"type":"response.completed","response":{"usage":{"input_tokens":160,"output_tokens":20,"input_tokens_details":{"cached_tokens":60}}}}`,
		}},
		{"Anthropic", usage.Anthropic, []string{
			`{"type":"message_start","message":{"usage":{"input_tokens":100,"cache_read_input_tokens":40}}}`,
			`{"type":"message_delta","usage":{"output_tokens":20,"cache_read_input_tokens":60}}`,
		}},
		{"Gemini", usage.Gemini, []string{
			`{"usageMetadata":{"promptTokenCount":140,"cachedContentTokenCount":40}}`,
			`{"usageMetadata":{"promptTokenCount":160,"candidatesTokenCount":20,"cachedContentTokenCount":60}}`,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got usage.Tokens
			for _, frame := range tc.frames {
				got.Observe(tc.decode(frame))
			}
			want := usage.Tokens{InputTokens: 160, OutputTokens: 20, CachedTokens: 60}
			if got != want {
				t.Fatalf("got %+v, want %+v", got, want)
			}
			// Terminal events may be repeated; cumulative usage is not additive.
			got.Observe(tc.decode(tc.frames[1]))
			if got != want {
				t.Fatalf("repeated snapshot changed usage: %+v", got)
			}
			for _, frame := range []string{"", "[DONE]", "not json", "{}", "null"} {
				got.Observe(tc.decode(frame))
				if got != want {
					t.Fatalf("%q changed usage: %+v", frame, got)
				}
			}
		})
	}
}

func TestAnthropicPartialUsagePreservesOtherInputPortions(t *testing.T) {
	var got usage.Tokens
	for _, step := range []struct {
		frame string
		want  usage.Tokens
	}{
		{`{"type":"message_start","message":{"usage":{"input_tokens":100,"cache_read_input_tokens":40,"cache_creation_input_tokens":10}}}`, usage.Tokens{InputTokens: 150, CachedTokens: 40, CacheCreationTokens: 10}},
		{`{"type":"message_delta","usage":{"cache_read_input_tokens":60,"output_tokens":20}}`, usage.Tokens{InputTokens: 170, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 10}},
		{`{"type":"message_delta","usage":{"input_tokens":120}}`, usage.Tokens{InputTokens: 190, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 10}},
		{`{"type":"message_delta","usage":{"cache_creation_input_tokens":30}}`, usage.Tokens{InputTokens: 210, OutputTokens: 20, CachedTokens: 60, CacheCreationTokens: 30}},
		{`{"type":"message_delta","usage":{"output_tokens":25}}`, usage.Tokens{InputTokens: 210, OutputTokens: 25, CachedTokens: 60, CacheCreationTokens: 30}},
		{`{"type":"message_delta","usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}`, usage.Tokens{InputTokens: 210, OutputTokens: 25, CachedTokens: 60, CacheCreationTokens: 30}},
		{`{"type":"message_delta","usage":{"input_tokens":null,"cache_read_input_tokens":-1}}`, usage.Tokens{InputTokens: 210, OutputTokens: 25, CachedTokens: 60, CacheCreationTokens: 30}},
	} {
		got.Observe(usage.Anthropic(step.frame))
		if got != step.want {
			t.Fatalf("frame %s: got %+v, want %+v", step.frame, got, step.want)
		}
	}
}

func TestObserveOnlyCacheAndPositiveCorrections(t *testing.T) {
	var got usage.Tokens
	got.Observe(usage.Anthropic(`{"usage":{"input_tokens":0,"cache_read_input_tokens":1000}}`))
	if got.InputTokens != 1000 {
		t.Fatalf("cache-only input = %+v", got)
	}
	got.Observe(usage.Anthropic(`{"usage":{"cache_read_input_tokens":800}}`))
	if got.InputTokens != 800 || got.CachedTokens != 800 {
		t.Fatalf("positive correction must replace, not max or add: %+v", got)
	}
}
