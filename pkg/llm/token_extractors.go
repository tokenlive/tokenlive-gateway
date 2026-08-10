package llm

import (
	"encoding/json"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// ApplyUsage writes extracted token counts into gctx, guarding each field with >0 so a
// zero-bearing frame never overwrites a real value from an earlier one. Shared by the
// streaming path (SSEInterceptWriter) and the non-streaming handlers so token write-back
// semantics live in one place.
func ApplyUsage(gctx *core.GatewayContext, in, out, cached, cacheCreated int) {
	if in > 0 {
		gctx.InputTokens = in
	}
	if out > 0 {
		gctx.OutputTokens = out
	}
	if cached > 0 {
		gctx.CachedTokens = cached
	}
	if cacheCreated > 0 {
		gctx.CacheCreationTokens = cacheCreated
	}
}

// TokenExtractor extracts input, output, cached, and cacheCreation token counts from SSE event data.
// Returns (0, 0, 0, 0) when the event carries no token information.
type TokenExtractor func(data string) (inputTokens, outputTokens, cachedTokens, cacheCreationTokens int)

// OpenAITokenExtractor extracts token counts from OpenAI usage JSON, including cached tokens.
// Format: {"usage":{"prompt_tokens":N,"completion_tokens":N,"prompt_tokens_details":{"cached_tokens":N}}}
func OpenAITokenExtractor(data string) (int, int, int, int) {
	var payload struct {
		Usage *struct {
			PromptTokens        int `json:"prompt_tokens"`
			CompletionTokens    int `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil || payload.Usage == nil {
		return 0, 0, 0, 0
	}
	cached := 0
	if payload.Usage.PromptTokensDetails != nil {
		cached = payload.Usage.PromptTokensDetails.CachedTokens
	}
	return payload.Usage.PromptTokens, payload.Usage.CompletionTokens, cached, 0
}

// AnthropicTokenExtractor extracts token counts from Anthropic usage data, including cache read and cache creation.
// Accepts either a streaming SSE event (message_start / message_delta) or a whole non-streaming
// response body (no "type" field, top-level "usage"), so streaming and non-streaming paths share one extractor.
//
// Key semantic difference from OpenAI: Anthropic's input_tokens represents only "uncached input",
// while cache_read_input_tokens and cache_creation_input_tokens are metered separately.
// To make the downstream billing formula (nonCached = InputTokens - Cached - CacheCreation)
// universally applicable across all providers, this function normalizes the returned
// inputTokens to "total input" = input_tokens + cache_read + cache_creation,
// aligning it with OpenAI's prompt_tokens (which already includes cached_tokens).
func AnthropicTokenExtractor(data string) (int, int, int, int) {
	var event struct {
		Type    string `json:"type"`
		Message *struct {
			Usage *struct {
				InputTokens              int `json:"input_tokens"`
				OutputTokens             int `json:"output_tokens"`
				CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
				CacheReadInputTokens     int `json:"cache_read_input_tokens"`
			} `json:"usage"`
		} `json:"message"`
		Usage *struct {
			InputTokens              int `json:"input_tokens"`
			OutputTokens             int `json:"output_tokens"`
			CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
			CacheReadInputTokens     int `json:"cache_read_input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return 0, 0, 0, 0
	}

	var in, out, cached, cacheCreated int
	switch event.Type {
	case "message_start":
		if event.Message != nil && event.Message.Usage != nil {
			u := event.Message.Usage
			cached = u.CacheReadInputTokens
			cacheCreated = u.CacheCreationInputTokens
			// Normalize to total input (including cache read + cache creation), aligning with OpenAI semantics
			in = u.InputTokens + cached + cacheCreated
		}
	case "message_delta":
		if event.Usage != nil {
			u := event.Usage
			out = u.OutputTokens
			cached = u.CacheReadInputTokens
			cacheCreated = u.CacheCreationInputTokens
			// Only report total input when this frame actually carries input info;
			// message_delta typically only updates output, so input fields are 0.
			// Returning 0 avoids overwriting the value from message_start.
			if u.InputTokens > 0 || cached > 0 || cacheCreated > 0 {
				in = u.InputTokens + cached + cacheCreated
			}
		}
	default:
		// Whole non-streaming response body: no "type" field, top-level "usage".
		if event.Usage != nil {
			u := event.Usage
			out = u.OutputTokens
			cached = u.CacheReadInputTokens
			cacheCreated = u.CacheCreationInputTokens
			in = u.InputTokens + cached + cacheCreated
		}
	}

	return in, out, cached, cacheCreated
}

// ResponsesTokenExtractor extracts token counts from Responses API SSE events.
// Usage is nested under response.usage in terminal events (response.completed/response.done),
// not at the top level like Chat Completions chunks.
func ResponsesTokenExtractor(data string) (int, int, int, int) {
	type usage struct {
		InputTokens        int `json:"input_tokens"`
		OutputTokens       int `json:"output_tokens"`
		InputTokensDetails *struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	}
	var payload struct {
		Usage    *usage `json:"usage"`
		Response *struct {
			Usage *usage `json:"usage"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return 0, 0, 0, 0
	}
	u := payload.Usage
	if u == nil && payload.Response != nil {
		u = payload.Response.Usage
	}
	if u == nil {
		return 0, 0, 0, 0
	}
	cached := 0
	if u.InputTokensDetails != nil {
		cached = u.InputTokensDetails.CachedTokens
	}
	return u.InputTokens, u.OutputTokens, cached, 0
}

// GeminiTokenExtractor extracts tokens from Gemini generateContent/streamGenerateContent responses.
// Format: {"usageMetadata":{"promptTokenCount":N,"candidatesTokenCount":N,"totalTokenCount":N}}
func GeminiTokenExtractor(data string) (int, int, int, int) {
	var payload struct {
		UsageMetadata *struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil || payload.UsageMetadata == nil {
		return 0, 0, 0, 0
	}
	return payload.UsageMetadata.PromptTokenCount, payload.UsageMetadata.CandidatesTokenCount, 0, 0
}

// ExtractContentLength extracts the character length of incremental response text
// (used for token estimation when a streaming response is interrupted).
func ExtractContentLength(protocol string, data string) int {
	if data == "" || data == "[DONE]" {
		return 0
	}
	switch protocol {
	case "anthropic":
		var payload struct {
			Delta *struct {
				Text string `json:"text"`
			} `json:"delta"`
		}
		if err := json.Unmarshal([]byte(data), &payload); err == nil && payload.Delta != nil {
			return len(payload.Delta.Text)
		}
	case "gemini":
		var payload struct {
			Candidates []struct {
				Content struct {
					Parts []struct {
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"content"`
			} `json:"candidates"`
		}
		if err := json.Unmarshal([]byte(data), &payload); err == nil {
			total := 0
			for _, c := range payload.Candidates {
				for _, p := range c.Content.Parts {
					total += len(p.Text)
				}
			}
			return total
		}
	default: // Parse using OpenAI format by default
		var payload struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &payload); err == nil && len(payload.Choices) > 0 {
			return len(payload.Choices[0].Delta.Content)
		}
	}
	return 0
}
