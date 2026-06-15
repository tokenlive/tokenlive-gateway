package llm

import (
	"encoding/json"
)

// TokenExtractor 从 SSE 事件数据中提取 input、output、cached 和 cacheCreation token 数量。
// 返回 (0, 0, 0, 0) 表示该事件不包含 token 信息。
type TokenExtractor func(data string) (inputTokens, outputTokens, cachedTokens, cacheCreationTokens int)

// OpenAITokenExtractor 从 OpenAI usage JSON 中提取 token 数量，包括缓存命中 token。
// 格式: {"usage":{"prompt_tokens":N,"completion_tokens":N,"prompt_tokens_details":{"cached_tokens":N}}}
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

// AnthropicTokenExtractor 从 Anthropic SSE 事件数据中提取 token 数量，包括缓存读取与写入。
// 处理 message_start (input_tokens) 和 message_delta (output_tokens)。
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
			in = event.Message.Usage.InputTokens
			cached = event.Message.Usage.CacheReadInputTokens
			cacheCreated = event.Message.Usage.CacheCreationInputTokens
		}
	case "message_delta":
		if event.Usage != nil {
			in = event.Usage.InputTokens
			out = event.Usage.OutputTokens
			cached = event.Usage.CacheReadInputTokens
			cacheCreated = event.Usage.CacheCreationInputTokens
		}
	}

	return in, out, cached, cacheCreated
}

// ExtractContentLength 提取增量响应文本字符长度（用于流式异常中断时进行 token 数估算）
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
	default: // 默认按 OpenAI 格式解析
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
