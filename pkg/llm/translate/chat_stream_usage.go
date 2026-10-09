package translate

import "encoding/json"

func chatStreamUsage(data string) TokenUsage {
	var payload struct {
		Usage *struct {
			PromptTokens        int `json:"prompt_tokens"`
			CompletionTokens    int `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal([]byte(data), &payload) != nil || payload.Usage == nil {
		return TokenUsage{}
	}
	usage := TokenUsage{
		InputTokens:  payload.Usage.PromptTokens,
		OutputTokens: payload.Usage.CompletionTokens,
	}
	if payload.Usage.PromptTokensDetails != nil {
		usage.CachedTokens = payload.Usage.PromptTokensDetails.CachedTokens
	}
	return usage
}
