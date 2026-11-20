package llm

import "encoding/json"

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
