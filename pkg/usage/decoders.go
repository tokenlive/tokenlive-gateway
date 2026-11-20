package usage

import "encoding/json"

type cachedDetails struct {
	CachedTokens *int `json:"cached_tokens"`
}

// OpenAI decodes Chat Completions usage, whose prompt count includes cache reads.
func OpenAI(data string) Observation {
	var payload struct {
		Usage *struct {
			Input   *int           `json:"prompt_tokens"`
			Output  *int           `json:"completion_tokens"`
			Details *cachedDetails `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal([]byte(data), &payload) != nil || payload.Usage == nil {
		return Observation{}
	}
	u := payload.Usage
	o := Observation{input: u.Input, output: u.Output}
	if u.Details != nil {
		o.cached = u.Details.CachedTokens
	}
	return o
}

type anthropicUsage struct {
	Input    *int `json:"input_tokens"`
	Output   *int `json:"output_tokens"`
	Cached   *int `json:"cache_read_input_tokens"`
	Creation *int `json:"cache_creation_input_tokens"`
}

// Anthropic decodes message_start, message_delta, and whole Messages responses.
// Its input count excludes both cache reads and cache creation.
func Anthropic(data string) Observation {
	var payload struct {
		Type    string          `json:"type"`
		Usage   *anthropicUsage `json:"usage"`
		Message *struct {
			Usage *anthropicUsage `json:"usage"`
		} `json:"message"`
	}
	if json.Unmarshal([]byte(data), &payload) != nil {
		return Observation{}
	}
	u := payload.Usage
	if payload.Type == "message_start" {
		if payload.Message == nil {
			return Observation{}
		}
		u = payload.Message.Usage
	}
	if u == nil {
		return Observation{}
	}
	return Observation{input: u.Input, output: u.Output, cached: u.Cached, creation: u.Creation, uncachedInput: true}
}

// Responses decodes whole Responses replies and response.usage terminal events.
func Responses(data string) Observation {
	type responseUsage struct {
		Input   *int           `json:"input_tokens"`
		Output  *int           `json:"output_tokens"`
		Details *cachedDetails `json:"input_tokens_details"`
	}
	var payload struct {
		Usage    *responseUsage `json:"usage"`
		Response *struct {
			Usage *responseUsage `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal([]byte(data), &payload) != nil {
		return Observation{}
	}
	u := payload.Usage
	if u == nil && payload.Response != nil {
		u = payload.Response.Usage
	}
	if u == nil {
		return Observation{}
	}
	o := Observation{input: u.Input, output: u.Output}
	if u.Details != nil {
		o.cached = u.Details.CachedTokens
	}
	return o
}

// Gemini decodes generateContent and streamGenerateContent usageMetadata.
func Gemini(data string) Observation {
	var payload struct {
		Usage *struct {
			Input  *int `json:"promptTokenCount"`
			Output *int `json:"candidatesTokenCount"`
			Cached *int `json:"cachedContentTokenCount"`
		} `json:"usageMetadata"`
	}
	if json.Unmarshal([]byte(data), &payload) != nil || payload.Usage == nil {
		return Observation{}
	}
	u := payload.Usage
	return Observation{input: u.Input, output: u.Output, cached: u.Cached}
}
