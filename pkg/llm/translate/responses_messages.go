package translate

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Defaults for Responses→Messages request translation.
const (
	// DefaultMessagesMaxTokens is the max_tokens fallback when the client sends
	// no max_output_tokens (Anthropic requires max_tokens on every request).
	DefaultMessagesMaxTokens = 8192
	// MinThinkingBudget is Anthropic's minimum thinking budget_tokens; when the
	// client's max_output_tokens cannot fit budget + output, thinking is dropped.
	MinThinkingBudget = 1024
)

// reasoningEffortBudget maps Responses reasoning.effort to Anthropic thinking budget_tokens.
var reasoningEffortBudget = map[string]int{
	"minimal": MinThinkingBudget,
	"low":     MinThinkingBudget,
	"medium":  4096,
	"high":    16384,
}

// ResponsesToMessagesResult is the Responses→Messages request translation result.
type ResponsesToMessagesResult struct {
	Body            []byte
	ThinkingEnabled bool     // thinking block config was emitted (drives downstream semantics)
	Warnings        []string // non-fatal degradations (stripped params, dropped items)
}

// MessagesToResponsesResult is the non-stream Messages→Responses translation result.
type MessagesToResponsesResult struct {
	Body                []byte
	Usage               TokenUsage
	CachedTokens        int
	CacheCreationTokens int
}

// ResponsesRequestToMessages translates an OpenAI Responses request to Anthropic Messages.
// model is the upstream model name (engine-resolved), not the client alias.
// Only function tools are converted; built-in tools (web_search etc.) are dropped.
// Stateless params (store, previous_response_id, include, background) are dropped by design.
func ResponsesRequestToMessages(rawBody []byte, model string, maxOutputTokens ...int) (ResponsesToMessagesResult, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		return ResponsesToMessagesResult{}, fmt.Errorf("parse raw body: %w", err)
	}

	var res ResponsesToMessagesResult
	out := map[string]interface{}{"model": model}

	var systemParts []string
	if instructions, ok := payload["instructions"].(string); ok && instructions != "" {
		systemParts = append(systemParts, instructions)
	}

	messages, sysParts, warnings, err := responsesInputToMessages(payload["input"])
	if err != nil {
		return ResponsesToMessagesResult{}, err
	}
	res.Warnings = append(res.Warnings, warnings...)
	systemParts = append(systemParts, sysParts...)
	if len(messages) == 0 {
		return ResponsesToMessagesResult{}, fmt.Errorf("no convertible input messages")
	}
	// Anthropic requires the first message to be from the user. Responses-style
	// chaining (input = prev output + new turn) often starts with assistant items.
	if messages[0]["role"] != "user" {
		messages = append([]map[string]interface{}{{
			"role":    "user",
			"content": []interface{}{map[string]interface{}{"type": "text", "text": " "}},
		}}, messages...)
	}
	out["messages"] = messages
	if len(systemParts) > 0 {
		out["system"] = strings.Join(systemParts, "\n")
	}

	if tools, warn := responsesToolsToMessages(payload["tools"]); len(tools) > 0 {
		out["tools"] = tools
		if warn != "" {
			res.Warnings = append(res.Warnings, warn)
		}
	} else if warn != "" {
		res.Warnings = append(res.Warnings, warn)
	}

	// thinking first: tool_choice compatibility depends on it.
	thinking, maxTokens, thinkWarnings := resolveThinking(payload, maxOutputTokens...)
	res.Warnings = append(res.Warnings, thinkWarnings...)
	if thinking != nil {
		out["thinking"] = thinking
		res.ThinkingEnabled = true
	}
	out["max_tokens"] = maxTokens

	if tc, warn := responsesToolChoiceToMessages(payload["tool_choice"], payload["parallel_tool_calls"], res.ThinkingEnabled); tc != nil {
		out["tool_choice"] = tc
		if warn != "" {
			res.Warnings = append(res.Warnings, warn)
		}
	} else if warn != "" {
		res.Warnings = append(res.Warnings, warn)
	}

	// Sampling params are incompatible with thinking (Anthropic 400s); strip them.
	if res.ThinkingEnabled {
		for _, k := range []string{"temperature", "top_p", "top_k"} {
			if _, exists := payload[k]; exists {
				res.Warnings = append(res.Warnings, fmt.Sprintf("%s stripped: incompatible with thinking", k))
			}
		}
	} else {
		for _, k := range []string{"temperature", "top_p"} {
			if v, exists := payload[k]; exists {
				out[k] = v
			}
		}
	}

	if md, ok := payload["metadata"].(map[string]interface{}); ok {
		if uid, ok := md["user_id"].(string); ok && uid != "" {
			out["metadata"] = map[string]interface{}{"user_id": uid}
		}
	}
	if v, exists := payload["stream"]; exists {
		out["stream"] = v
	}

	body, err := json.Marshal(out)
	if err != nil {
		return ResponsesToMessagesResult{}, fmt.Errorf("marshal translated body: %w", err)
	}
	res.Body = body
	return res, nil
}

// resolveThinking maps reasoning.effort → thinking config and resolves max_tokens:
//   - no effort: max_tokens = max_output_tokens or DefaultMessagesMaxTokens
//   - effort without client cap: max_tokens = budget + DefaultMessagesMaxTokens
//   - effort with client cap: budget clamps to max_tokens-MinThinkingBudget;
//     if even MinThinkingBudget cannot fit, thinking is disabled entirely.
func resolveThinking(payload map[string]interface{}, maxOutputTokens ...int) (thinking map[string]interface{}, maxTokens int, warnings []string) {
	defaultMax := DefaultMessagesMaxTokens
	limit := 0
	if len(maxOutputTokens) > 0 && maxOutputTokens[0] > 0 {
		defaultMax = maxOutputTokens[0]
		limit = maxOutputTokens[0]
	}

	effort := ""
	if r, ok := payload["reasoning"].(map[string]interface{}); ok {
		effort, _ = r["effort"].(string)
	}
	budget, hasBudget := reasoningEffortBudget[effort]

	clientMax, hasClientMax := payload["max_output_tokens"].(float64)

	if !hasBudget {
		if hasClientMax && clientMax > 0 {
			res := int(clientMax)
			if limit > 0 && res > limit {
				res = limit
			}
			return nil, res, nil
		}
		return nil, defaultMax, nil
	}

	if !hasClientMax || clientMax <= 0 {
		maxTokens = budget + defaultMax
		if limit > 0 && maxTokens > limit {
			maxTokens = limit
		}
	} else {
		maxTokens = int(clientMax)
		if limit > 0 && maxTokens > limit {
			maxTokens = limit
		}
		if budget > maxTokens-MinThinkingBudget {
			budget = maxTokens - MinThinkingBudget
		}
		if budget < MinThinkingBudget {
			warnings = append(warnings, fmt.Sprintf(
				"thinking disabled: max_output_tokens %d cannot fit budget + %d output tokens",
				maxTokens, MinThinkingBudget))
			return nil, maxTokens, warnings
		}
	}
	return map[string]interface{}{"type": "enabled", "budget_tokens": budget}, maxTokens, warnings
}

// responsesInputToMessages converts the Responses input field to Anthropic messages.
// Returns extra system parts collected from developer/system role items.
func responsesInputToMessages(input interface{}) (messages []map[string]interface{}, systemParts []string, warnings []string, err error) {
	if input == nil {
		return nil, nil, nil, fmt.Errorf("missing input")
	}
	if s, ok := input.(string); ok {
		if s == "" {
			return nil, nil, nil, fmt.Errorf("empty input")
		}
		return []map[string]interface{}{{
			"role":    "user",
			"content": []interface{}{map[string]interface{}{"type": "text", "text": s}},
		}}, nil, nil, nil
	}
	inputArr, ok := input.([]interface{})
	if !ok {
		return nil, nil, nil, fmt.Errorf("unsupported input type %T", input)
	}

	// appendBlock merges same-role consecutive messages (Anthropic requires strict
	// user/assistant alternation; tool_results and function calls arrive as items).
	appendBlock := func(role string, block map[string]interface{}) {
		// Thinking blocks must lead assistant content; prepend when merging.
		if len(messages) > 0 && messages[len(messages)-1]["role"] == role {
			last := messages[len(messages)-1]
			if content, ok := last["content"].([]interface{}); ok {
				if block["type"] == "thinking" {
					last["content"] = append([]interface{}{block}, content...)
				} else {
					last["content"] = append(content, block)
				}
				return
			}
		}
		messages = append(messages, map[string]interface{}{
			"role":    role,
			"content": []interface{}{block},
		})
	}

	for _, item := range inputArr {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		itemType, _ := itemMap["type"].(string)
		role, _ := itemMap["role"].(string)
		if itemType == "" && role != "" {
			itemType = "message" // legacy {role, content} items
		}

		switch itemType {
		case "message":
			switch role {
			case "system", "developer":
				if text := extractResponsesText(itemMap["content"]); text != "" {
					systemParts = append(systemParts, text)
				}
			case "user", "assistant":
				blocks, warns := responsesContentToBlocks(itemMap["content"], role)
				warnings = append(warnings, warns...)
				for _, b := range blocks {
					appendBlock(role, b)
				}
			default:
				warnings = append(warnings, fmt.Sprintf("message item with unknown role %q dropped", role))
			}

		case "function_call":
			id, _ := itemMap["call_id"].(string)
			if id == "" {
				id, _ = itemMap["id"].(string)
			}
			name, _ := itemMap["name"].(string)
			if name == "" {
				warnings = append(warnings, "function_call item without name dropped")
				continue
			}
			var inputObj interface{}
			if args, _ := itemMap["arguments"].(string); args != "" {
				_ = json.Unmarshal([]byte(args), &inputObj)
			}
			if inputObj == nil {
				inputObj = map[string]interface{}{}
			}
			appendBlock("assistant", map[string]interface{}{
				"type":  "tool_use",
				"id":    NormalizeToolUseID(id),
				"name":  name,
				"input": inputObj,
			})

		case "function_call_output":
			callID, _ := itemMap["call_id"].(string)
			content := extractResponsesText(itemMap["output"])
			appendBlock("user", map[string]interface{}{
				"type":        "tool_result",
				"tool_use_id": NormalizeToolUseID(callID),
				"content":     content,
			})

		case "reasoning":
			// Round-trip: our response-side conversion stores the Anthropic thinking
			// signature in encrypted_content; without it the block cannot be replayed.
			sig, _ := itemMap["encrypted_content"].(string)
			if sig == "" {
				continue
			}
			var text string
			if summary, ok := itemMap["summary"].([]interface{}); ok {
				var parts []string
				for _, p := range summary {
					if pm, ok := p.(map[string]interface{}); ok {
						if t, ok := pm["text"].(string); ok {
							parts = append(parts, t)
						}
					}
				}
				text = strings.Join(parts, "\n")
			}
			appendBlock("assistant", map[string]interface{}{
				"type":      "thinking",
				"thinking":  text,
				"signature": sig,
			})

		default:
			warnings = append(warnings, fmt.Sprintf("input item type %q dropped", itemType))
		}
	}
	return messages, systemParts, warnings, nil
}

// responsesContentToBlocks converts a Responses message content field to Anthropic blocks.
func responsesContentToBlocks(content interface{}, role string) (blocks []map[string]interface{}, warnings []string) {
	if s, ok := content.(string); ok {
		if s != "" {
			blocks = append(blocks, map[string]interface{}{"type": "text", "text": s})
		}
		return blocks, nil
	}
	arr, ok := content.([]interface{})
	if !ok {
		return nil, nil
	}
	for _, part := range arr {
		pm, ok := part.(map[string]interface{})
		if !ok {
			continue
		}
		ptype, _ := pm["type"].(string)
		switch ptype {
		case "input_text", "output_text", "text":
			if t, ok := pm["text"].(string); ok && t != "" {
				blocks = append(blocks, map[string]interface{}{"type": "text", "text": t})
			}
		case "input_image":
			urlStr, _ := pm["image_url"].(string)
			if strings.HasPrefix(urlStr, "data:") {
				stripped := strings.TrimPrefix(urlStr, "data:")
				if parts := strings.SplitN(stripped, ";base64,", 2); len(parts) == 2 {
					blocks = append(blocks, map[string]interface{}{
						"type": "image",
						"source": map[string]interface{}{
							"type":       "base64",
							"media_type": parts[0],
							"data":       parts[1],
						},
					})
				}
			} else if strings.HasPrefix(urlStr, "http://") || strings.HasPrefix(urlStr, "https://") {
				blocks = append(blocks, map[string]interface{}{
					"type":   "image",
					"source": map[string]interface{}{"type": "url", "url": urlStr},
				})
			} else if urlStr != "" {
				warnings = append(warnings, "input_image with unsupported source dropped")
			}
		case "refusal", "input_file":
			warnings = append(warnings, fmt.Sprintf("content part %q dropped", ptype))
		default:
			if ptype != "" {
				warnings = append(warnings, fmt.Sprintf("content part %q dropped", ptype))
			}
		}
	}
	return blocks, warnings
}

// extractResponsesText flattens a string-or-content-parts field to plain text.
func extractResponsesText(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	arr, ok := v.([]interface{})
	if !ok {
		return ""
	}
	var parts []string
	for _, p := range arr {
		if pm, ok := p.(map[string]interface{}); ok {
			if t, ok := pm["text"].(string); ok {
				parts = append(parts, t)
			}
		}
	}
	return strings.Join(parts, "\n")
}

// responsesToolsToMessages converts Responses function tools to Anthropic tools.
// Built-in tools (web_search, code_interpreter, …) are dropped.
func responsesToolsToMessages(toolsVal interface{}) (tools []interface{}, warning string) {
	toolsArr, ok := toolsVal.([]interface{})
	if !ok {
		return nil, ""
	}
	dropped := 0
	for _, t := range toolsArr {
		tMap, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		std := BuildStandardTool(tMap)
		if std == nil {
			continue
		}
		if stdType, _ := std["type"].(string); stdType != "function" {
			dropped++
			continue
		}
		name, _ := std["name"].(string)
		tool := map[string]interface{}{"name": name}
		if desc, ok := std["description"].(string); ok && desc != "" {
			tool["description"] = desc
		}
		if params, ok := std["parameters"].(map[string]interface{}); ok {
			tool["input_schema"] = params
		} else {
			tool["input_schema"] = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
		}
		tools = append(tools, tool)
	}
	if dropped > 0 {
		warning = fmt.Sprintf("%d built-in tool(s) dropped: not supported on anthropic upstream", dropped)
	}
	return tools, warning
}

// responsesToolChoiceToMessages maps Responses tool_choice to Anthropic tool_choice.
// parallel_tool_calls=false maps to disable_parallel_tool_use on auto/any.
// With thinking enabled, forced choices (any/tool) degrade to auto.
func responsesToolChoiceToMessages(tcVal interface{}, parallelVal interface{}, thinkingEnabled bool) (toolChoice map[string]interface{}, warning string) {
	switch v := tcVal.(type) {
	case string:
		switch v {
		case "auto":
			toolChoice = map[string]interface{}{"type": "auto"}
		case "required":
			toolChoice = map[string]interface{}{"type": "any"}
		case "none":
			toolChoice = map[string]interface{}{"type": "none"}
		}
	case map[string]interface{}:
		if t, _ := v["type"].(string); t == "function" {
			if name, _ := v["name"].(string); name != "" {
				toolChoice = map[string]interface{}{"type": "tool", "name": name}
			}
		} else if t != "" {
			warning = fmt.Sprintf("tool_choice type %q dropped", t)
		}
	}

	if thinkingEnabled && toolChoice != nil {
		if tcType, _ := toolChoice["type"].(string); tcType == "any" || tcType == "tool" {
			toolChoice = map[string]interface{}{"type": "auto"}
			warning = "tool_choice downgraded to auto: forced choice is incompatible with thinking"
		}
	}

	if parallel, ok := parallelVal.(bool); ok && !parallel {
		if toolChoice == nil {
			toolChoice = map[string]interface{}{"type": "auto"}
		}
		if tcType, _ := toolChoice["type"].(string); tcType == "auto" || tcType == "any" {
			toolChoice["disable_parallel_tool_use"] = true
		}
	}
	return toolChoice, warning
}

// MessagesResponseToResponses translates a non-stream Anthropic Messages response to Responses.
func MessagesResponseToResponses(anthropicBody []byte, model string) (MessagesToResponsesResult, error) {
	var aResp map[string]interface{}
	if err := json.Unmarshal(anthropicBody, &aResp); err != nil {
		return MessagesToResponsesResult{}, fmt.Errorf("parse anthropic response: %w", err)
	}

	if errBody, ok := MessagesErrorToResponses(anthropicBody); ok {
		return MessagesToResponsesResult{Body: errBody}, nil
	}

	rawID, _ := aResp["id"].(string)
	respID := normalizeResponsesID(rawID)
	// Client-facing model name wins; upstream echo is only a fallback.
	respModel := model
	if respModel == "" {
		respModel, _ = aResp["model"].(string)
	}

	var output []interface{}
	if contentArr, ok := aResp["content"].([]interface{}); ok {
		for i, block := range contentArr {
			blockMap, ok := block.(map[string]interface{})
			if !ok {
				continue
			}
			switch blockType, _ := blockMap["type"].(string); blockType {
			case "thinking":
				thinking, _ := blockMap["thinking"].(string)
				item := map[string]interface{}{
					"id":   fmt.Sprintf("rs_%s_%d", stripMsgPrefix(rawID), i),
					"type": "reasoning",
					"summary": []interface{}{
						map[string]interface{}{"type": "summary_text", "text": thinking},
					},
				}
				if sig, _ := blockMap["signature"].(string); sig != "" {
					item["encrypted_content"] = sig
				}
				output = append(output, item)
			case "redacted_thinking":
				item := map[string]interface{}{
					"id":      fmt.Sprintf("rs_%s_%d", stripMsgPrefix(rawID), i),
					"type":    "reasoning",
					"summary": []interface{}{},
				}
				if data, _ := blockMap["data"].(string); data != "" {
					item["encrypted_content"] = data
				}
				output = append(output, item)
			case "text":
				text, _ := blockMap["text"].(string)
				output = append(output, map[string]interface{}{
					"id":     fmt.Sprintf("msg_%s_%d", stripMsgPrefix(rawID), i),
					"type":   "message",
					"status": "completed",
					"role":   "assistant",
					"content": []interface{}{
						map[string]interface{}{"type": "output_text", "text": text, "annotations": []interface{}{}},
					},
				})
			case "tool_use":
				toolID, _ := blockMap["id"].(string)
				name, _ := blockMap["name"].(string)
				var arguments string
				if inputBytes, err := json.Marshal(blockMap["input"]); err == nil {
					arguments = string(inputBytes)
				} else {
					arguments = "{}"
				}
				output = append(output, map[string]interface{}{
					"id":        fmt.Sprintf("fc_%s", stripToolUsePrefix(toolID)),
					"call_id":   toolID,
					"type":      "function_call",
					"status":    "completed",
					"name":      name,
					"arguments": arguments,
				})
			}
		}
	}

	status := "completed"
	var incompleteDetails interface{}
	if sr, _ := aResp["stop_reason"].(string); sr == "max_tokens" {
		status = "incomplete"
		incompleteDetails = map[string]interface{}{"reason": "max_output_tokens"}
	}

	inputTokens := intFrom(aRespUsage(aResp, "input_tokens"))
	outputTokens := intFrom(aRespUsage(aResp, "output_tokens"))
	cachedTokens := intFrom(aRespUsage(aResp, "cache_read_input_tokens"))
	cacheCreationTokens := intFrom(aRespUsage(aResp, "cache_creation_input_tokens"))
	// Normalize to OpenAI semantics: input_tokens includes cached + cache-creation.
	totalInput := inputTokens + cachedTokens + cacheCreationTokens

	now := time.Now().Unix()
	responsesResp := map[string]interface{}{
		"id":         respID,
		"object":     "response",
		"created_at": now,
		"status":     status,
		"model":      respModel,
		"output":     output,
		"usage": map[string]interface{}{
			"input_tokens":  totalInput,
			"output_tokens": outputTokens,
			"total_tokens":  totalInput + outputTokens,
			"input_tokens_details": map[string]interface{}{
				"cached_tokens": cachedTokens,
			},
		},
	}
	if incompleteDetails != nil {
		responsesResp["incomplete_details"] = incompleteDetails
	}

	body, err := json.Marshal(responsesResp)
	if err != nil {
		return MessagesToResponsesResult{}, err
	}
	return MessagesToResponsesResult{
		Body:                body,
		Usage:               TokenUsage{InputTokens: totalInput, OutputTokens: outputTokens},
		CachedTokens:        cachedTokens,
		CacheCreationTokens: cacheCreationTokens,
	}, nil
}

// MessagesErrorToResponses converts an Anthropic error envelope to the OpenAI-style
// error body used by the Responses API. ok=false when body is not an Anthropic error.
func MessagesErrorToResponses(body []byte) (converted []byte, ok bool) {
	var envelope struct {
		Type  string `json:"type"`
		Error *struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Error == nil {
		return nil, false
	}
	errType := envelope.Error.Type
	switch errType {
	case "overloaded_error", "api_error":
		errType = "server_error"
	}
	out, err := json.Marshal(map[string]interface{}{
		"error": map[string]interface{}{
			"message": envelope.Error.Message,
			"type":    errType,
			"code":    nil,
		},
	})
	if err != nil {
		return nil, false
	}
	return out, true
}

// aRespUsage reads a numeric usage field from an Anthropic response map.
func aRespUsage(aResp map[string]interface{}, field string) interface{} {
	usage, ok := aResp["usage"].(map[string]interface{})
	if !ok {
		return nil
	}
	return usage[field]
}

// normalizeResponsesID rewrites an upstream id to the resp_ prefix.
func normalizeResponsesID(id string) string {
	if id == "" {
		return "resp_mock"
	}
	stripped := stripMsgPrefix(id)
	if stripped == "" {
		return "resp_mock"
	}
	return "resp_" + stripped
}

// stripToolUsePrefix removes the toolu_ prefix for embedding in fc_ item ids.
func stripToolUsePrefix(id string) string {
	return strings.TrimPrefix(id, "toolu_")
}
