package translate

import (
	"fmt"
	"strings"
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

func firstNonEmptyReasoning(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// 别名是路径规则，不是所有协议共用的接受集合。非流 Messages 只接受 reasoning_content。
func chatResponsesReasoning(reasoningContent, reasoning string) string {
	return firstNonEmptyReasoning(reasoningContent, reasoning)
}

func chatMessagesStreamReasoning(reasoningContent, thinking, reasoning, thought string) string {
	return firstNonEmptyReasoning(reasoningContent, thinking, reasoning, thought)
}

func responsesReasoningEffort(payload map[string]interface{}) string {
	if reasoning, ok := payload["reasoning"].(map[string]interface{}); ok {
		effort, _ := reasoning["effort"].(string)
		return effort
	}
	return ""
}

func hasReasoningEffort(effort string) bool {
	return effort != "" && effort != "none"
}

// pending 的覆盖/消费时机由 caller 的轮次组装决定；这里只补 assistant 字段。
func applyAssistantReasoning(message map[string]interface{}, text string, enabled bool) {
	if message["role"] != "assistant" {
		return
	}
	if text != "" {
		message["reasoning_content"] = text
	} else if enabled {
		if _, exists := message["reasoning_content"]; !exists {
			message["reasoning_content"] = ""
		}
	}
}

// EnsureChatReasoningHistory 在 Chat 参数或历史非 null reasoning_content 表达意图时，
// 仅补齐 assistant 缺失的 reasoning_content；已有 null、false、空值均保留。
// 修改普通解码 map，不处理传输或 Provider 协议特化。
func EnsureChatReasoningHistory(payload map[string]interface{}) {
	effort, _ := payload["reasoning_effort"].(string)
	enabled := hasReasoningEffort(effort)
	if thinking, ok := payload["thinking"].(map[string]interface{}); ok {
		if kind, _ := thinking["type"].(string); kind != "" && kind != "disabled" {
			enabled = true
		}
	}
	messages, ok := payload["messages"].([]interface{})
	if !ok {
		return
	}
	for _, message := range messages {
		if message, ok := message.(map[string]interface{}); ok {
			if reasoning, exists := message["reasoning_content"]; exists && reasoning != nil {
				enabled = true
				break
			}
		}
	}
	if enabled {
		for _, message := range messages {
			if message, ok := message.(map[string]interface{}); ok {
				applyAssistantReasoning(message, "", true)
			}
		}
	}
}

// extractReasoningSummary 提取 Responses 历史明文供 Chat 使用，不添加分隔符；
// 签名 thinking 回放使用下方独立规则。
func extractReasoningSummary(itemMap map[string]interface{}) string {
	if summary, ok := itemMap["summary"].([]interface{}); ok {
		if text := reasoningPartsText(summary); text != "" {
			return text
		}
	}
	summary, _ := itemMap["summary"].(string)
	reasoningContent, _ := itemMap["reasoning_content"].(string)
	text, _ := itemMap["text"].(string)
	if text := firstNonEmptyReasoning(summary, reasoningContent, text); text != "" {
		return text
	}
	if content, ok := itemMap["content"].([]interface{}); ok {
		return reasoningPartsText(content)
	}
	return ""
}

func reasoningPartsText(parts []interface{}) string {
	var text strings.Builder
	for _, part := range parts {
		if partMap, ok := part.(map[string]interface{}); ok {
			if value, _ := partMap["text"].(string); value != "" {
				text.WriteString(value)
			}
		} else if value, ok := part.(string); ok && value != "" {
			text.WriteString(value)
		}
	}
	return text.String()
}

func thinkingText(block map[string]interface{}) string {
	text, _ := block["thinking"].(string)
	return text
}

func messagesThinkingBlock(text string) map[string]interface{} {
	return map[string]interface{}{"type": "thinking", "thinking": text}
}

func responsesReasoningToThinking(item map[string]interface{}) map[string]interface{} {
	signature, _ := item["encrypted_content"].(string)
	if signature == "" {
		return nil
	}
	var parts []string
	if summary, ok := item["summary"].([]interface{}); ok {
		for _, part := range summary {
			if part, ok := part.(map[string]interface{}); ok {
				if text, ok := part["text"].(string); ok {
					parts = append(parts, text)
				}
			}
		}
	}
	block := messagesThinkingBlock(strings.Join(parts, "\n"))
	block["signature"] = signature
	return block
}

func reasoningSummaryPart(text string) map[string]interface{} {
	return map[string]interface{}{"type": "summary_text", "text": text}
}

// 包含空 summary_text 是非流 thinking 的旧规则；stream 空摘要由 caller 保持 []。
func reasoningSummary(text string) []interface{} {
	return []interface{}{reasoningSummaryPart(text)}
}

func responsesReasoningItem(id string, summary []interface{}, encryptedContent string) map[string]interface{} {
	item := map[string]interface{}{"id": id, "type": "reasoning", "summary": summary}
	if encryptedContent != "" {
		item["encrypted_content"] = encryptedContent
	}
	return item
}

func completedChatReasoningItem(id, text string) map[string]interface{} {
	item := responsesReasoningItem(id, reasoningSummary(text), "")
	item["status"] = "completed"
	return item
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
