package translate

import (
	"encoding/json"
	"fmt"
	"strings"
)

func chatResponsesID(id, prefix string) string {
	if strings.HasPrefix(id, "chatcmpl-") {
		return strings.Replace(id, "chatcmpl-", prefix, 1)
	}
	if id == "" {
		return prefix + "mock"
	}
	if !strings.HasPrefix(id, prefix) {
		return prefix + id
	}
	return id
}

func splitChatToolName(name string) (namespace, localName string) {
	if idx := strings.LastIndex(name, "."); idx > 0 && idx < len(name)-1 {
		return name[:idx], name[idx+1:]
	}
	return "", name
}

// NormalizeAnthropicID normalizes an upstream id to the Anthropic msg_ prefix.
func NormalizeAnthropicID(id string) string {
	if id == "" {
		return "msg_mockprobe1234567890"
	}
	orig := id
	if strings.HasPrefix(orig, "chatcmpl-") {
		orig = orig[9:]
	} else if strings.HasPrefix(orig, "msg_") {
		orig = orig[4:]
	}
	var sb strings.Builder
	sb.WriteString("msg_")
	for _, ch := range orig {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			sb.WriteRune(ch)
		}
	}
	res := sb.String()
	if len(res) <= 4 {
		return "msg_mockprobe1234567890"
	}
	return res
}

// NormalizeToolUseID normalizes a tool call id to the toolu_ prefix.
func NormalizeToolUseID(id string) string {
	if id == "" {
		return "toolu_mock"
	}
	if strings.HasPrefix(id, "toolu_") {
		return id
	}
	res := strings.TrimPrefix(id, "call_")
	res = strings.TrimPrefix(res, "toolu-")
	return "toolu_" + res
}

// EnsureFunctionCallItemID normalizes a tool call ID to the OpenAI Responses fc_ prefix for item IDs.
func EnsureFunctionCallItemID(id string) string {
	if id == "" {
		return "fc_mock"
	}
	if strings.HasPrefix(id, "fc_") {
		return id
	}
	res := strings.TrimPrefix(id, "call_")
	res = strings.TrimPrefix(res, "toolu_")
	res = strings.TrimPrefix(res, "toolu-")
	if res == "" {
		return "fc_mock"
	}
	return "fc_" + res
}

// EnsureCustomToolCallItemID normalizes a tool call ID to the OpenAI Responses ctc_ prefix for custom tool call item IDs.
func EnsureCustomToolCallItemID(id string) string {
	if id == "" {
		return "ctc_mock"
	}
	if strings.HasPrefix(id, "ctc_") {
		return id
	}
	res := strings.TrimPrefix(id, "call_")
	res = strings.TrimPrefix(res, "fc_")
	res = strings.TrimPrefix(res, "toolu_")
	res = strings.TrimPrefix(res, "toolu-")
	if res == "" {
		return "ctc_mock"
	}
	return "ctc_" + res
}

// ExtractPatchInput extracts raw patch text from arguments.
// If arguments is a JSON object with a "patch", "diff", "content", or "input" field, it returns the string value.
// Otherwise it falls back to the trimmed arguments string itself.
func ExtractPatchInput(args string) string {
	trimmed := strings.TrimSpace(args)
	if strings.HasPrefix(trimmed, "{") {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(trimmed), &m); err == nil {
			for _, key := range []string{"patch", "diff", "content", "input"} {
				if val, ok := m[key].(string); ok {
					return val
				}
			}
		}
	}
	return args
}

func cleanJSONSchema(m map[string]interface{}, removeAdditionalProps bool) map[string]interface{} {
	if m == nil {
		return m
	}
	res := make(map[string]interface{})
	for k, v := range m {
		if k == "$schema" || k == "propertyNames" || k == "minItems" || k == "maxItems" ||
			k == "minLength" || k == "maxLength" || k == "default" || k == "pattern" {
			continue
		}
		if k == "additionalProperties" && removeAdditionalProps {
			continue
		}
		if k == "required" && v == nil {
			continue
		}
		if subMap, ok := v.(map[string]interface{}); ok {
			res[k] = cleanJSONSchema(subMap, removeAdditionalProps)
		} else if subArr, ok := v.([]interface{}); ok {
			newArr := make([]interface{}, 0, len(subArr))
			for _, item := range subArr {
				if itemMap, ok := item.(map[string]interface{}); ok {
					newArr = append(newArr, cleanJSONSchema(itemMap, removeAdditionalProps))
				} else {
					newArr = append(newArr, item)
				}
			}
			res[k] = newArr
		} else {
			res[k] = v
		}
	}
	if isObjectSchema(res) {
		if req, ok := res["required"].([]interface{}); !ok || req == nil {
			res["required"] = make([]interface{}, 0)
		}
	}
	return res
}

func isObjectSchema(m map[string]interface{}) bool {
	if t, ok := m["type"].(string); ok && t == "object" {
		return true
	}
	_, ok := m["properties"]
	return ok
}

func degradeMessagesToTextOnly(msgs []interface{}) []interface{} {
	var temp []interface{}
	for _, m := range msgs {
		mMap, ok := m.(map[string]interface{})
		if !ok {
			temp = append(temp, m)
			continue
		}
		role, _ := mMap["role"].(string)
		if role == "system" && len(temp) > 0 {
			role = "user"
			mMap["role"] = "user"
		}
		if role == "tool" {
			toolCallID, _ := mMap["tool_call_id"].(string)
			content, _ := mMap["content"].(string)
			temp = append(temp, map[string]interface{}{
				"role":    "user",
				"content": fmt.Sprintf("<historical_tool_result id=\"%s\">\n%s\n</historical_tool_result>", toolCallID, content),
			})
		} else if role == "assistant" {
			content, _ := mMap["content"].(string)
			temp = append(temp, map[string]interface{}{
				"role":    "assistant",
				"content": content,
			})
		} else {
			temp = append(temp, mMap)
		}
	}

	var res []interface{}
	for _, m := range temp {
		mMap, ok := m.(map[string]interface{})
		if !ok {
			res = append(res, m)
			continue
		}
		if len(res) == 0 {
			res = append(res, mMap)
			continue
		}
		lastMap, ok := res[len(res)-1].(map[string]interface{})
		if !ok {
			res = append(res, mMap)
			continue
		}
		if lastMap["role"] == mMap["role"] {
			lastContent, lastOK := lastMap["content"].(string)
			thisContent, thisOK := mMap["content"].(string)
			if lastOK && thisOK {
				lastMap["content"] = lastContent + "\n\n" + thisContent
			} else {
				// Non-string content (e.g. multimodal image array) can't be
				// string-merged; keep as a separate message to avoid data loss.
				res = append(res, mMap)
			}
		} else {
			res = append(res, mMap)
		}
	}
	return res
}

func cleanSystemPrompt(prompt string) string {
	lines := strings.Split(prompt, "\n")
	var cleanLines []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "x-anthropic-") {
			continue
		}
		cleanLines = append(cleanLines, line)
	}
	return strings.Join(cleanLines, "\n")
}
