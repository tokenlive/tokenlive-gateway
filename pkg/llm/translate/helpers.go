package translate

import (
	"fmt"
	"strings"
)

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
			lastContent, _ := lastMap["content"].(string)
			thisContent, _ := mMap["content"].(string)
			lastMap["content"] = lastContent + "\n\n" + thisContent
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
