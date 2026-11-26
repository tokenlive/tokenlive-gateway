package providers

import (
	"bytes"
	"encoding/json"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

func hasChatDialects(endpoint *core.Endpoint) bool {
	return endpoint != nil && len(endpoint.DialectImplementations) != 0
}

func adaptChatDialect(implementations []string, body []byte) ([]byte, error) {
	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &payload) != nil {
		return body, nil
	}
	changed := false
	for _, implementation := range implementations {
		switch implementation {
		case "chat_developer_to_system":
			rewritten, err := rewriteChatDeveloperRoles(payload["messages"])
			if err != nil {
				return nil, err
			}
			if rewritten != nil {
				payload["messages"] = rewritten
				changed = true
			}
		case "chat_store_unsupported":
			switch string(bytes.TrimSpace(payload["store"])) {
			case "true":
				return nil, core.ChatDialectError("store=true is not supported for this model")
			case "false":
				delete(payload, "store")
				changed = true
			}
		}
	}
	if !changed {
		return body, nil
	}
	return json.Marshal(payload)
}

func rewriteChatDeveloperRoles(raw json.RawMessage) (json.RawMessage, error) {
	var messages []json.RawMessage
	if json.Unmarshal(raw, &messages) != nil {
		return nil, nil
	}
	var hasSystem, hasDeveloper bool
	for index, rawMessage := range messages {
		var message map[string]json.RawMessage
		if json.Unmarshal(rawMessage, &message) != nil {
			continue
		}
		var role string
		_ = json.Unmarshal(message["role"], &role)
		switch role {
		case "system":
			hasSystem = true
		case "developer":
			hasDeveloper = true
			message["role"] = json.RawMessage(`"system"`)
			rewritten, err := json.Marshal(message)
			if err != nil {
				return nil, err
			}
			messages[index] = rewritten
		}
	}
	if hasSystem && hasDeveloper {
		return nil, core.ChatDialectError("system and developer roles cannot be combined for this model")
	}
	if !hasDeveloper {
		return nil, nil
	}
	return json.Marshal(messages)
}
