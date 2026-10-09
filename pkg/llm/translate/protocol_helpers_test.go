package translate_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
)

func TestChatResponses_IDCompatibility(t *testing.T) {
	for _, tc := range []struct {
		id, responseID, messageID, reasoningID string
	}{
		{"", "resp_mock", "msg_mock", "rs_mock"},
		{"chatcmpl-abc", "resp_abc", "msg_abc", "rs_abc"},
		{"chatcmpl-", "resp_", "msg_", "rs_"},
		{"resp_abc", "resp_abc", "msg_resp_abc", "rs_resp_abc"},
		{"msg_abc", "resp_msg_abc", "msg_abc", "rs_msg_abc"},
		{"raw-中文", "resp_raw-中文", "msg_raw-中文", "rs_raw-中文"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"id":%q,"choices":[{"message":{"content":"text","reasoning_content":"thought"}}]}`, tc.id))
			result, err := translate.ChatCompletionToResponses(body, "model", nil)
			require.NoError(t, err)
			var response map[string]interface{}
			require.NoError(t, json.Unmarshal(result.Body, &response))
			require.Equal(t, tc.responseID, response["id"])
			output := response["output"].([]interface{})
			require.Equal(t, tc.reasoningID, output[0].(map[string]interface{})["id"])
			require.Equal(t, tc.messageID, output[1].(map[string]interface{})["id"])

			stream := translate.NewChatToResponsesStream("model", nil, translate.TokenUsage{})
			stream.FeedJSON(fmt.Sprintf(`{"id":%q,"choices":[{"delta":{"content":"text","reasoning_content":"thought"}}]}`, tc.id))
			events, _ := stream.Finish(translate.TokenUsage{})
			var completed map[string]interface{}
			require.NoError(t, json.Unmarshal(events[len(events)-1].Data, &completed))
			final := completed["response"].(map[string]interface{})
			require.Equal(t, tc.responseID, final["id"])
			output = final["output"].([]interface{})
			require.Equal(t, tc.reasoningID, output[0].(map[string]interface{})["id"])
			require.Equal(t, tc.messageID, output[1].(map[string]interface{})["id"])
		})
	}
}

func TestToolNameMapper_DotBoundaryCompatibility(t *testing.T) {
	mapper := translate.NewToolNameMapper()
	for _, tc := range []struct{ input, namespace, name string }{
		{"plain", "", "plain"},
		{".leading", "", ".leading"},
		{"trailing.", "", "trailing."},
		{".", "", "."},
		{"a.b.c", "a.b", "c"},
		{"a..b", "a.", "b"},
		{"", "", ""},
	} {
		t.Run(tc.input, func(t *testing.T) {
			namespace, name := mapper.Restore(tc.input)
			require.Equal(t, tc.namespace, namespace)
			require.Equal(t, tc.name, name)
		})
	}
}

func TestMessagesToChatCompletion_StopReasonCompatibility(t *testing.T) {
	for _, tc := range []struct {
		stop string
		tool bool
		want string
	}{
		{"max_tokens", true, "length"},
		{"tool_use", false, "tool_calls"},
		{"end_turn", true, "tool_calls"},
		{"stop_sequence", false, "stop"},
		{"unknown", true, "tool_calls"},
		{"unknown", false, "stop"},
		{"", true, "tool_calls"},
	} {
		t.Run(fmt.Sprintf("%s_tool_%v", tc.stop, tc.tool), func(t *testing.T) {
			content := `[{"type":"text","text":"ok"}]`
			if tc.tool {
				content = `[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{}}]`
			}
			body := []byte(fmt.Sprintf(`{"stop_reason":%q,"content":%s}`, tc.stop, content))
			result, err := translate.MessagesToChatCompletion(body, "model")
			require.NoError(t, err)
			var response map[string]interface{}
			require.NoError(t, json.Unmarshal(result.Body, &response))
			choice := response["choices"].([]interface{})[0].(map[string]interface{})
			require.Equal(t, tc.want, choice["finish_reason"])
		})
	}
}
