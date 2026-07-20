package llm

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

type contentBlockStartEvent struct {
	Type         string `json:"type"`
	Index        int    `json:"index"`
	ContentBlock struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content_block"`
}

type contentBlockDeltaEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	Delta struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"delta"`
}

type contentBlockStopEvent struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
}

type messageStopEvent struct {
	Type string `json:"type"`
}

type messageStartEvent struct {
	Type    string `json:"type"`
	Message struct {
		ID           string      `json:"id"`
		Type         string      `json:"type"`
		Role         string      `json:"role"`
		Content      []string    `json:"content"`
		Model        string      `json:"model"`
		StopReason   *string     `json:"stop_reason"`
		StopSequence interface{} `json:"stop_sequence"`
		Usage        struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

func writeEvent(w io.Writer, eventType string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, string(jsonData))
	return err
}

func normalizeAnthropicID(id string) string {
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

// TryMockMessagesProbe 检测是否为 Claude Code / Anthropic SDK 的连通性探测请求。
// 如果是，在网关本地进行 Mock 回复，避免将此请求转发给上游。
func TryMockMessagesProbe(gctx *core.GatewayContext) (bool, error) {
	if gctx == nil || len(gctx.RawBody) == 0 {
		return false, nil
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(gctx.RawBody, &payload); err != nil {
		return false, nil // 解析失败说明不是规范的探测请求
	}

	// 检测是否为 Claude Code / Anthropic SDK 的连通性探测请求
	// 特征：max_tokens == 1 且只有一个消息，内容为 "."
	isProbe := false
	if maxTokens, ok := payload["max_tokens"].(float64); ok && maxTokens == 1 {
		if msgs, ok := payload["messages"].([]interface{}); ok && len(msgs) == 1 {
			if firstMsg, ok := msgs[0].(map[string]interface{}); ok {
				if content, ok := firstMsg["content"].(string); ok && content == "." {
					isProbe = true
				}
			}
		}
	}

	if !isProbe {
		return false, nil
	}

	if gctx.Request != nil {
		if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
			gctx.ResponseWriter.Header().Set("anthropic-version", ver)
		} else {
			gctx.ResponseWriter.Header().Set("anthropic-version", "2023-06-01")
		}
	}

	respModel := gctx.OriginalModel
	if respModel == "" {
		respModel = gctx.Model
	}

	if gctx.IsStream {
		gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
		gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
		gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
		gctx.ResponseWriter.Header().Set("X-Accel-Buffering", "no")
		gctx.ResponseWriter.WriteHeader(http.StatusOK)

		msgID := normalizeAnthropicID(fmt.Sprintf("probe%d", time.Now().UnixNano()))

		// 1. message_start
		var startEv messageStartEvent
		startEv.Type = "message_start"
		startEv.Message.ID = msgID
		startEv.Message.Type = "message"
		startEv.Message.Role = "assistant"
		startEv.Message.Content = []string{}
		startEv.Message.Model = respModel
		startEv.Message.Usage.InputTokens = 5
		startEv.Message.Usage.OutputTokens = 1
		if err := writeEvent(gctx.ResponseWriter, "message_start", startEv); err != nil {
			return true, err
		}

		// 2. content_block_start
		var blockStartEv contentBlockStartEvent
		blockStartEv.Type = "content_block_start"
		blockStartEv.Index = 0
		blockStartEv.ContentBlock.Type = "text"
		blockStartEv.ContentBlock.Text = ""
		if err := writeEvent(gctx.ResponseWriter, "content_block_start", blockStartEv); err != nil {
			return true, err
		}

		// 3. content_block_delta
		var deltaEv contentBlockDeltaEvent
		deltaEv.Type = "content_block_delta"
		deltaEv.Index = 0
		deltaEv.Delta.Type = "text_delta"
		deltaEv.Delta.Text = "."
		if err := writeEvent(gctx.ResponseWriter, "content_block_delta", deltaEv); err != nil {
			return true, err
		}

		// 4. content_block_stop
		var blockStopEv contentBlockStopEvent
		blockStopEv.Type = "content_block_stop"
		blockStopEv.Index = 0
		if err := writeEvent(gctx.ResponseWriter, "content_block_stop", blockStopEv); err != nil {
			return true, err
		}

		// 5. message_delta
		var deltaMsgEv struct {
			Type  string `json:"type"`
			Delta struct {
				StopReason   string  `json:"stop_reason"`
				StopSequence *string `json:"stop_sequence"`
			} `json:"delta"`
			Usage struct {
				OutputTokens int `json:"output_tokens"`
			} `json:"usage"`
		}
		deltaMsgEv.Type = "message_delta"
		deltaMsgEv.Delta.StopReason = "max_tokens"
		deltaMsgEv.Delta.StopSequence = nil
		deltaMsgEv.Usage.OutputTokens = 1
		if err := writeEvent(gctx.ResponseWriter, "message_delta", deltaMsgEv); err != nil {
			return true, err
		}

		// 6. message_stop
		var stopEv messageStopEvent
		stopEv.Type = "message_stop"
		if err := writeEvent(gctx.ResponseWriter, "message_stop", stopEv); err != nil {
			return true, err
		}

		if flusher, ok := gctx.ResponseWriter.(http.Flusher); ok {
			flusher.Flush()
		}

		gctx.InputTokens = 5
		gctx.OutputTokens = 1
		gctx.TriggerFirstByte()
		return true, nil
	}

	mockResp := map[string]interface{}{
		"id":            normalizeAnthropicID(fmt.Sprintf("probe%d", time.Now().UnixNano())),
		"type":          "message",
		"role":          "assistant",
		"model":         respModel,
		"content": []interface{}{
			map[string]interface{}{
				"type": "text",
				"text": ".",
			},
		},
		"stop_reason":   "max_tokens",
		"stop_sequence": nil,
		"usage": map[string]interface{}{
			"input_tokens":  5,
			"output_tokens": 1,
		},
	}
	respBytes, err := json.Marshal(mockResp)
	if err != nil {
		return true, err
	}
	gctx.UpstreamBody = respBytes
	gctx.Response = mockResp
	gctx.InputTokens = 5
	gctx.OutputTokens = 1
	gctx.TriggerFirstByte()
	return true, nil
}
