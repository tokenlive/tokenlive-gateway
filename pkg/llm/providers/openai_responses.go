package providers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"

	"go.uber.org/zap"
)

type openaiResponsesInvoker struct{}

func (i *openaiResponsesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	gctx.Logger(zap.L()).Info("openaiResponsesInvoker Invoke entry called", zap.String("model", gctx.Model), zap.String("req_type", string(gctx.RequestType)))
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}

	// 1. Branch decision: does the endpoint natively support responses?
	hasResponseCapability := false
	if gctx.SelectedEndpoint != nil {
		for _, cap := range gctx.SelectedEndpoint.RequestTypes {
			if cap == core.RequestTypeResponses {
				hasResponseCapability = true
				break
			}
		}
	}

	// Branch A: native same-name forwarding
	if hasResponseCapability {
		newBody, orig, final, summary, err := translate.CorrectNativeResponsesRequest(gctx.RawBody)
		if err == nil {
			gctx.RawBody = newBody
			gctx.Logger(zap.L()).Debug("native responses tools conversion result summary",
				zap.Int("original_count", orig),
				zap.Int("final_count", final),
				zap.Strings("final_tools_summary", summary),
			)
		} else {
			gctx.Logger(zap.L()).Warn("failed to correct tools for native responses", zap.Error(err))
		}
		endpoint := op.baseURL + "/responses"
		return op.doRequest(gctx, endpoint)
	}

	// Branch B: protocol downgrade and translation (Responses -> Chat/Completions)
	newBody, err := translate.ResponsesRequestToChat(gctx.RawBody)
	if err != nil {
		return err
	}
	gctx.RawBody = newBody

	// Redirect to upstream /chat/completions
	endpoint := op.baseURL + "/chat/completions"
	if err := op.doRequest(gctx, endpoint); err != nil {
		return err
	}

	// Translate response body (OpenAI Chat -> Responses)
	if gctx.IsStream {
		return handleResponsesStream(gctx, gctx.UpstreamResponse)
	}
	if err := translateResponsesNonStreamResponse(gctx); err != nil {
		return fmt.Errorf("translate response: %w", err)
	}
	return nil
}

// Compatible with same-package callers (joycode / tests)
func translateResponsesToChatCompletion(rawBody []byte) ([]byte, error) {
	return translate.ResponsesRequestToChat(rawBody)
}

func translateResponsesNonStreamResponse(gctx *core.GatewayContext) error {
	res, err := translate.ChatCompletionToResponses(gctx.UpstreamBody, gctx.Model)
	if err != nil {
		return err
	}
	gctx.UpstreamBody = res.Body
	var result map[string]interface{}
	if err := json.Unmarshal(res.Body, &result); err != nil {
		return err
	}
	gctx.Response = result
	return nil
}

type responseCreatedEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
	} `json:"response"`
}

type responseInProgressEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
	} `json:"response"`
}

type responseOutputItemAddedEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID      string        `json:"id"`
		Type    string        `json:"type"`
		Status  string        `json:"status"`
		Role    string        `json:"role"`
		Content []interface{} `json:"content"`
	} `json:"item"`
}

type responseContentPartAddedEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Part         struct {
		Type        string        `json:"type"`
		Text        string        `json:"text"`
		Annotations []interface{} `json:"annotations"`
	} `json:"part"`
}

type responseOutputTextDeltaEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta"`
}

type responseOutputTextDoneEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Text         string `json:"text"`
}

type responseContentPartDoneEvent struct {
	Type         string `json:"type"`
	ResponseID   string `json:"response_id"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Part         struct {
		Type        string        `json:"type"`
		Text        string        `json:"text"`
		Annotations []interface{} `json:"annotations"`
	} `json:"part"`
}

type responseOutputItemDoneEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID      string        `json:"id"`
		Type    string        `json:"type"`
		Status  string        `json:"status"`
		Role    string        `json:"role"`
		Content []interface{} `json:"content"`
	} `json:"item"`
}

type responseDoneEvent struct {
	Type     string `json:"type"`
	Response struct {
		ID        string        `json:"id"`
		Object    string        `json:"object"`
		CreatedAt int64         `json:"created_at"`
		Status    string        `json:"status"`
		Model     string        `json:"model"`
		Output    []interface{} `json:"output"`
		Usage     struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
	} `json:"response"`
}

type responseOutputItemAddedFunctionCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"item"`
}

type responseFunctionCallArgumentsDeltaEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	ItemID      string `json:"item_id"`
	OutputIndex int    `json:"output_index"`
	Delta       string `json:"delta"`
}

type responseFunctionCallArgumentsDoneEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	ItemID      string `json:"item_id"`
	OutputIndex int    `json:"output_index"`
	Arguments   string `json:"arguments"`
}

type responseOutputItemDoneFunctionCallEvent struct {
	Type        string `json:"type"`
	ResponseID  string `json:"response_id"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		ID        string `json:"id"`
		Type      string `json:"type"`
		Status    string `json:"status"`
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"item"`
}

func writeResponseEvent(w io.Writer, eventType string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventType, string(jsonData))
	return err
}



func sendPlainResponsesText(gctx *core.GatewayContext, respID string, txt string, msgID string, messageAdded *bool, textOutputIndex *int, currentOutputIndex *int) error {
	if !*messageAdded {
		*messageAdded = true
		*textOutputIndex = *currentOutputIndex
		// response.output_item.added (text message)
		var evItemAdded responseOutputItemAddedEvent
		evItemAdded.Type = "response.output_item.added"
		evItemAdded.ResponseID = respID
		evItemAdded.OutputIndex = *textOutputIndex
		evItemAdded.Item.ID = msgID
		evItemAdded.Item.Type = "message"
		evItemAdded.Item.Status = "in_progress"
		evItemAdded.Item.Role = "assistant"
		evItemAdded.Item.Content = []interface{}{}
		if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.added", evItemAdded); err != nil {
			return err
		}

		// response.content_part.added (output_text)
		var evPartAdded responseContentPartAddedEvent
		evPartAdded.Type = "response.content_part.added"
		evPartAdded.ResponseID = respID
		evPartAdded.ItemID = msgID
		evPartAdded.OutputIndex = *textOutputIndex
		evPartAdded.ContentIndex = 0
		evPartAdded.Part.Type = "output_text"
		evPartAdded.Part.Text = ""
		evPartAdded.Part.Annotations = []interface{}{}
		if err := writeResponseEvent(gctx.ResponseWriter, "response.content_part.added", evPartAdded); err != nil {
			return err
		}

		*currentOutputIndex++
	}

	var evDelta responseOutputTextDeltaEvent
	evDelta.Type = "response.output_text.delta"
	evDelta.ResponseID = respID
	evDelta.ItemID = msgID
	evDelta.OutputIndex = *textOutputIndex
	evDelta.ContentIndex = 0
	evDelta.Delta = txt

	return writeResponseEvent(gctx.ResponseWriter, "response.output_text.delta", evDelta)
}

func handleResponsesStream(gctx *core.GatewayContext, resp *http.Response) error {
	defer resp.Body.Close()

	defer func() {
		if r := recover(); r != nil {
			gctx.Logger(zap.L()).Error("[DEBUG-responses-stream] panic captured in handleResponsesStream",
				zap.Any("panic_info", r),
				zap.String("stack", string(debug.Stack())),
			)
			panic(r)
		}
	}()

	flusher, hasFlusher := gctx.ResponseWriter.(http.Flusher)

	parser := llm.NewSSEParser()
	buf := make([]byte, 4096)
	started := false

	var fullText strings.Builder
	var lastResponseID string
	var lastModelName string

	messageAdded := false
	textOutputIndex := -1
	currentOutputIndex := 0

	type localToolCall struct {
		ID          string
		Name        string
		Arguments   strings.Builder
		OutputIndex int
		Added       bool
	}
	var localToolCalls = make(map[int]*localToolCall)

	headersSent := false

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			// 1. Sniff SSE error events from all frames
			events := parser.Feed(buf[:n])
			for _, ev := range events {
				if strings.Contains(ev.Data, `"error"`) {
					cleanData := strings.TrimSpace(ev.Data)
					if strings.HasPrefix(cleanData, "data:") {
						cleanData = strings.TrimSpace(strings.TrimPrefix(cleanData, "data:"))
					}

					var errChunk struct {
						Error *struct {
							Message string `json:"message"`
							Type    string `json:"type"`
							Code    any    `json:"code"`
							Cause   string `json:"cause"`
						} `json:"error"`
					}
					if json.Unmarshal([]byte(cleanData), &errChunk) == nil && errChunk.Error != nil && (errChunk.Error.Message != "" || errChunk.Error.Type != "") {
						errMsg := errChunk.Error.Message
						if errChunk.Error.Cause != "" {
							var innerErr struct {
								Error struct {
									Message string `json:"message"`
								} `json:"error"`
							}
							if json.Unmarshal([]byte(errChunk.Error.Cause), &innerErr) == nil && innerErr.Error.Message != "" {
								errMsg = fmt.Sprintf("%s (cause: %s)", errMsg, innerErr.Error.Message)
							} else {
								errMsg = fmt.Sprintf("%s (cause: %s)", errMsg, errChunk.Error.Cause)
							}
						}
						return fmt.Errorf("upstream stream returned error event: %s", errMsg)
					}
				}
			}

			if !headersSent {
				// 2. Sniff first frame raw data for plain JSON error
				trimmed := strings.TrimSpace(string(buf[:n]))
				if strings.HasPrefix(trimmed, "{") {
					var errJSON struct {
						Error struct {
							Message string `json:"message"`
							Type    string `json:"type"`
							Code    any    `json:"code"`
						} `json:"error"`
						Message string `json:"message"`
					}
					if jsonErr := json.Unmarshal([]byte(trimmed), &errJSON); jsonErr == nil {
						errMsg := errJSON.Error.Message
						if errMsg == "" {
							errMsg = errJSON.Message
						}
						if errMsg == "" {
							errMsg = trimmed
						}
						return fmt.Errorf("upstream returned JSON error: %s", errMsg)
					}
					return fmt.Errorf("upstream stream returned JSON error body: %s", trimmed)
				}

				// If headers haven't been sent yet, now is the best time to send them and trigger first-byte timing
				gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
				gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
				gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
				gctx.ResponseWriter.WriteHeader(http.StatusOK)
				gctx.TriggerFirstByte()
				headersSent = true
			}
			hasDone := false
			for _, ev := range events {
				if ev.Done {
					hasDone = true
					break
				}

				if ev.InputTokens > 0 {
					gctx.InputTokens = ev.InputTokens
				}
				if ev.OutputTokens > 0 {
					gctx.OutputTokens = ev.OutputTokens
				}
				if ev.CachedTokens > 0 {
					gctx.CachedTokens = ev.CachedTokens
				}
				if ev.CacheCreationTokens > 0 {
					gctx.CacheCreationTokens = ev.CacheCreationTokens
				}

				var chunk struct {
					ID      string `json:"id"`
					Model   string `json:"model"`
					Choices []struct {
						Delta struct {
							Content   string `json:"content"`
							ToolCalls []struct {
								Index    int    `json:"index"`
								ID       string `json:"id"`
								Type     string `json:"type"`
								Function struct {
									Name      string `json:"name"`
									Arguments string `json:"arguments"`
								} `json:"function"`
							} `json:"tool_calls"`
						} `json:"delta"`
					} `json:"choices"`
				}

				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					gctx.Logger(zap.L()).Warn("[DEBUG-responses-stream] json.Unmarshal failed", zap.String("raw_data", ev.Data), zap.Error(err))
					continue
				}

				if chunk.ID != "" {
					lastResponseID = chunk.ID
					if gctx.Tags == nil {
						gctx.Tags = make(map[string]string)
					}
					respID := chunk.ID
					if strings.HasPrefix(respID, "chatcmpl-") {
						respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
					} else if !strings.HasPrefix(respID, "resp_") {
						respID = "resp_" + respID
					}
					gctx.Tags["response_id"] = respID
				}
				if chunk.Model != "" {
					lastModelName = chunk.Model
					if gctx.Tags == nil {
						gctx.Tags = make(map[string]string)
					}
					gctx.Tags["response_model"] = chunk.Model
				}

				respID := lastResponseID
				if strings.HasPrefix(respID, "chatcmpl-") {
					respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
				} else if respID == "" {
					respID = "resp_mock"
				} else if !strings.HasPrefix(respID, "resp_") {
					respID = "resp_" + respID
				}

				msgID := lastResponseID
				if strings.HasPrefix(msgID, "chatcmpl-") {
					msgID = strings.Replace(msgID, "chatcmpl-", "msg_", 1)
				} else if msgID == "" {
					msgID = "msg_mock"
				} else if !strings.HasPrefix(msgID, "msg_") {
					msgID = "msg_" + msgID
				}

				modelName := lastModelName
				if modelName == "" {
					modelName = gctx.Model
				}

				if !started {
					started = true
					now := time.Now().Unix()

					// 1. response.created
					var evCreated responseCreatedEvent
					evCreated.Type = "response.created"
					evCreated.Response.ID = respID
					evCreated.Response.Object = "response"
					evCreated.Response.CreatedAt = now
					evCreated.Response.Status = "in_progress"
					evCreated.Response.Model = modelName
					evCreated.Response.Output = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.created", evCreated); err != nil {
						return err
					}

					// 2. response.in_progress
					var evInProgress responseInProgressEvent
					evInProgress.Type = "response.in_progress"
					evInProgress.Response.ID = respID
					evInProgress.Response.Object = "response"
					evInProgress.Response.CreatedAt = now
					evInProgress.Response.Status = "in_progress"
					evInProgress.Response.Model = modelName
					evInProgress.Response.Output = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.in_progress", evInProgress); err != nil {
						return err
					}

					// 2.1 Pre-emptively send response.output_item.added (message) so the client renderer can register view element IDs
					messageAdded = true
					textOutputIndex = currentOutputIndex
					currentOutputIndex++

					var evItemAdded responseOutputItemAddedEvent
					evItemAdded.Type = "response.output_item.added"
					evItemAdded.ResponseID = respID
					evItemAdded.OutputIndex = textOutputIndex
					evItemAdded.Item.ID = msgID
					evItemAdded.Item.Type = "message"
					evItemAdded.Item.Status = "in_progress"
					evItemAdded.Item.Role = "assistant"
					evItemAdded.Item.Content = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.added", evItemAdded); err != nil {
						return err
					}

					// 2.2 Pre-send response.content_part.added (output_text)
					var evPartAdded responseContentPartAddedEvent
					evPartAdded.Type = "response.content_part.added"
					evPartAdded.ResponseID = respID
					evPartAdded.ItemID = msgID
					evPartAdded.OutputIndex = textOutputIndex
					evPartAdded.ContentIndex = 0
					evPartAdded.Part.Type = "output_text"
					evPartAdded.Part.Text = ""
					evPartAdded.Part.Annotations = []interface{}{}
					if err := writeResponseEvent(gctx.ResponseWriter, "response.content_part.added", evPartAdded); err != nil {
						return err
					}

					if hasFlusher {
						flusher.Flush()
					}
				}

				if len(chunk.Choices) > 0 {
					choice := chunk.Choices[0]

					// Process text
					txt := choice.Delta.Content
					if txt != "" {
						if err := sendPlainResponsesText(gctx, respID, txt, msgID, &messageAdded, &textOutputIndex, &currentOutputIndex); err != nil {
							return err
						}
						fullText.WriteString(txt)
						gctx.TransmittedChars += len(txt)
					}

					// Process tool calls
					if len(choice.Delta.ToolCalls) > 0 {
						for _, tc := range choice.Delta.ToolCalls {
							localTC, exist := localToolCalls[tc.Index]
							if !exist {
								localTC = &localToolCall{}
								if tc.ID != "" {
									localTC.ID = tc.ID
								} else {
									localTC.ID = fmt.Sprintf("call_%s_%d", msgID, tc.Index)
								}
								localTC.Name = tc.Function.Name
								localTC.OutputIndex = currentOutputIndex
								currentOutputIndex++
								localToolCalls[tc.Index] = localTC
							}
							if tc.Function.Name != "" && localTC.Name == "" {
								localTC.Name = tc.Function.Name
							}

							// Once we have a tool name and haven't sent the added event yet, send it immediately
							if localTC.Name != "" && !localTC.Added {
								localTC.Added = true
								var evTCAdded responseOutputItemAddedFunctionCallEvent
								evTCAdded.Type = "response.output_item.added"
								evTCAdded.ResponseID = respID
								evTCAdded.OutputIndex = localTC.OutputIndex
								evTCAdded.Item.ID = localTC.ID
								evTCAdded.Item.Type = "function_call"
								evTCAdded.Item.Status = "in_progress"
								evTCAdded.Item.Name = localTC.Name
								evTCAdded.Item.Arguments = ""
								if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.added", evTCAdded); err != nil {
									return err
								}
							}

							argDelta := tc.Function.Arguments
							if argDelta != "" {
								localTC.Arguments.WriteString(argDelta)

								// Send arguments delta
								var evTCDelta responseFunctionCallArgumentsDeltaEvent
								evTCDelta.Type = "response.function_call.arguments.delta"
								evTCDelta.ResponseID = respID
								evTCDelta.ItemID = localTC.ID
								evTCDelta.OutputIndex = localTC.OutputIndex
								evTCDelta.Delta = argDelta
								if err := writeResponseEvent(gctx.ResponseWriter, "response.function_call.arguments.delta", evTCDelta); err != nil {
									return err
								}
							}
						}
					}

					if hasFlusher {
						flusher.Flush()
					}
				}
			}
			if hasDone {
				break
			}
		}

		if err != nil {
			if err == io.EOF {
				if !headersSent {
					return fmt.Errorf("upstream stream closed before sending any data (EOF)")
				}
				break
			}
			return fmt.Errorf("read upstream stream: %w", err)
		}
	}

	if !headersSent {
		gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
		gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
		gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
		gctx.ResponseWriter.WriteHeader(http.StatusOK)
		gctx.TriggerFirstByte()
	}

	if started {
		respID := lastResponseID
		if strings.HasPrefix(respID, "chatcmpl-") {
			respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
		} else if respID == "" {
			respID = "resp_mock"
		} else if !strings.HasPrefix(respID, "resp_") {
			respID = "resp_" + respID
		}

		msgID := lastResponseID
		if strings.HasPrefix(msgID, "chatcmpl-") {
			msgID = strings.Replace(msgID, "chatcmpl-", "msg_", 1)
		} else if msgID == "" {
			msgID = "msg_mock"
		} else if !strings.HasPrefix(msgID, "msg_") {
			msgID = "msg_" + msgID
		}



		modelName := lastModelName
		if modelName == "" {
			modelName = gctx.Model
		}

		now := time.Now().Unix()

		// Finalize text message
		if messageAdded {
			finalText := fullText.String()

			// 5. response.output_text.done
			var evTextDone responseOutputTextDoneEvent
			evTextDone.Type = "response.output_text.done"
			evTextDone.ResponseID = respID
			evTextDone.ItemID = msgID
			evTextDone.OutputIndex = textOutputIndex
			evTextDone.ContentIndex = 0
			evTextDone.Text = finalText
			if err := writeResponseEvent(gctx.ResponseWriter, "response.output_text.done", evTextDone); err != nil {
				return err
			}

			// 6. response.content_part.done
			var evPartDone responseContentPartDoneEvent
			evPartDone.Type = "response.content_part.done"
			evPartDone.ResponseID = respID
			evPartDone.ItemID = msgID
			evPartDone.OutputIndex = textOutputIndex
			evPartDone.ContentIndex = 0
			evPartDone.Part.Type = "output_text"
			evPartDone.Part.Text = finalText
			evPartDone.Part.Annotations = []interface{}{}
			if err := writeResponseEvent(gctx.ResponseWriter, "response.content_part.done", evPartDone); err != nil {
				return err
			}

			// 7. response.output_item.done
			var evItemDone responseOutputItemDoneEvent
			evItemDone.Type = "response.output_item.done"
			evItemDone.ResponseID = respID
			evItemDone.OutputIndex = textOutputIndex
			evItemDone.Item.ID = msgID
			evItemDone.Item.Type = "message"
			evItemDone.Item.Status = "completed"
			evItemDone.Item.Role = "assistant"
			evItemDone.Item.Content = []interface{}{
				map[string]interface{}{
					"type":        "output_text",
					"text":        finalText,
					"annotations": []interface{}{},
				},
			}
			if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.done", evItemDone); err != nil {
				return err
			}
		}

		// Finalize tool calls
		var outputs []interface{}
		outputs = append(outputs, map[string]interface{}{
			"id":     msgID,
			"type":   "message",
			"status": "completed",
			"role":   "assistant",
			"content": []interface{}{
				map[string]interface{}{
					"type":        "output_text",
					"text":        fullText.String(),
					"annotations": []interface{}{},
				},
			},
		})

		// Iterate tool call completion events in index order
		var indices []int
		for idx := range localToolCalls {
			indices = append(indices, idx)
		}
		sort.Ints(indices)

		for _, idx := range indices {
			tc := localToolCalls[idx]
			finalArgs := tc.Arguments.String()

			// Send arguments done
			var evTCDone responseFunctionCallArgumentsDoneEvent
			evTCDone.Type = "response.function_call.arguments.done"
			evTCDone.ResponseID = respID
			evTCDone.ItemID = tc.ID
			evTCDone.OutputIndex = tc.OutputIndex
			evTCDone.Arguments = finalArgs
			if err := writeResponseEvent(gctx.ResponseWriter, "response.function_call.arguments.done", evTCDone); err != nil {
				return err
			}

			// Send output_item.done
			var evTCItemDone responseOutputItemDoneFunctionCallEvent
			evTCItemDone.Type = "response.output_item.done"
			evTCItemDone.ResponseID = respID
			evTCItemDone.OutputIndex = tc.OutputIndex
			evTCItemDone.Item.ID = tc.ID
			evTCItemDone.Item.Type = "function_call"
			evTCItemDone.Item.Status = "completed"
			evTCItemDone.Item.Name = tc.Name
			evTCItemDone.Item.Arguments = finalArgs
			if err := writeResponseEvent(gctx.ResponseWriter, "response.output_item.done", evTCItemDone); err != nil {
				return err
			}

			outputs = append(outputs, map[string]interface{}{
				"id":        tc.ID,
				"type":      "function_call",
				"status":    "completed",
				"name":      tc.Name,
				"arguments": finalArgs,
			})
		}

		// 8. response.done
		var evCompleted responseDoneEvent
		evCompleted.Type = "response.done"
		evCompleted.Response.ID = respID
		evCompleted.Response.Object = "response"
		evCompleted.Response.CreatedAt = now - 1
		evCompleted.Response.Status = "completed"
		evCompleted.Response.Model = modelName
		evCompleted.Response.Output = outputs
		evCompleted.Response.Usage.InputTokens = gctx.InputTokens
		evCompleted.Response.Usage.OutputTokens = gctx.OutputTokens
		evCompleted.Response.Usage.TotalTokens = gctx.InputTokens + gctx.OutputTokens
		if err := writeResponseEvent(gctx.ResponseWriter, "response.done", evCompleted); err != nil {
			return err
		}

		// Also send response.completed for old client compatibility to avoid indefinite waiting timeout
		evCompleted.Type = "response.completed"
		if err := writeResponseEvent(gctx.ResponseWriter, "response.completed", evCompleted); err != nil {
			return err
		}

		if gctx.Tags == nil {
			gctx.Tags = make(map[string]string)
		}
		gctx.Tags["response_completed_sent"] = "true"

		// Send data: [DONE] to explicitly end the client's SSE listening
		_, _ = fmt.Fprintf(gctx.ResponseWriter, "data: [DONE]\n\n")

		if hasFlusher {
			flusher.Flush()
		}
	}

	return nil
}


