package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/upstream"

	"go.uber.org/zap"
)

func init() {
	core.RegisterProviderFactory(core.ProviderOpenAI, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewOpenAIProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeChatCompletion, &openaiChatInvoker{})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeEmbedding, &openaiEmbeddingInvoker{})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeResponses, &openaiResponsesInvoker{})
	core.RegisterRequestInvoker(core.ProviderOpenAI, core.RequestTypeMessages, &openaiMessagesInvoker{})
}

// OpenAIProvider implements core.Provider, adapting OpenAI-compatible APIs.
type OpenAIProvider struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
	models  []string
}

// NewOpenAIProvider creates an OpenAI provider instance.
func NewOpenAIProvider(name, baseURL, apiKey string, models []string) *OpenAIProvider {
	return &OpenAIProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{},
		models:  models,
	}
}

func (p *OpenAIProvider) Name() string            { return p.name }
func (p *OpenAIProvider) Type() core.ProviderType { return core.ProviderOpenAI }
func (p *OpenAIProvider) ValidateConfig() error   { return nil }

// RequestTypes returns the request types supported by this provider.
func (p *OpenAIProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{
		core.RequestTypeChatCompletion,
		core.RequestTypeEmbedding,
		core.RequestTypeResponses,
		core.RequestTypeMessages,
	}
}

// HealthCheck probes upstream reachability via GET /models.
func (p *OpenAIProvider) HealthCheck(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/models", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return fmt.Errorf("health check failed: status %d", resp.StatusCode)
	}
	return nil
}

// Invoke dispatches to the corresponding RequestInvoker handler based on request type.
func (p *OpenAIProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

// doRequest handles POST requests uniformly (chat completion, embedding, etc.).
// Transport details are handled by upstream.Call; this function only prepares auth headers / protocol-specific body
// and selects Consume or Handoff.
func (p *OpenAIProvider) doRequest(gctx *core.GatewayContext, endpoint string) error {
	body := gctx.RawBody
	if gctx.IsStream {
		// To prevent third-party OpenAI-compatible endpoints (e.g. Volcano Engine) from erroring with InvalidParameter,
		// only inject stream_options when explicitly targeting official OpenAI or local testing.
		isOfficialOrTest := strings.Contains(p.baseURL, "api.openai.com") ||
			strings.Contains(p.baseURL, "127.0.0.1") ||
			strings.Contains(p.baseURL, "localhost")
		if isOfficialOrTest {
			body = ensureStreamUsage(body)
		}
	}

	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	h.Set("Authorization", "Bearer "+p.apiKey)

	// For Messages translation / Responses downgrade to chat, body is handed off to the translation invoker
	streamMode := upstream.Consume
	if gctx.IsStream {
		if gctx.RequestType == core.RequestTypeMessages ||
			(gctx.RequestType == core.RequestTypeResponses && strings.HasSuffix(endpoint, "/chat/completions")) {
			streamMode = upstream.Handoff
		}
	}

	resp, err := upstream.Call(gctx, upstream.Request{
		Client: p.client,
		URL:    endpoint,
		Body:   body,
		Header: h,
		Stream: streamMode,
	})
	if err != nil {
		return err
	}

	if streamMode == upstream.Handoff {
		// Translation layer handles Close; body is already attached to gctx.UpstreamResponse
		return nil
	}

	defer resp.Body.Close()
	if gctx.IsStream {
		return handleOpenAIStream(gctx, resp)
	}
	return handleOpenAINonStream(gctx, resp)
}

// handleOpenAIStream handles SSE streaming responses, intercepting the byte stream via SSEInterceptWriter to extract token stats.
func handleOpenAIStream(gctx *core.GatewayContext, resp *http.Response) error {
	defer func() {
		if r := recover(); r != nil {
			gctx.Logger(zap.L()).Error("[DEBUG-openai-stream] panic captured in handleOpenAIStream",
				zap.Any("panic_info", r),
				zap.String("stack", string(debug.Stack())),
			)
			panic(r)
		}
	}()

	// Native responses passthrough nests usage under response.usage; chat uses top-level usage.
	var writerOpts []llm.SSEOption
	if gctx.RequestType == core.RequestTypeResponses {
		writerOpts = append(writerOpts, llm.WithTokenExtractor(llm.ResponsesTokenExtractor))
	}
	writer := llm.NewSSEInterceptWriter(gctx, writerOpts...)

	buf := make([]byte, 4096)
	headersSent := false
	parser := llm.NewSSEParser()
	pending := make([]byte, 0, len(buf))
	for {
		n, err := resp.Body.Read(buf)
		previousPendingLen := len(pending)
		if n > 0 {
			pending = append(pending, buf[:n]...)
		}

		completeLen := completeSSEPrefixLen(pending, max(0, previousPendingLen-3))
		if completeLen == 0 && len(pending) > maxOpenAISSEFrameBytes {
			return fmt.Errorf("upstream SSE frame exceeds %d bytes", maxOpenAISSEFrameBytes)
		}
		rawFrames := pending[:completeLen]

		if len(rawFrames) > 0 {
			// 1. Sniff SSE error events from all frames
			events := parser.Feed(rawFrames)
			for _, ev := range events {
				if gctx.RequestType == core.RequestTypeResponses && gctx.GetTagValue("response_id") == "" {
					var respChunk struct {
						ResponseID string `json:"response_id"`
						Response   *struct {
							ID    string `json:"id"`
							Model string `json:"model"`
						} `json:"response"`
						Model string `json:"model"`
					}
					cleanData := strings.TrimSpace(ev.Data)
					if strings.HasPrefix(cleanData, "data:") {
						cleanData = strings.TrimSpace(strings.TrimPrefix(cleanData, "data:"))
					}
					if json.Unmarshal([]byte(cleanData), &respChunk) == nil {
						respID := respChunk.ResponseID
						if respID == "" && respChunk.Response != nil {
							respID = respChunk.Response.ID
						}
						if respID != "" {
							if strings.HasPrefix(respID, "chatcmpl-") {
								respID = strings.Replace(respID, "chatcmpl-", "resp_", 1)
							} else if !strings.HasPrefix(respID, "resp_") {
								respID = "resp_" + respID
							}
							if gctx.Tags == nil {
								gctx.Tags = make(map[string]string)
							}
							gctx.Tags["response_id"] = respID
						}

						modelName := respChunk.Model
						if modelName == "" && respChunk.Response != nil {
							modelName = respChunk.Response.Model
						}
						if modelName != "" {
							if gctx.Tags == nil {
								gctx.Tags = make(map[string]string)
							}
							gctx.Tags["response_model"] = modelName
						}
					}
				}

				if gctx.RequestType == core.RequestTypeResponses {
					cleanData := strings.TrimSpace(ev.Data)
					if strings.HasPrefix(cleanData, "data:") {
						cleanData = strings.TrimSpace(strings.TrimPrefix(cleanData, "data:"))
					}
					var eventTypeCheck struct {
						Type string `json:"type"`
					}
					if json.Unmarshal([]byte(cleanData), &eventTypeCheck) == nil {
						if eventTypeCheck.Type == "response.done" || eventTypeCheck.Type == "response.completed" {
							if gctx.Tags == nil {
								gctx.Tags = make(map[string]string)
							}
							gctx.Tags["response_completed_sent"] = "true"
						}
					}
				}

				if streamErr := openAIStreamEventError(ev.Data); streamErr != nil {
					return streamErr
				}
			}

			if !headersSent {
				// 2. Sniff first frame raw data for plain JSON error
				if jsonErr := openAIJSONStreamError(rawFrames); jsonErr != nil {
					return jsonErr
				}

				// Normal data — send headers and start writing
				writer.Header().Set("Content-Type", "text/event-stream")
				writer.Header().Set("Cache-Control", "no-cache")
				writer.Header().Set("Connection", "keep-alive")
				writer.WriteHeader(http.StatusOK)
				headersSent = true
			}

			if _, werr := writer.Write(rawFrames); werr != nil {
				return werr
			}
			writer.Flush()

			copy(pending, pending[completeLen:])
			pending = pending[:len(pending)-completeLen]
			if len(pending) > maxOpenAISSEFrameBytes {
				return fmt.Errorf("upstream SSE frame exceeds %d bytes", maxOpenAISSEFrameBytes)
			}
		}
		if err != nil {
			if err == io.EOF {
				if len(bytes.TrimSpace(pending)) > 0 {
					if jsonErr := openAIJSONStreamError(pending); jsonErr != nil {
						return jsonErr
					}
					tailParser := llm.NewSSEParser()
					tailEvents := tailParser.Feed(append(append([]byte(nil), pending...), '\n', '\n'))
					for _, ev := range tailEvents {
						if streamErr := openAIStreamEventError(ev.Data); streamErr != nil {
							return streamErr
						}
					}
					return fmt.Errorf("upstream stream closed with incomplete SSE frame")
				}
				if !headersSent {
					return fmt.Errorf("upstream stream closed before sending any data (EOF)")
				}
				break
			}
			if errors.Is(err, context.Canceled) && gctx.Request != nil && gctx.Request.Context().Err() != nil {
				return fmt.Errorf("%w: %v", core.ErrClientDisconnected, err)
			}
			return fmt.Errorf("read upstream stream: %w", err)
		}
	}

	if !headersSent {
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.Header().Set("Cache-Control", "no-cache")
		writer.Header().Set("Connection", "keep-alive")
		writer.WriteHeader(http.StatusOK)
	}

	return nil
}

const maxOpenAISSEFrameBytes = 1 << 20

func completeSSEPrefixLen(data []byte, start int) int {
	if start < 0 {
		start = 0
	}

	completeLen := 0
	for i := start; i < len(data); i++ {
		end := 0
		switch data[i] {
		case '\n':
			if i+1 < len(data) && data[i+1] == '\n' {
				end = i + 2
			} else if i+2 < len(data) && data[i+1] == '\r' && data[i+2] == '\n' {
				end = i + 3
			}
		case '\r':
			if i+1 < len(data) && data[i+1] == '\r' {
				end = i + 2
			} else if i+2 < len(data) && data[i+1] == '\n' && data[i+2] == '\n' {
				end = i + 3
			} else if i+3 < len(data) && data[i+1] == '\n' && data[i+2] == '\r' && data[i+3] == '\n' {
				end = i + 4
			}
		}
		if end > 0 {
			completeLen = end
			i = end - 1
		}
	}
	return completeLen
}

func openAIStreamEventError(data string) error {
	if !strings.Contains(data, `"error"`) {
		return nil
	}

	cleanData := strings.TrimSpace(data)
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
	if json.Unmarshal([]byte(cleanData), &errChunk) != nil || errChunk.Error == nil || (errChunk.Error.Message == "" && errChunk.Error.Type == "") {
		return nil
	}

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

func openAIJSONStreamError(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if !strings.HasPrefix(trimmed, "{") {
		return nil
	}

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

// handleOpenAINonStream handles non-streaming responses, parsing JSON and extracting usage info.
func handleOpenAINonStream(gctx *core.GatewayContext, resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	gctx.TriggerFirstByte()
	gctx.UpstreamBody = body

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	gctx.Response = result

	in, out, cached, cacheCreated := llm.OpenAITokenExtractor(string(body))
	llm.ApplyUsage(gctx, in, out, cached, cacheCreated)
	return nil
}

// ensureStreamUsage injects stream_options.include_usage = true into the streaming request body,
// so the upstream OpenAI-compatible API returns usage stats in the final SSE chunk.
func ensureStreamUsage(body []byte) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}

	var opts struct {
		IncludeUsage *bool `json:"include_usage"`
	}
	if raw, ok := m["stream_options"]; ok {
		if err := json.Unmarshal(raw, &opts); err == nil && opts.IncludeUsage != nil && *opts.IncludeUsage {
			return body // already has include_usage: true, no modification needed
		}
	}

	m["stream_options"] = json.RawMessage(`{"include_usage":true}`)
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// Compile-time interface assertion
var _ core.Provider = (*OpenAIProvider)(nil)
