package providers

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/translate"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/upstream"
)

func init() {
	core.RegisterProviderFactory(core.ProviderJoyCode, func(name, baseURL, apiKey string, models []string) core.Provider {
		return NewJoyCodeProvider(name, baseURL, apiKey, models)
	})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeChatCompletion, &joycodeChatInvoker{})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeResponses, &joycodeResponsesInvoker{})
	core.RegisterRequestInvoker(core.ProviderJoyCode, core.RequestTypeMessages, &joycodeMessagesInvoker{})
}

// JoyCodeProvider adapts the JD JoyCode API.
type JoyCodeProvider struct {
	name    string
	baseURL string
	apiKey  string
	client  *http.Client
	models  []string
}

// NewJoyCodeProvider creates a JoyCode provider instance.
func NewJoyCodeProvider(name, baseURL, apiKey string, models []string) *JoyCodeProvider {
	if baseURL == "" {
		baseURL = "http://joycode-api-saas.jd.com"
	}
	return &JoyCodeProvider{
		name:    name,
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{},
		models:  models,
	}
}

func (p *JoyCodeProvider) Name() string            { return p.name }
func (p *JoyCodeProvider) Type() core.ProviderType { return core.ProviderJoyCode }
func (p *JoyCodeProvider) ValidateConfig() error   { return nil }

func (p *JoyCodeProvider) RequestTypes() []core.RequestType {
	return []core.RequestType{
		core.RequestTypeChatCompletion,
		core.RequestTypeResponses,
		core.RequestTypeMessages,
	}
}

// HealthCheck probes upstream reachability.
func (p *JoyCodeProvider) HealthCheck(ctx context.Context) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "modelList")
	} else {
		endpoint = p.baseURL + "/api/saas/models/v2/modelList"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader("{}"))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("ptKey", p.apiKey)
	req.Header.Set("loginType", getLoginTypeForPtKey(p.apiKey))
	req.Header.Set("x-ms-client-request-id", uuid.NewString())
	req.Header.Set("client", "JoyCodeIDE")
	req.Header.Set("clientVersion", "3.8.61")

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

func (p *JoyCodeProvider) Invoke(gctx *core.GatewayContext) error {
	invoker, ok := core.GetRequestInvoker(p.Type(), gctx.RequestType)
	if !ok {
		return fmt.Errorf("unsupported request type: %s", gctx.RequestType)
	}
	return invoker.Invoke(gctx, p)
}

// isAnthropicModel checks whether the current model is an Anthropic (Claude) branch model
func isAnthropicModel(model string, gctx *core.GatewayContext) bool {
	lower := strings.ToLower(model)
	if strings.Contains(lower, "claude") {
		return true
	}
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.Metadata != nil {
		if val, exists := gctx.SelectedEndpoint.Metadata["adapter"]; exists && val == "anthropic" {
			return true
		}
	}
	return false
}

// ==========================================
// 1. ChatCompletion Invoker
// ==========================================
type joycodeChatInvoker struct{}

func (i *joycodeChatInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("expected *JoyCodeProvider, got %T", p)
	}

	model := gctx.Model
	if model == "" {
		model = "Kimi-K2.6"
	}

	if isAnthropicModel(model, gctx) {
		// Anthropic messages branch
		return jp.invokeAnthropic(gctx, model)
	}

	// Standard OpenAI completions branch
	return jp.invokeOpenAI(gctx, model)
}

// ==========================================
// 2. Responses Invoker
// ==========================================
type joycodeResponsesInvoker struct{}

func (i *joycodeResponsesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("expected *JoyCodeProvider, got %T", p)
	}

	// Protocol downgrade: convert responses to chatCompletions format
	newBody, err := translate.ResponsesRequestToChat(gctx.RawBody)
	if err != nil {
		return fmt.Errorf("translate responses to chat completion: %w", err)
	}
	gctx.RawBody = newBody

	model := gctx.Model
	if model == "" {
		model = "Kimi-K2.6"
	}

	if isAnthropicModel(model, gctx) {
		if err := jp.invokeAnthropic(gctx, model); err != nil {
			return err
		}
	} else {
		if err := jp.invokeOpenAI(gctx, model); err != nil {
			return err
		}
	}

	// Reverse-translate response body (OpenAI Chat -> Responses)
	if gctx.IsStream {
		return handleResponsesStream(gctx, gctx.UpstreamResponse)
	}
	if err := translateResponsesNonStreamResponse(gctx); err != nil {
		return fmt.Errorf("translate response: %w", err)
	}
	return nil
}

// ==========================================
// 3. Messages Invoker (native Anthropic Messages request)
// ==========================================
type joycodeMessagesInvoker struct{}

func (i *joycodeMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	jp, ok := p.(*JoyCodeProvider)
	if !ok {
		return fmt.Errorf("expected *JoyCodeProvider, got %T", p)
	}

	model := gctx.Model
	if model == "" {
		model = "claude-3-5-sonnet-v2"
	}

	// Native Anthropic messages passthrough
	return jp.doAnthropicMessagesDirect(gctx, model)
}

// ==========================================
// JoyCode core request handling logic
// ==========================================

// invokeOpenAI sends a request to the standard OpenAI /v2 endpoint and handles the response
func (p *JoyCodeProvider) invokeOpenAI(gctx *core.GatewayContext, model string) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "chat_completions")
	} else {
		endpoint = p.baseURL + "/api/saas/openai/v2/chat/completions"
	}

	reqBody := injectJoyCodePayload(gctx.RawBody)
	resp, err := upstream.Call(gctx, upstream.Request{
		Client: p.client,
		URL:    endpoint,
		Body:   reqBody,
		Header: joyCodeAuthHeaders(gctx, p.apiKey, false),
		Stream: upstream.Consume,
	})
	if err != nil {
		return err
	}

	if gctx.IsStream {
		defer resp.Body.Close()
		// Standard models use standard OpenAI stream parsing
		return handleOpenAIStream(gctx, resp)
	}

	// Non-streaming response handling
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	gctx.UpstreamBody = body
	gctx.TriggerFirstByte()

	// Token stats
	in, out, cached, cacheCreated := llm.OpenAITokenExtractor(string(body))
	llm.ApplyUsage(gctx, in, out, cached, cacheCreated)
	return nil
}

// invokeAnthropic converts OpenAI request format to Anthropic messages format, sends to upstream,
// and converts the response back to OpenAI format.
func (p *JoyCodeProvider) invokeAnthropic(gctx *core.GatewayContext, model string) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "anthropic_completions")
	} else {
		endpoint = p.baseURL + "/api/saas/anthropic/v1/messages"
	}

	// 1. OpenAI -> Anthropic request body translation
	anthropicBody, err := translate.ChatRequestToMessages(gctx.RawBody, model)
	if err != nil {
		return fmt.Errorf("translate openAI to anthropic request: %w", err)
	}
	anthropicBody = adaptThinkingBehavior(anthropicBody)
	anthropicBody = injectJoyCodePayload(anthropicBody)

	resp, err := upstream.Call(gctx, upstream.Request{
		Client: p.client,
		URL:    endpoint,
		Body:   anthropicBody,
		Header: joyCodeAuthHeaders(gctx, p.apiKey, true),
		Stream: upstream.Consume,
	})
	if err != nil {
		return err
	}

	if gctx.IsStream {
		// Intercept and translate streaming (Anthropic SSE -> OpenAI SSE).
		// handleAnthropicStreamToOpenAI closes resp.Body itself.
		return p.handleAnthropicStreamToOpenAI(gctx, resp)
	}

	// Non-streaming response translation
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	// 2. Anthropic -> OpenAI response translation (including tool_use -> tool_calls)
	res, err := translate.MessagesToChatCompletion(body, model)
	if err != nil {
		return fmt.Errorf("translate anthropic response: %w", err)
	}

	gctx.UpstreamBody = res.Body
	gctx.TriggerFirstByte()
	gctx.InputTokens = res.Usage.InputTokens
	gctx.OutputTokens = res.Usage.OutputTokens
	return nil
}

// doAnthropicMessagesDirect forwards native Anthropic /messages requests without format conversion; only injects headers.
func (p *JoyCodeProvider) doAnthropicMessagesDirect(gctx *core.GatewayContext, model string) error {
	var endpoint string
	if strings.HasPrefix(p.baseURL, "https://") {
		endpoint = signJoyCodeGatewayURL(p.baseURL, "anthropic_completions")
	} else {
		endpoint = p.baseURL + "/api/saas/anthropic/v1/messages"
	}

	adaptedBody := injectJoyCodePayload(adaptThinkingBehavior(gctx.RawBody))
	resp, err := upstream.Call(gctx, upstream.Request{
		Client: p.client,
		URL:    endpoint,
		Body:   adaptedBody,
		Header: joyCodeAuthHeaders(gctx, p.apiKey, true),
		Stream: upstream.Consume,
	})
	if err != nil {
		return err
	}

	if gctx.IsStream {
		// Use Anthropic SSE passthrough handler directly; handleMessagesStream closes resp.Body itself.
		return p.handleMessagesStream(gctx, resp)
	}

	// Non-streaming: read body directly
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	gctx.UpstreamBody = body
	gctx.TriggerFirstByte()

	// Token stats
	in, out, cached, cacheCreated := llm.AnthropicTokenExtractor(string(body))
	llm.ApplyUsage(gctx, in, out, cached, cacheCreated)
	return nil
}

// handleMessagesStream transparently passes through Anthropic SSE format streams
func (p *JoyCodeProvider) handleMessagesStream(gctx *core.GatewayContext, resp *http.Response) error {
	writer := llm.NewSSEInterceptWriter(gctx, llm.WithTokenExtractor(llm.AnthropicTokenExtractor))
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := writer.Write(buf[:n]); werr != nil {
				resp.Body.Close()
				return werr
			}
			writer.Flush()
		}
		if err != nil {
			resp.Body.Close()
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read upstream stream: %w", err)
		}
	}
	return nil
}

// ==========================================
// 4. OpenAI <-> Anthropic protocol translation functions
// ==========================================

// translateOpenAIToAnthropic converts OpenAI chat.completions request format to Anthropic messages format.
// Legacy test entry point: delegates to pkg/llm/translate
func translateOpenAIToAnthropic(rawBody []byte, model string) ([]byte, error) {
	return translate.ChatRequestToMessages(rawBody, model)
}

func translateAnthropicToOpenAINonStream(anthropicBody []byte, model string) ([]byte, error) {
	res, err := translate.MessagesToChatCompletion(anthropicBody, model)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// handleAnthropicStreamToOpenAI intercepts Anthropic streaming SSE, translates and reassembles into OpenAI SSE chunks in real time.
// Protocol translation is handled by translate.MessagesToChatStream; this function only does SSE framing and output.
func (p *JoyCodeProvider) handleAnthropicStreamToOpenAI(gctx *core.GatewayContext, resp *http.Response) error {
	gctx.ResponseWriter.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	gctx.ResponseWriter.Header().Set("Cache-Control", "no-cache")
	gctx.ResponseWriter.Header().Set("Connection", "keep-alive")
	gctx.ResponseWriter.WriteHeader(http.StatusOK)

	flusher, hasFlusher := gctx.ResponseWriter.(http.Flusher)
	defer resp.Body.Close()

	parser := llm.NewSSEParser()
	stream := translate.NewMessagesToChatStream(gctx.Model)
	buf := make([]byte, 4096)

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			events := parser.Feed(buf[:n])
			for _, ev := range events {
				gctx.TriggerFirstByte()

				if ev.Done {
					_, _ = fmt.Fprint(gctx.ResponseWriter, "data: [DONE]\n\n")
					if hasFlusher {
						flusher.Flush()
					}
					return nil
				}

				chunks, meta := stream.FeedJSON(ev.Data)
				if meta.InputTokens > 0 {
					gctx.InputTokens = meta.InputTokens
				}
				if meta.OutputTokens > 0 {
					gctx.OutputTokens = meta.OutputTokens
				}
				if meta.TransmittedChars > 0 {
					gctx.TransmittedChars += meta.TransmittedChars
				}

				for _, c := range chunks {
					_, _ = fmt.Fprint(gctx.ResponseWriter, translate.FormatSSEData(c))
					if hasFlusher {
						flusher.Flush()
					}
				}
				if meta.EmitDone {
					_, _ = fmt.Fprint(gctx.ResponseWriter, "data: [DONE]\n\n")
					if hasFlusher {
						flusher.Flush()
					}
					return nil
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read upstream stream: %w", err)
		}
	}
	return nil
}

// joyCodeAuthHeaders builds the JoyCode auth/config headers plus (for Anthropic protocol)
// anthropic-version/anthropic-beta. Endpoint.Headers and InjectedHeaders are NOT applied here —
// upstream.Call merges those, so applying them here too would be redundant.
func joyCodeAuthHeaders(gctx *core.GatewayContext, apiKey string, isAnthropic bool) http.Header {
	h := make(http.Header)
	h.Set("ptKey", apiKey)
	h.Set("loginType", getLoginTypeForPtKey(apiKey))
	h.Set("x-ms-client-request-id", uuid.NewString())
	h.Set("client", "JoyCodeIDE")
	h.Set("clientVersion", "3.8.61")
	h.Set("Content-Type", "application/json; charset=UTF-8")

	if isAnthropic {
		version := "2023-06-01"
		if gctx.Request != nil {
			if ver := gctx.Request.Header.Get("anthropic-version"); ver != "" {
				version = ver
			}
			if beta := gctx.Request.Header.Get("anthropic-beta"); beta != "" {
				h.Set("anthropic-beta", beta)
			}
		}
		h.Set("anthropic-version", version)
	}
	return h
}

func adaptThinkingBehavior(body []byte) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}

	thinking, exists := m["thinking"]
	if !exists {
		return body
	}

	tMap, ok := thinking.(map[string]interface{})
	if !ok {
		return body
	}

	tType, _ := tMap["type"].(string)
	if tType == "enabled" {
		tMap["type"] = "adaptive"
		delete(tMap, "budget_tokens")
		m["output_config"] = map[string]interface{}{
			"effort": "high",
		}
	}

	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// Compile-time interface assertion
var _ core.Provider = (*JoyCodeProvider)(nil)

func signJoyCodeGatewayURL(baseURL, functionID string) string {
	baseURL = strings.TrimSuffix(strings.TrimSpace(baseURL), "/")
	t := time.Now().UnixNano() / int64(time.Millisecond)
	stringToSign := fmt.Sprintf("joycode_ide&%s&%d", functionID, t)
	key := []byte("0691a3f0b37b4a85aeb63ad0fc7db3ed")

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(stringToSign))
	sign := hex.EncodeToString(mac.Sum(nil))

	return fmt.Sprintf("%s/api?appid=joycode_ide&functionId=%s&t=%d&sign=%s", baseURL, functionID, t, sign)
}

func getLoginTypeForPtKey(ptKey string) string {
	if strings.HasPrefix(ptKey, "BJ.") {
		return "ERP"
	}
	return "N_PIN_PC"
}

func injectJoyCodePayload(rawBody []byte) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(rawBody, &m); err != nil {
		return rawBody
	}
	m["client"] = "JoyCodeIDE"
	m["clientVersion"] = "3.8.61"

	if out, err := json.Marshal(m); err == nil {
		return out
	}
	return rawBody
}


