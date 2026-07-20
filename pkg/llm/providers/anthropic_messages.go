package providers

import (
	"fmt"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm/upstream"
)

type anthropicMessagesInvoker struct{}

func (i *anthropicMessagesInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	ap, ok := p.(*AnthropicProvider)
	if !ok {
		return fmt.Errorf("expected *AnthropicProvider, got %T", p)
	}

	mocked, err := llm.TryMockMessagesProbe(gctx)
	if err != nil {
		return err
	}
	if mocked {
		return nil
	}

	endpoint := ap.baseURL + "/messages"

	h := make(http.Header)
	h.Set("Content-Type", "application/json")

	isOAuth := false
	if gctx.SelectedEndpoint != nil && gctx.SelectedEndpoint.AuthType == "oauth_token" {
		isOAuth = true
	}

	h.Set("Authorization", "Bearer "+ap.apiKey)
	if !isOAuth {
		h.Set("x-api-key", ap.apiKey)
	}
	h.Set("anthropic-version", "2023-06-01")

	resp, err := upstream.Call(gctx, upstream.Request{
		Client: ap.client,
		URL:    endpoint,
		Body:   gctx.RawBody,
		Header: h,
		Stream: upstream.Consume,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if gctx.IsStream {
		return ap.handleMessagesStream(gctx, resp)
	}
	return ap.handleMessagesNonStream(gctx, resp)
}
