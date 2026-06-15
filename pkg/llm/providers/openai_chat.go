package providers

import (
	"fmt"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

type openaiChatInvoker struct{}

func (i *openaiChatInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}
	endpoint := op.baseURL + "/chat/completions"
	return op.doRequest(gctx, endpoint)
}
