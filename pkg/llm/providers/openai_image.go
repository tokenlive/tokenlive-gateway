package providers

import (
	"fmt"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

type openaiImageGenerationInvoker struct{}

func (i *openaiImageGenerationInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}
	return op.doRequest(gctx, op.baseURL+"/images/generations")
}
