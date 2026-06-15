package providers

import (
	"fmt"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

type openaiEmbeddingInvoker struct{}

func (i *openaiEmbeddingInvoker) Invoke(gctx *core.GatewayContext, p core.Provider) error {
	op, ok := p.(*OpenAIProvider)
	if !ok {
		return fmt.Errorf("expected *OpenAIProvider, got %T", p)
	}
	endpoint := op.baseURL + "/embeddings"
	return op.doRequest(gctx, endpoint)
}
