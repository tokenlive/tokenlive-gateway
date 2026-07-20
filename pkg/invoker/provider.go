package invoker

import (
	"encoding/json"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// ProviderInvoker is a leaf invoker wrapping a Provider + Endpoint.
type ProviderInvoker struct {
	Provider core.Provider
	endpoint *core.Endpoint
}

// NewProviderInvoker constructs a ProviderInvoker.
func NewProviderInvoker(provider core.Provider, endpoint *core.Endpoint) *ProviderInvoker {
	return &ProviderInvoker{
		Provider: provider,
		endpoint: endpoint,
	}
}

// Invoke performs the upstream provider call.
func (pi *ProviderInvoker) Invoke(gctx *core.GatewayContext) error {
	gctx.SelectedInvoker = pi
	gctx.SelectedEndpoint = pi.endpoint
	gctx.UpstreamConnect = time.Now()

	if pi.endpoint != nil {
		gctx.Logger(zap.NewNop()).Info("invoking provider endpoint",
			zap.String("endpoint_id", pi.endpoint.ID),
			zap.String("provider", pi.endpoint.Provider),
			zap.String("url", pi.endpoint.URL),
		)
	}

	if pi.Provider == nil {
		return nil
	}

	// Rewrite model to upstream name when configured.
	originalModel := gctx.Model
	var originalRawBody []byte
	if pi.endpoint != nil {
		effectiveModel := pi.endpoint.EffectiveModel()
		if effectiveModel != "" && (effectiveModel != originalModel || (gctx.OriginalModel != "" && effectiveModel != gctx.OriginalModel)) {
			gctx.Model = effectiveModel
			originalRawBody = gctx.RawBody
			gctx.RawBody = replaceModelInBody(gctx.RawBody, effectiveModel)
		}
	}
	defer func() {
		gctx.Model = originalModel
		if originalRawBody != nil {
			gctx.RawBody = originalRawBody
		}
	}()

	return pi.Provider.Invoke(gctx)
}

func (pi *ProviderInvoker) Endpoint() *core.Endpoint {
	return pi.endpoint
}

// replaceModelInBody sets the "model" field in a JSON request body.
func replaceModelInBody(body []byte, newModel string) []byte {
	var m map[string]interface{}
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	m["model"] = newModel
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}
