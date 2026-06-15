package invoker

import (
	"encoding/json"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// ProviderInvoker 叶子调用器，封装一个 Provider + Endpoint
type ProviderInvoker struct {
	Provider core.Provider
	endpoint *core.Endpoint
}

// NewProviderInvoker 构造函数（便于包外实例化，但其实同包内也可以直接字面量初始化）
func NewProviderInvoker(provider core.Provider, endpoint *core.Endpoint) *ProviderInvoker {
	return &ProviderInvoker{
		Provider: provider,
		endpoint: endpoint,
	}
}

// Invoke 执行上游调用
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

	// 如果 Provider 为空（尚未实现），返回 nil
	if pi.Provider == nil {
		return nil
	}

	// 用 upstream model name 替换（如有）
	originalModel := gctx.Model
	var originalRawBody []byte
	if pi.endpoint != nil {
		effectiveModel := pi.endpoint.EffectiveModel()
		if effectiveModel != "" && effectiveModel != originalModel {
			gctx.Model = effectiveModel
			// 同步替换 RawBody 中的 model 字段
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

// replaceModelInBody 替换 JSON 请求体中的 model 字段
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
