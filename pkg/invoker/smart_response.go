package invoker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// Private response writer: internal calls never write to the external client.
type smartResponseBuffer struct {
	header http.Header
	body   bytes.Buffer
	limit  int64
}

func newSmartResponseBuffer() *smartResponseBuffer {
	return &smartResponseBuffer{header: make(http.Header)}
}
func (b *smartResponseBuffer) Header() http.Header { return b.header }
func (b *smartResponseBuffer) WriteHeader(int)     {}
func (b *smartResponseBuffer) Write(p []byte) (int, error) {
	if b.limit > 0 && int64(b.body.Len())+int64(len(p)) > b.limit {
		return 0, fmt.Errorf("internal response exceeds byte limit")
	}
	return b.body.Write(p)
}

func smartResponseBytes(g *core.GatewayContext) ([]byte, error) {
	if len(g.UpstreamBody) > 0 {
		return g.UpstreamBody, nil
	}
	if g.Response != nil {
		return json.Marshal(g.Response)
	}
	if buffer, ok := g.ResponseWriter.(*smartResponseBuffer); ok {
		return append([]byte(nil), buffer.body.Bytes()...), nil
	}
	return nil, nil
}
