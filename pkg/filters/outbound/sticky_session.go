package outbound

import (
	"context"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// StickySessionFilter saves the SessionID -> EndpointID mapping after a successful request.
type StickySessionFilter struct {
	stateStore core.StateStore
	ttl        time.Duration
}

// NewStickySessionFilter creates a StickySessionFilter.
func NewStickySessionFilter(ss core.StateStore, ttl time.Duration) *StickySessionFilter {
	return &StickySessionFilter{stateStore: ss, ttl: ttl}
}

func (f *StickySessionFilter) Name() string                        { return "sticky_session" }
func (f *StickySessionFilter) Order() int                          { return 20 }
func (f *StickySessionFilter) Criticality() core.FilterCriticality { return core.Critical }

func (f *StickySessionFilter) OnResponse(gctx *core.GatewayContext) error {
	if gctx.SessionID == "" || gctx.SelectedEndpoint == nil {
		return nil
	}
	return f.stateStore.StickySet(context.Background(), gctx.SessionID, gctx.SelectedEndpoint.ID, f.ttl)
}
