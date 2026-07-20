package lbs

import (
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// Default stats window for least-latency LB.
const defaultLatencyWindow = 5 * time.Minute

// LeastLatencyLoadBalancer picks the lowest average latency endpoint.
type LeastLatencyLoadBalancer struct {
	stateStore core.StateStore
	window     time.Duration
}

// NewLeastLatencyLoadBalancer creates a least-latency LB.
func NewLeastLatencyLoadBalancer(ss core.StateStore) *LeastLatencyLoadBalancer {
	return &LeastLatencyLoadBalancer{
		stateStore: ss,
		window:     defaultLatencyWindow,
	}
}

// Select picks the endpoint with lowest average latency.
// Params from gctx.Policy.LoadBalancePolicy.Params (live read):
//   - latency_window: window seconds (default 300)
//   - latency_metric: "total" (full; default) or "ttft" (stream first-byte)
func (lb *LeastLatencyLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	window, metric := lb.resolveConfig(gctx)

	// Query source: ttft series vs full-latency series.
	queryAvg := func(epID string) (time.Duration, error) {
		if metric == "ttft" {
			return lb.stateStore.GetAvgTTFT(gctx.Ctx, epID, window)
		}
		return lb.stateStore.GetAvgLatency(gctx.Ctx, epID, window)
	}

	var selected *core.Endpoint
	var minLatency time.Duration = -1

	for _, ep := range endpoints {
		avgLatency, err := queryAvg(ep.ID)
		if err != nil {
			// Query failure: treat as infinite latency, skip.
			continue
		}

		// Zero latency (no samples) is still selectable.
		if minLatency < 0 || avgLatency < minLatency {
			minLatency = avgLatency
			selected = ep
		}
	}

	// If all queries fail, pick the first endpoint.
	if selected == nil {
		selected = endpoints[0]
	}

	return invoker.NewProviderInvoker(selected.ProviderImpl, selected)
}

// resolveConfig reads latency_window and latency_metric from Policy.Params.
func (lb *LeastLatencyLoadBalancer) resolveConfig(gctx *core.GatewayContext) (window time.Duration, metric string) {
	window = lb.window
	metric = "total"

	if gctx == nil || gctx.Policy == nil || gctx.Policy.LoadBalancePolicy == nil || gctx.Policy.LoadBalancePolicy.Params == nil {
		return
	}
	params := gctx.Policy.LoadBalancePolicy.Params

	// latency_window: number (float64 via YAML/JSON) or string
	if v, ok := params["latency_window"]; ok {
		switch x := v.(type) {
		case float64:
			if x > 0 {
				window = time.Duration(x) * time.Second
			}
		case int:
			if x > 0 {
				window = time.Duration(x) * time.Second
			}
		case string:
			if d, err := time.ParseDuration(x); err == nil && d > 0 {
				window = d
			}
		}
	}

	// latency_metric: total (default) or ttft
	if v, ok := params["latency_metric"]; ok {
		if s, ok := v.(string); ok && (s == "ttft" || s == "total") {
			metric = s
		}
	}
	return
}
