package limiter

import (
	"math"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// Record only a fully successful policy; failed executors already roll back.
func recordReservations(gctx *core.GatewayContext, lp *policy.LimitPolicy, key string, estimate int64) {
	if !gctx.TrackLimitReservations {
		return
	}
	for _, sw := range lp.SlidingWindows {
		window := time.Duration(sw.TimeWindowInMs) * time.Millisecond
		if window <= 0 {
			window = time.Minute
		}
		record := core.LimitReservation{
			Key: key + ":" + window.String(), Type: lp.Type,
			Estimated: estimate, Window: window, Rate: sw.Threshold,
		}
		if sw.BurstRatio != nil && *sw.BurstRatio > 0 {
			record.Burst = true
			record.Capacity = max(1, int64(math.Ceil(float64(sw.Threshold)**sw.BurstRatio)))
		}
		gctx.LimitReservations = append(gctx.LimitReservations, record)
	}
}
