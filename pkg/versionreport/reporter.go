package versionreport

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// Sender sends a single version report over the selected internal channel.
type Sender interface {
	Send(context.Context, Node) error
}

// Run reports immediately, then every 30 seconds until canceled. The caller
// runs it in a background goroutine so reporting never gates request handling.
func Run(ctx context.Context, sender Sender, node Node, after func(time.Duration) <-chan time.Time) {
	if sender == nil {
		return
	}
	if after == nil {
		after = time.After
	}
	var lastWarning time.Time
	for {
		if ctx.Err() != nil {
			return
		}
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := sender.Send(sendCtx, node)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil && (lastWarning.IsZero() || time.Since(lastWarning) >= time.Minute) {
			// Keep URLs, tokens and server response bodies out of background logs.
			zap.L().Warn("gateway version report failed; retrying on the next interval")
			lastWarning = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-after(30 * time.Second):
		}
	}
}
