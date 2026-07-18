package llm

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// IdleTimeoutReadCloser wraps an io.ReadCloser with read idle-timeout detection.
// If no data arrives within idleTimeout, it calls cancel() to abort the underlying connection
// and returns a custom idle-timeout error from Read.
type IdleTimeoutReadCloser struct {
	io.ReadCloser
	idleTimeout time.Duration
	timer       *time.Timer
	cancel      context.CancelFunc
	isTimedOut  atomic.Bool
}

// WrapIdleTimeoutReader wraps an io.ReadCloser with read idle-timeout monitoring.
func WrapIdleTimeoutReader(body io.ReadCloser, idleTimeout time.Duration, cancel context.CancelFunc) io.ReadCloser {
	if idleTimeout <= 0 {
		return body
	}
	return &IdleTimeoutReadCloser{
		ReadCloser:  body,
		idleTimeout: idleTimeout,
		cancel:      cancel,
	}
}

// Read manages the idle timer to detect read stalls.
func (r *IdleTimeoutReadCloser) Read(p []byte) (n int, err error) {
	// Already timed out — return immediately
	if r.isTimedOut.Load() {
		return 0, fmt.Errorf("stream read idle timeout after %v", r.idleTimeout)
	}

	// Lazily init the timer, or reset before each blocking Read
	if r.timer == nil {
		r.timer = time.AfterFunc(r.idleTimeout, func() {
			r.isTimedOut.Store(true)
			r.cancel()
		})
	} else {
		r.timer.Reset(r.idleTimeout)
	}

	n, err = r.ReadCloser.Read(p)

	// On successful data read without timeout, reset timer for next Read
	if n > 0 && !r.isTimedOut.Load() {
		r.timer.Reset(r.idleTimeout)
	}

	// If the error was caused by timer-triggered context cancellation, intercept and convert to idle-timeout error
	if err != nil && r.isTimedOut.Load() {
		return n, fmt.Errorf("stream read idle timeout after %v", r.idleTimeout)
	}

	return n, err
}

// Close stops and releases the timer.
func (r *IdleTimeoutReadCloser) Close() error {
	if r.timer != nil {
		r.timer.Stop()
	}
	return r.ReadCloser.Close()
}
