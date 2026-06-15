package llm

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// IdleTimeoutReadCloser 包装一个 io.ReadCloser，提供读空闲超时监测功能。
// 如果在指定的 idleTimeout 时间内没有读取到任何数据，它会调用 cancel() 触发底层连接取消，
// 并在 Read 返回时返回一个描述空闲超时的自定义错误。
type IdleTimeoutReadCloser struct {
	io.ReadCloser
	idleTimeout time.Duration
	timer       *time.Timer
	cancel      context.CancelFunc
	isTimedOut  atomic.Bool
}

// WrapIdleTimeoutReader 将一个已有的 io.ReadCloser 包装为具备读空闲超时监控能力的 ReadCloser。
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

// Read 重写了 Read 方法以管理空闲计时器。
func (r *IdleTimeoutReadCloser) Read(p []byte) (n int, err error) {
	// 如果已经因为空闲超时而被标记，则直接返回超时错误
	if r.isTimedOut.Load() {
		return 0, fmt.Errorf("stream read idle timeout after %v", r.idleTimeout)
	}

	// 惰性初始化定时器，或者在每次发起 Read 阻塞前 Reset 定时器
	if r.timer == nil {
		r.timer = time.AfterFunc(r.idleTimeout, func() {
			r.isTimedOut.Store(true)
			r.cancel()
		})
	} else {
		r.timer.Reset(r.idleTimeout)
	}

	// 发起阻塞的底层 Read 调用
	n, err = r.ReadCloser.Read(p)

	// 如果读取成功获得了数据，并且还未超时，Reset 计时器，给下一次 Read 留出完整的超时时间
	if n > 0 && !r.isTimedOut.Load() {
		r.timer.Reset(r.idleTimeout)
	}

	// 如果发生了错误，且是由定时器触发导致的 Context 取消，拦截并转换为自定义空闲超时错误
	if err != nil && r.isTimedOut.Load() {
		return n, fmt.Errorf("stream read idle timeout after %v", r.idleTimeout)
	}

	return n, err
}

// Close 重写 Close 以便正确停止并释放定时器。
func (r *IdleTimeoutReadCloser) Close() error {
	if r.timer != nil {
		r.timer.Stop()
	}
	return r.ReadCloser.Close()
}
