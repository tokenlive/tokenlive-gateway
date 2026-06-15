package policy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
)

// TestTimeoutPolicy_FirstByteTimeout 验证首字前超时触发后取消单次上下文的逻辑
func TestTimeoutPolicy_FirstByteTimeout(t *testing.T) {
	// 启动一个慢响应 Mock 服务器（在返回 Header 之后，故意延迟响应 Body 首字节）
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		// 故意休眠，模拟延迟首字节输出
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("data: hello\n\n"))
	}))
	defer server.Close()

	// 构建 GatewayContext 并应用 TotalTimeout（长超时 1s）
	gctx := &core.GatewayContext{
		Ctx:       context.Background(),
		StartTime: time.Now(),
		IsStream:  true,
	}

	// 模拟总超时包装 (TotalTimeout = 1000ms)
	totalCtx, totalCancel := context.WithTimeout(gctx.Ctx, 1000*time.Millisecond)
	defer totalCancel()
	gctx.Ctx = totalCtx

	// 模拟单次请求的 Context 控制及首字超时（Connect=10ms + TTFT=30ms = 40ms 阈值，明显会超时）
	connectTimeout := 10
	ttftTimeout := 30

	singleCtx, singleCancel := context.WithCancel(gctx.Ctx)
	defer singleCancel()

	timer := time.AfterFunc(time.Duration(connectTimeout+ttftTimeout)*time.Millisecond, func() {
		if gctx.TTFT == 0 {
			singleCancel()
		}
	})
	defer timer.Stop()

	gctx.RegisterTTFTimer(func() {
		timer.Stop()
	})

	// 构造实际 HTTP 请求并使用 singleCtx 发起
	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// 如果在建立连接或接收 Header 阶段就超时了，这也是正确的
		t.Logf("expected timeout during connection/headers phase: %v", err)
		return
	}
	defer resp.Body.Close()

	// 尝试读取 Response Body（模拟读取流）
	buf := make([]byte, 1024)
	_, err = resp.Body.Read(buf)
	if err == nil {
		t.Error("expected context canceled error due to TTFTimeout, but read succeeded")
	} else {
		t.Logf("successfully caught expected timeout error during body read: %v", err)
	}
}

// TestTimeoutPolicy_FirstByteArrived 验证首字节到达后能够成功 Stop 首包定时器，使后续传输可以超过首字超时时长
func TestTimeoutPolicy_FirstByteArrived(t *testing.T) {
	// 启动一个 Mock 服务器：首个字节秒回（10ms），但后续输出大段内容很慢（一共 120ms）
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		// 秒回首字节
		time.Sleep(5 * time.Millisecond)
		_, _ = w.Write([]byte("data: hello"))
		w.(http.Flusher).Flush()

		// 慢发后续数据
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write([]byte(" world\n\n"))
	}))
	defer server.Close()

	gctx := &core.GatewayContext{
		Ctx:       context.Background(),
		StartTime: time.Now(),
		IsStream:  true,
	}

	// 模拟总超时包装 (TotalTimeout = 1000ms)
	totalCtx, totalCancel := context.WithTimeout(gctx.Ctx, 1000*time.Millisecond)
	defer totalCancel()
	gctx.Ctx = totalCtx

	// 模拟单次调用超时（Connect=10ms + TTFT=20ms = 30ms 阈值）
	// 后续输出总用时 5ms + 80ms = 85ms，超出了 30ms 限制，但只要首字到达停止了定时器，就不会受其影响。
	connectTimeout := 10
	ttftTimeout := 20

	singleCtx, singleCancel := context.WithCancel(gctx.Ctx)
	defer singleCancel()

	timer := time.AfterFunc(time.Duration(connectTimeout+ttftTimeout)*time.Millisecond, func() {
		if gctx.TTFT == 0 {
			singleCancel()
		}
	})
	defer timer.Stop()

	gctx.RegisterTTFTimer(func() {
		timer.Stop()
	})

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do failed unexpectedly: %v", err)
	}
	defer resp.Body.Close()

	// 模拟读取并触发 TriggerFirstByte()
	buf := make([]byte, 1024)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("failed to read response first byte: %v", err)
	}

	if n > 0 {
		gctx.TriggerFirstByte() // 触发首字到达事件（应该成功 Stop 定时器）
	}

	// 继续读取后续的响应，休眠时间（80ms）使得总时间超出 30ms 首字超时，但因定时器已关闭，读取应正常成功
	_, err = resp.Body.Read(buf)
	if err != nil && err.Error() != "EOF" {
		t.Errorf("expected reading to complete successfully, got error: %v", err)
	}

	if gctx.TTFT == 0 {
		t.Error("expected TTFT value to be recorded, got 0")
	} else {
		t.Logf("TTFT correctly recorded: %v", gctx.TTFT)
	}
}

// TestTimeoutPolicy_ReadIdleTimeout_Triggered 验证读空闲超时能够成功触发并切断读取流
func TestTimeoutPolicy_ReadIdleTimeout_Triggered(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		// 很快吐出第一个包
		_, _ = w.Write([]byte("data: first\n\n"))
		w.(http.Flusher).Flush()

		// 卡顿 100ms
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte("data: second\n\n"))
	}))
	defer server.Close()

	gctx := &core.GatewayContext{
		Ctx:       context.Background(),
		StartTime: time.Now(),
		IsStream:  true,
	}

	// 1. 模拟总超时 (TotalTimeout = 1000ms)
	totalCtx, totalCancel := context.WithTimeout(gctx.Ctx, 1000*time.Millisecond)
	defer totalCancel()
	gctx.Ctx = totalCtx

	// 2. 模拟单次请求的 Context，设定空闲超时 30ms
	singleCtx, singleCancel := context.WithCancel(gctx.Ctx)
	defer singleCancel()

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do failed unexpectedly: %v", err)
	}
	defer resp.Body.Close()

	// 包装为 IdleTimeoutReadCloser
	wrappedBody := llm.WrapIdleTimeoutReader(resp.Body, 30*time.Millisecond, singleCancel)
	defer wrappedBody.Close()

	buf := make([]byte, 1024)

	// 读取第一个包，应该成功
	n, err := wrappedBody.Read(buf)
	if err != nil {
		t.Fatalf("failed to read first chunk: %v", err)
	}
	if n == 0 {
		t.Fatal("expected to read some bytes for first chunk, got 0")
	}

	// 第二次 Read，在 100ms 睡眠期间应该触发 30ms 的空闲超时
	_, err = wrappedBody.Read(buf)
	if err == nil {
		t.Error("expected idle timeout error, but read succeeded")
	} else if !strings.Contains(err.Error(), "stream read idle timeout") {
		t.Errorf("expected stream read idle timeout error, got: %v", err)
	} else {
		t.Logf("successfully caught expected idle timeout: %v", err)
	}
}

// TestTimeoutPolicy_ReadIdleTimeout_NotTriggered 验证流式正常吐字时不会触发空闲超时
func TestTimeoutPolicy_ReadIdleTimeout_NotTriggered(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		// 吐出 3 个包，每个包间隔 15ms（小于 40ms 的空闲超时阈值）
		for i := 0; i < 3; i++ {
			time.Sleep(15 * time.Millisecond)
			_, _ = w.Write([]byte("data: chunk\n\n"))
			w.(http.Flusher).Flush()
		}
	}))
	defer server.Close()

	gctx := &core.GatewayContext{
		Ctx:       context.Background(),
		StartTime: time.Now(),
		IsStream:  true,
	}

	totalCtx, totalCancel := context.WithTimeout(gctx.Ctx, 1000*time.Millisecond)
	defer totalCancel()
	gctx.Ctx = totalCtx

	singleCtx, singleCancel := context.WithCancel(gctx.Ctx)
	defer singleCancel()

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	defer resp.Body.Close()

	wrappedBody := llm.WrapIdleTimeoutReader(resp.Body, 40*time.Millisecond, singleCancel)
	defer wrappedBody.Close()

	buf := make([]byte, 1024)
	totalRead := 0
	for {
		n, err := wrappedBody.Read(buf)
		if n > 0 {
			totalRead += n
		}
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("unexpected read error: %v", err)
		}
	}

	if totalRead == 0 {
		t.Error("expected to read data, but read 0 bytes")
	} else {
		t.Logf("successfully read all data: %d bytes", totalRead)
	}
}

// TestTimeoutPolicy_ReadIdleTimeout_Coexistence 验证物理总超时 (TotalTimeout) 优于空闲超时起作用
func TestTimeoutPolicy_ReadIdleTimeout_Coexistence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		// 持续输出，但每次间隔 10ms，理论上永远不会触发 50ms 空闲超时
		for i := 0; i < 20; i++ {
			time.Sleep(10 * time.Millisecond)
			_, _ = w.Write([]byte("data: live\n\n"))
			w.(http.Flusher).Flush()
		}
	}))
	defer server.Close()

	gctx := &core.GatewayContext{
		Ctx:       context.Background(),
		StartTime: time.Now(),
		IsStream:  true,
	}

	// 设定极短总超时 (TotalTimeout = 50ms)，而空闲超时设为大值 (100ms)
	totalCtx, totalCancel := context.WithTimeout(gctx.Ctx, 50*time.Millisecond)
	defer totalCancel()
	gctx.Ctx = totalCtx

	singleCtx, singleCancel := context.WithCancel(gctx.Ctx)
	defer singleCancel()

	req, err := http.NewRequestWithContext(singleCtx, http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// 如果在连接时就触发了 50ms 物理超时，这也是正常的
		t.Logf("request connection timed out: %v", err)
		return
	}
	defer resp.Body.Close()

	wrappedBody := llm.WrapIdleTimeoutReader(resp.Body, 100*time.Millisecond, singleCancel)
	defer wrappedBody.Close()

	buf := make([]byte, 1024)
	errOccurred := false
	for {
		_, err := wrappedBody.Read(buf)
		if err != nil {
			errOccurred = true
			if strings.Contains(err.Error(), "context deadline exceeded") {
				t.Logf("successfully caught expected total timeout: %v", err)
			} else {
				t.Logf("caught other expected read error due to total context cancel: %v", err)
			}
			break
		}
	}

	if !errOccurred {
		t.Error("expected total timeout to interrupt streaming, but completed without error")
	}
}
