package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestStatusCollectorFilter_OnResponse(t *testing.T) {
	// 1. 初始化 miniredis 和 redis client
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	f := NewStatusCollectorFilter(rdb, nil, "", "", nil)

	t.Run("Record success model and endpoints history", func(t *testing.T) {
		gctx := &core.GatewayContext{
			Ctx:          context.Background(),
			Request:      httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
			Model:        "gpt-4",
			Err:          nil,
			InputTokens:  10,
			OutputTokens: 20,
			Cost:         0.05,
			History: []core.AttemptRecord{
				{
					EndpointID: "ep-1",
					Success:    false, // 第一次尝试失败，会触发 failover
				},
				{
					EndpointID: "ep-2",
					Success:    true, // 第二次尝试成功
				},
			},
		}

		err := f.OnResponse(gctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// 等待异步协程执行完毕
		time.Sleep(100 * time.Millisecond)

		minute := time.Now().Unix() / 60
		dateStr := time.Now().Format("2006-01-02")

		getVal := func(k string) string {
			v, _ := mr.Get(k)
			return v
		}

		// 校验 Model 维度 (Success)
		modelKey := fmt.Sprintf("aigw:status:model:gpt-4:%d:s", minute)
		if val := getVal(modelKey); val != "1" {
			t.Errorf("expected model success count to be 1, got %q", val)
		}

		// 校验 Endpoint 1 (Fail)
		ep1Key := fmt.Sprintf("aigw:status:endpoint:ep-1:%d:f", minute)
		if val := getVal(ep1Key); val != "1" {
			t.Errorf("expected endpoint ep-1 fail count to be 1, got %q", val)
		}

		// 校验 Endpoint 2 (Success)
		ep2Key := fmt.Sprintf("aigw:status:endpoint:ep-2:%d:s", minute)
		if val := getVal(ep2Key); val != "1" {
			t.Errorf("expected endpoint ep-2 success count to be 1, got %q", val)
		}

		// 校验自然日统计项
		dailyReqKey := fmt.Sprintf("aigw:status:daily:req:%s", dateStr)
		if val := getVal(dailyReqKey); val != "1" {
			t.Errorf("expected daily req count to be 1, got %q", val)
		}

		dailyCostKey := fmt.Sprintf("aigw:status:daily:cost:%s", dateStr)
		if val := getVal(dailyCostKey); val != "0.05" {
			t.Errorf("expected daily cost to be 0.05, got %q", val)
		}
	})
}

func TestStatusCollectorFilter_ClientDisconnectDoesNotAffectAvailability(t *testing.T) {
	metricCh := make(chan RequestMetric, 1)
	f := &StatusCollectorFilter{metricCh: metricCh}
	gctx := &core.GatewayContext{
		Ctx:          context.Background(),
		Request:      httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
		Model:        "gpt-5.6-sol",
		Err:          fmt.Errorf("%w: context canceled", core.ErrClientDisconnected),
		InputTokens:  100,
		OutputTokens: 20,
		History: []core.AttemptRecord{
			{EndpointID: "ep-joycode", Success: true},
		},
	}

	if err := f.OnResponse(gctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case metric := <-metricCh:
		t.Fatalf("client disconnect must not emit availability metrics, got %+v", metric)
	default:
	}
}

func TestStatusCollectorFilter_OnResponseIncludesProviderInMemoryMetric(t *testing.T) {
	metricCh := make(chan RequestMetric, 1)
	f := &StatusCollectorFilter{metricCh: metricCh}
	gctx := &core.GatewayContext{
		Ctx:     context.Background(),
		Request: httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		Model:   "gpt-4",
		SelectedEndpoint: &core.Endpoint{
			ID:       "ep-1",
			Provider: "openai",
		},
	}

	if err := f.OnResponse(gctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case metric := <-metricCh:
		if metric.Provider != "openai" {
			t.Fatalf("expected provider openai, got %q", metric.Provider)
		}
	default:
		t.Fatal("expected a metric to be queued")
	}
}

func TestCollectEndpointPerfWriteAttributesOnlyWinningEndpoint(t *testing.T) {
	start := time.Now().Add(-2 * time.Second)
	gctx := &core.GatewayContext{
		Err:          nil,
		TTFT:         250 * time.Millisecond,
		OutputTokens: 40,
		StartTime:    start,
		SelectedEndpoint: &core.Endpoint{
			ID: "ep-2",
		},
		History: []core.AttemptRecord{
			{EndpointID: "ep-1", Success: false},
			{EndpointID: "ep-2", Success: true},
		},
	}

	perf := collectEndpointPerfWrite(gctx)
	if perf.EndpointID != "ep-2" {
		t.Fatalf("expected winning endpoint ep-2, got %q", perf.EndpointID)
	}
	if perf.TTFTMs != 250 {
		t.Fatalf("expected ttft 250ms, got %d", perf.TTFTMs)
	}
	if perf.Output != 40 {
		t.Fatalf("expected output tokens on winning endpoint only, got %d", perf.Output)
	}
	if perf.DurationMs <= 0 {
		t.Fatalf("expected positive duration, got %d", perf.DurationMs)
	}
}

func TestStatusCollectorFilter_OnResponse_DoesNotCreditFailedRetryWithTokens(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	f := NewStatusCollectorFilter(rdb, nil, "", "", nil)
	gctx := &core.GatewayContext{
		Ctx:          context.Background(),
		Request:      httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		Model:        "gpt-4",
		Err:          nil,
		TTFT:         180 * time.Millisecond,
		OutputTokens: 30,
		StartTime:    time.Now().Add(-1500 * time.Millisecond),
		SelectedEndpoint: &core.Endpoint{
			ID: "ep-2",
		},
		History: []core.AttemptRecord{
			{EndpointID: "ep-1", Success: false},
			{EndpointID: "ep-2", Success: true},
		},
	}

	if err := f.OnResponse(gctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	minute := time.Now().Unix() / 60
	if val, _ := mr.Get(fmt.Sprintf("aigw:status:endpoint:ep-1:%d:out", minute)); val != "" {
		t.Fatalf("failed retry must not receive output tokens, got %q", val)
	}
	if val, _ := mr.Get(fmt.Sprintf("aigw:status:endpoint:ep-1:%d:ttft_sum", minute)); val != "" {
		t.Fatalf("failed retry must not receive ttft, got %q", val)
	}
	if val, _ := mr.Get(fmt.Sprintf("aigw:status:endpoint:ep-2:%d:out", minute)); val != "30" {
		t.Fatalf("expected winning endpoint output 30, got %q", val)
	}
	if val, _ := mr.Get(fmt.Sprintf("aigw:status:endpoint:ep-2:%d:ttft_sum", minute)); val != "180" {
		t.Fatalf("expected winning endpoint ttft_sum 180, got %q", val)
	}
	if val, _ := mr.Get(fmt.Sprintf("aigw:status:endpoint:ep-2:%d:ttft_cnt", minute)); val != "1" {
		t.Fatalf("expected winning endpoint ttft_cnt 1, got %q", val)
	}
	if val, _ := mr.Get(fmt.Sprintf("aigw:status:model:gpt-4:%d:out", minute)); val != "30" {
		t.Fatalf("expected model output 30, got %q", val)
	}
	if val, _ := mr.Get(fmt.Sprintf("aigw:status:model:gpt-4:%d:ttft_sum", minute)); val != "180" {
		t.Fatalf("expected model ttft_sum 180, got %q", val)
	}
}

func TestRequestMetricJSONKeepsLegacyFieldsWithoutNewKeys(t *testing.T) {
	raw, err := json.Marshal(RequestMetric{
		Time:         1,
		Model:        "gpt-4",
		Success:      true,
		InputTokens:  10,
		OutputTokens: 20,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if payload["model"] != "gpt-4" {
		t.Fatalf("expected model gpt-4, got %#v", payload["model"])
	}
	if _, ok := payload["endpoint_id"]; ok {
		t.Fatalf("empty endpoint_id should be omitted, payload=%v", payload)
	}
	if _, ok := payload["ttft_ms"]; ok {
		t.Fatalf("zero ttft_ms should be omitted, payload=%v", payload)
	}

	legacy := []byte(`{"time":1,"model":"gpt-4","success":true,"input_tokens":10,"output_tokens":20}`)
	var decoded RequestMetric
	if err := json.Unmarshal(legacy, &decoded); err != nil {
		t.Fatalf("legacy payload must still decode: %v", err)
	}
	if decoded.Model != "gpt-4" || decoded.OutputTokens != 20 || decoded.EndpointID != "" {
		t.Fatalf("unexpected legacy decode: %+v", decoded)
	}
}

func TestStatusCollectorFilter_OnResponse_HTTP(t *testing.T) {
	var receivedPayload struct {
		Metrics       []RequestMetric `json:"metrics"`
		OpenEndpoints []string        `json:"open_endpoints"`
		OpenServices  []string        `json:"open_services"`
	}
	var receivedToken string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedToken = r.Header.Get("X-Sync-Token")
		if r.URL.Path == "/api/v1/gateway/metrics" && r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&receivedPayload)
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	logger := zap.NewNop()

	f := NewStatusCollectorFilter(nil, nil, ts.URL, "test-token", logger)
	defer f.cancel()

	gctx := &core.GatewayContext{
		Ctx:          context.Background(),
		Request:      httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		Model:        "gpt-4",
		Err:          nil,
		InputTokens:  10,
		OutputTokens: 20,
		Cost:         0.05,
		History: []core.AttemptRecord{
			{
				EndpointID: "ep-1",
				Success:    true,
			},
		},
	}

	err := f.OnResponse(gctx)
	if err != nil {
		t.Fatalf("OnResponse failed: %v", err)
	}

	f.flush([]RequestMetric{
		{
			Time:        time.Now().Unix(),
			Model:       gctx.Model,
			Success:     true,
			InputTokens: 10,
		},
	})

	if receivedToken != "test-token" {
		t.Errorf("expected token 'test-token', got %q", receivedToken)
	}
	if len(receivedPayload.Metrics) != 1 || receivedPayload.Metrics[0].Model != "gpt-4" {
		t.Errorf("unexpected metrics payload: %+v", receivedPayload)
	}
}
