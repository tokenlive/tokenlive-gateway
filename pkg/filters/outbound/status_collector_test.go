package outbound

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestStatusCollectorFilter_OnResponse(t *testing.T) {
	// 1. 初始化 miniredis 和 redis client
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer rdb.Close()

	f := NewStatusCollectorFilter(rdb)

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
