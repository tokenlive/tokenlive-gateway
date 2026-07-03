package lbs

import (
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/invoker"
)

// 最低延迟负载均衡器默认统计窗口。
const defaultLatencyWindow = 5 * time.Minute

// LeastLatencyLoadBalancer 最低延迟负载均衡器
type LeastLatencyLoadBalancer struct {
	stateStore core.StateStore
	window     time.Duration
}

// NewLeastLatencyLoadBalancer 创建最低延迟负载均衡器
func NewLeastLatencyLoadBalancer(ss core.StateStore) *LeastLatencyLoadBalancer {
	return &LeastLatencyLoadBalancer{
		stateStore: ss,
		window:     defaultLatencyWindow,
	}
}

// Select 选择平均延迟最低的端点。
// 配置项从 gctx.Policy.LoadBalancePolicy.Params 现读（与 SessionReaderFilter 一致）：
//   - latency_window: 统计窗口秒数（默认 300）
//   - latency_metric: "total"（整单耗时，默认）或 "ttft"（首包耗时，仅流式有意义）
func (lb *LeastLatencyLoadBalancer) Select(gctx *core.GatewayContext, endpoints []*core.Endpoint) core.Invoker {
	if len(endpoints) == 0 {
		return nil
	}

	window, metric := lb.resolveConfig(gctx)

	// 根据指标选择查询源：ttft 走独立序列，否则走整单耗时序列。
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
			// 查询失败时视为无限延迟，跳过
			continue
		}

		// 0 延迟（无采样数据）视为可选
		if minLatency < 0 || avgLatency < minLatency {
			minLatency = avgLatency
			selected = ep
		}
	}

	// 如果所有端点查询都失败，回退选择第一个
	if selected == nil {
		selected = endpoints[0]
	}

	return invoker.NewProviderInvoker(selected.ProviderImpl, selected)
}

// resolveConfig 从 Policy.Params 读取 latency_window 与 latency_metric，缺省回退到默认值。
func (lb *LeastLatencyLoadBalancer) resolveConfig(gctx *core.GatewayContext) (window time.Duration, metric string) {
	window = lb.window
	metric = "total"

	if gctx == nil || gctx.Policy == nil || gctx.Policy.LoadBalancePolicy == nil || gctx.Policy.LoadBalancePolicy.Params == nil {
		return
	}
	params := gctx.Policy.LoadBalancePolicy.Params

	// latency_window：支持数字（YAML 经 JSON 解析为 float64）或字符串
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

	// latency_metric：total（默认）或 ttft
	if v, ok := params["latency_metric"]; ok {
		if s, ok := v.(string); ok && (s == "ttft" || s == "total") {
			metric = s
		}
	}
	return
}
