package routers

import (
	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// PriorityRouter 优先级路由器。
// 必须在 CircuitBreakerRouter 之后执行。
// 作用：从传入的可用端点中，只选择 Priority 值最小（优先级最高）的端点集合。
// 如果存在多个相同最小优先级的端点，则共同作为候选，由下游 LoadBalancer 进行分流。
type PriorityRouter struct {
	logger *zap.Logger
}

// NewPriorityRouter 创建 PriorityRouter。
func NewPriorityRouter(logger *zap.Logger) *PriorityRouter {
	return &PriorityRouter{logger: logger}
}

// Name 返回路由器名称
func (r *PriorityRouter) Name() string { return "priority" }

// Route 过滤端点
func (r *PriorityRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	if len(endpoints) == 0 {
		return endpoints
	}

	// 找出剩余端点中最小的 Priority 字段值（值越小优先级越高）
	minPriority := endpoints[0].Priority
	for _, ep := range endpoints[1:] {
		if ep.Priority < minPriority {
			minPriority = ep.Priority
		}
	}

	// 筛选出所有 Priority 等于 minPriority 的端点
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if ep.Priority == minPriority {
			result = append(result, ep)
		} else {
			r.logger.Debug("priority router: skip endpoint due to lower priority",
				zap.String("endpoint_id", ep.ID),
				zap.Int("priority", ep.Priority),
				zap.Int("min_priority", minPriority))
		}
	}

	return result
}
