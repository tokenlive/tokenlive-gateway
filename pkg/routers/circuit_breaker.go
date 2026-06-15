package routers

import (
	"sort"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"

	"go.uber.org/zap"
)

// CircuitBreakerRouter 过滤处于熔断开启（Open）状态的 Endpoint。
// 通过 CircuitBreakerManager 查询本地内存中的熔断状态，实现实例级熔断。
// 检查两层熔断：
//   - 服务级（provider:model）：整个服务不可用
//   - 实例级（endpoint ID）：单个实例不可用
type CircuitBreakerRouter struct {
	cbManager    *core.CircuitBreakerManager
	enableActive bool
	logger       *zap.Logger
}

// NewCircuitBreakerRouter 创建 CircuitBreakerRouter。
func NewCircuitBreakerRouter(cbManager *core.CircuitBreakerManager, enableActive bool, logger *zap.Logger) *CircuitBreakerRouter {
	return &CircuitBreakerRouter{cbManager: cbManager, enableActive: enableActive, logger: logger}
}

func (r *CircuitBreakerRouter) Name() string { return "circuit_breaker" }

func (r *CircuitBreakerRouter) Route(gctx *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	if len(endpoints) == 0 {
		return endpoints
	}

	// 预先建立熔断器 key 到 model 的关联关系，以便状态变更发射指标时能带上 model 标签
	for _, ep := range endpoints {
		serviceKey := ep.Provider + ":" + ep.Model
		r.cbManager.GetEntryWithModel(serviceKey, ep.Model)
		r.cbManager.GetEntryWithModel(ep.ID, ep.Model)
	}

	if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
		for _, p := range gctx.Policy.CircuitBreakPolicies {
			if p == nil {
				continue
			}
			level := strings.ToUpper(p.Level)
			if level == "" || level == "SERVICE" {
				for _, ep := range endpoints {
					serviceKey := ep.Provider + ":" + ep.Model
					r.cbManager.CheckAndResetOnVersionChange(serviceKey, p.Version)
				}
			}
			if level == "" || level == "INSTANCE" || level == "ENDPOINT" {
				for _, ep := range endpoints {
					r.cbManager.CheckAndResetOnVersionChange(ep.ID, p.Version)
				}
			}
		}
	}

	// 1. 第一阶段：独立过滤掉服务级熔断已触发的端点 (避免一票否决全部服务)
	var servicePassed []*core.Endpoint
	for _, ep := range endpoints {
		serviceKey := ep.Provider + ":" + ep.Model
		if !r.cbManager.AllowRequest(serviceKey, r.enableActive) {
			r.logger.Warn("circuit breaker: service not allowed, skipping",
				zap.String("key", serviceKey))
			continue
		}
		servicePassed = append(servicePassed, ep)
	}

	if len(servicePassed) == 0 {
		return nil
	}

	// 2. 第二阶段：按服务（Provider:Model）分组，对实例级熔断做比例限制
	groups := make(map[string][]*core.Endpoint)
	for _, ep := range servicePassed {
		key := ep.Provider + ":" + ep.Model
		groups[key] = append(groups[key], ep)
	}

	// 查找当前请求关联的实例级策略中的最小非零 OutlierMaxPercent
	var maxPercent int = 0
	hasInstancePolicy := false
	if gctx.Policy != nil && len(gctx.Policy.CircuitBreakPolicies) > 0 {
		for _, p := range gctx.Policy.CircuitBreakPolicies {
			if p == nil {
				continue
			}
			level := strings.ToUpper(p.Level)
			if level == "" || level == "INSTANCE" || level == "ENDPOINT" {
				hasInstancePolicy = true
				if p.OutlierMaxPercent > 0 {
					if maxPercent == 0 || p.OutlierMaxPercent < maxPercent {
						maxPercent = p.OutlierMaxPercent
					}
				}
			}
		}
	}

	blockedIDs := make(map[string]bool)

	for _, groupEPs := range groups {
		var groupBlocked []*core.Endpoint
		for _, ep := range groupEPs {
			if !r.cbManager.AllowRequest(ep.ID, r.enableActive) {
				groupBlocked = append(groupBlocked, ep)
			}
		}

		if len(groupBlocked) == 0 {
			continue
		}

		// 如果有实例策略并且设定了有效的 outlier_max_percent
		if hasInstancePolicy && maxPercent > 0 {
			totalInGroup := len(groupEPs)
			maxAllowed := totalInGroup * maxPercent / 100
			if maxAllowed == 0 && totalInGroup > 1 {
				maxAllowed = 1
			}

			if len(groupBlocked) > maxAllowed {
				// 按熔断打开时间升序排序（先熔断的优先排除），时间相同则按 ID 字典序
				sort.Slice(groupBlocked, func(i, j int) bool {
					ti := r.cbManager.GetOpenSince(groupBlocked[i].ID)
					tj := r.cbManager.GetOpenSince(groupBlocked[j].ID)
					if ti.Equal(tj) {
						return groupBlocked[i].ID < groupBlocked[j].ID
					}
					return ti.Before(tj)
				})

				// 仅前 maxAllowed 个节点继续熔断（先熔断的），其余放通（后熔断的）
				for i := 0; i < maxAllowed; i++ {
					blockedIDs[groupBlocked[i].ID] = true
				}
				r.logger.Warn("circuit breaker: instance percent limit hit",
					zap.Int("total", totalInGroup),
					zap.Int("blocked", len(groupBlocked)),
					zap.Int("maxAllowed", maxAllowed),
					zap.Int("finalBlocked", maxAllowed),
					zap.Strings("keptBlocked", func() []string {
						var ids []string
						for i := 0; i < maxAllowed; i++ {
							ids = append(ids, groupBlocked[i].ID)
						}
						return ids
					}()),
					zap.Strings("released", func() []string {
						var ids []string
						for i := maxAllowed; i < len(groupBlocked); i++ {
							ids = append(ids, groupBlocked[i].ID)
						}
						return ids
					}()))
			} else {
				// 未超限，正常熔断
				for _, ep := range groupBlocked {
					blockedIDs[ep.ID] = true
				}
			}
		} else {
			// 无实例策略或无比例限制，按老逻辑默认全熔断
			for _, ep := range groupBlocked {
				blockedIDs[ep.ID] = true
			}
		}
	}

	// 3. 第三阶段：过滤并组装最终结果
	var result []*core.Endpoint
	for _, ep := range servicePassed {
		if !blockedIDs[ep.ID] {
			result = append(result, ep)
		} else {
			r.logger.Warn("circuit breaker: instance not allowed, skipping",
				zap.String("endpoint", ep.ID))
		}
	}
	return result
}
