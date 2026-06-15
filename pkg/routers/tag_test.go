package routers

import (
	"context"
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"

	"go.uber.org/zap"
)

func TestTagRouter_Route(t *testing.T) {
	logger := zap.NewNop()
	router := NewTagRouter(logger)

	// 准备候选端点
	epPremium := &core.Endpoint{
		ID:     "ep-premium",
		Weight: 100,
		Metadata: map[string]string{
			"endpoint_tier": "premium",
		},
	}
	epStandard := &core.Endpoint{
		ID:     "ep-standard",
		Weight: 100,
		Metadata: map[string]string{
			"endpoint_tier": "standard",
		},
	}
	endpoints := []*core.Endpoint{epPremium, epStandard}

	// 1. VIP 染色路由测试
	gctx := &core.GatewayContext{
		Ctx:  context.Background(),
		Tags: map[string]string{"priority": "high"},
		Policy: &policy.Policy{
			RoutePolicies: []*policy.RoutePolicy{
				{
					Name:  "vip-route",
					Order: 1,
					TagRules: []*policy.TagRule{
						{
							RelationType: "AND",
							Conditions: []*matcher.TagCondition{
								{
									Type:   "tag",
									OpType: "EQUAL",
									Key:    "priority",
									Values: []string{"high"},
								},
							},
							Destinations: []*policy.Destination{
								{
									Weight:       100,
									RelationType: "AND",
									Conditions: []*matcher.TagCondition{
										{
											OpType: "EQUAL",
											Key:    "endpoint_tier",
											Values: []string{"premium"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	res := router.Route(gctx, endpoints)
	if len(res) != 1 || res[0].ID != "ep-premium" {
		t.Errorf("expected routed to premium endpoint, got %+v", res)
	}

	// 2. 降级逃生测试：如果 premium 专线端点全部不可用，应退避逃生回默认端点
	resEscape := router.Route(gctx, []*core.Endpoint{epStandard})
	if len(resEscape) != 1 || resEscape[0].ID != "ep-standard" {
		t.Errorf("expected escape to standard endpoint, got %+v", resEscape)
	}
}
