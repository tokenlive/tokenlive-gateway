package tagging

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/matcher"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

// TaggingEngine 染色打标规则引擎
// 按 Order 顺序执行 TaggingPolicy，命中条件时将标签注入 GatewayContext.Tags
type TaggingEngine struct {
	interpolator *Interpolator
}

// NewTaggingEngine 创建 TaggingEngine
func NewTaggingEngine() *TaggingEngine {
	return &TaggingEngine{interpolator: &Interpolator{}}
}

// Process 按 Order 顺序执行所有染色打标规则，基于 Action.Type 改写请求、响应、Cookie或请求体
func (e *TaggingEngine) Process(ctx context.Context, gctx *core.GatewayContext, taggingPolicies []*policy.TaggingPolicy) {
	if len(taggingPolicies) == 0 {
		return
	}
	if gctx.Tags == nil {
		gctx.Tags = make(map[string]string)
	}

	// 按 Order 排序（稳定排序，保持同 Order 的原始顺序）
	rules := make([]*policy.TaggingPolicy, len(taggingPolicies))
	copy(rules, taggingPolicies)
	sort.SliceStable(rules, func(i, j int) bool {
		return rules[i].Order < rules[j].Order
	})

	for _, rule := range rules {
		if e.matchConditions(ctx, gctx, rule) {
			for _, action := range rule.Actions {
				value := e.interpolator.Interpolate(gctx, action.Value)
				actionType := strings.ToUpper(action.Type)
				if actionType == "" {
					actionType = "TAG"
				}

				switch actionType {
				case "TAG":
					gctx.Tags[action.Key] = value

				case "REQ_HEADER":
					if gctx.InjectedHeaders == nil {
						gctx.InjectedHeaders = make(map[string]string)
					}
					gctx.InjectedHeaders[action.Key] = value
					if gctx.Request != nil {
						gctx.Request.Header.Set(action.Key, value)
					}

				case "RSP_HEADER":
					if gctx.ResponseWriter != nil {
						gctx.ResponseWriter.Header().Set(action.Key, value)
					}

				case "REQ_COOKIE":
					if gctx.Request != nil {
						cookies := gctx.Request.Cookies()
						found := false
						for _, cookie := range cookies {
							if cookie.Name == action.Key {
								cookie.Value = value
								found = true
								break
							}
						}
						if !found {
							gctx.Request.AddCookie(&http.Cookie{Name: action.Key, Value: value})
						} else {
							gctx.Request.Header.Del("Cookie")
							for _, cookie := range cookies {
								gctx.Request.AddCookie(cookie)
							}
						}
					}

				case "RSP_COOKIE":
					if gctx.ResponseWriter != nil {
						http.SetCookie(gctx.ResponseWriter, &http.Cookie{
							Name:  action.Key,
							Value: value,
							Path:  "/",
						})
					}

				case "REQ_BODY":
					if len(gctx.RawBody) > 0 {
						var bodyMap map[string]interface{}
						if err := json.Unmarshal(gctx.RawBody, &bodyMap); err == nil {
							var val interface{} = value
							if b, err := strconv.ParseBool(value); err == nil {
								val = b
							} else if i, err := strconv.ParseInt(value, 10, 64); err == nil {
								val = i
							} else if f, err := strconv.ParseFloat(value, 64); err == nil {
								val = f
							}
							bodyMap[action.Key] = val
							if newBody, err := json.Marshal(bodyMap); err == nil {
								gctx.RawBody = newBody
							}
						}
					}
				}
			}
		}
	}
}

// matchConditions 判断 TaggingPolicy 的所有条件是否满足
// Relation 默认为 AND（全部满足），可设为 OR（任一满足）
func (e *TaggingEngine) matchConditions(ctx context.Context, gctx *core.GatewayContext, rule *policy.TaggingPolicy) bool {
	var validConds []*matcher.TagCondition
	for _, cond := range rule.Conditions {
		if cond.Type != "" {
			validConds = append(validConds, cond)
		}
	}
	if len(validConds) == 0 {
		return true // 空条件默认命中
	}

	isOr := strings.EqualFold(rule.Relation, "OR")

	for _, cond := range validConds {
		m := matcher.DefaultTagMatcherFactory.Get(strings.ToLower(cond.Type))
		matched := m != nil && m.Match(ctx, cond, gctx)

		if isOr && matched {
			return true // OR 模式：任一命中即返回
		}
		if !isOr && !matched {
			return false // AND 模式：任一未命中即返回
		}
	}

	return !isOr // AND 全部命中 → true；OR 全部未命中 → false
}
