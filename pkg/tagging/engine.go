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

// TaggingEngine applies tagging policies in order.
// Runs TaggingPolicy by Order; injects tags into GatewayContext.Tags on match.
type TaggingEngine struct {
	interpolator *Interpolator
}

// NewTaggingEngine creates a TaggingEngine.
func NewTaggingEngine() *TaggingEngine {
	return &TaggingEngine{interpolator: &Interpolator{}}
}

// Process runs policies by Order; rewrites req/resp/cookie/body per Action.Type.
func (e *TaggingEngine) Process(ctx context.Context, gctx *core.GatewayContext, taggingPolicies []*policy.TaggingPolicy) {
	if len(taggingPolicies) == 0 {
		return
	}
	if gctx.Tags == nil {
		gctx.Tags = make(map[string]string)
	}

	// Stable sort by Order.
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

// matchConditions evaluates TaggingPolicy conditions.
// Relation defaults to AND; OR matches any condition.
func (e *TaggingEngine) matchConditions(ctx context.Context, gctx *core.GatewayContext, rule *policy.TaggingPolicy) bool {
	var validConds []*matcher.TagCondition
	for _, cond := range rule.Conditions {
		if cond.Type != "" {
			validConds = append(validConds, cond)
		}
	}
	if len(validConds) == 0 {
		return true // empty conditions match
	}

	isOr := strings.EqualFold(rule.Relation, "OR")

	for _, cond := range validConds {
		m := matcher.DefaultTagMatcherFactory.Get(strings.ToLower(cond.Type))
		matched := m != nil && m.Match(ctx, cond, gctx)

		if isOr && matched {
			return true // OR: any match
		}
		if !isOr && !matched {
			return false // AND: any miss
		}
	}

	return !isOr // AND all match → true; OR none → false
}
