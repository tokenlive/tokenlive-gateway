package tagging

import (
	"regexp"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// 变量插值模式：${prefix.key}
var placeholderRe = regexp.MustCompile(`\$\{(\w+)\.([^}]+)\}`)

// Interpolator 变量插值引擎，将模板字符串中的 ${prefix.key} 替换为实际值
//
// 支持的变量前缀：
//
//	${header.X-Name}  → gctx.GetHeader("X-Name")
//	${query.param}    → gctx.GetQuery("param")
//	${cookie.name}    → gctx.GetCookie("name")
//	${system.model}   → gctx.Model
//	${system.user}    → gctx.UserID
//	${system.apikey}  → gctx.APIKey
//	${tag.xxx}        → gctx.Tags["xxx"]
type Interpolator struct{}

// Interpolate 将模板字符串中的变量占位符替换为 GatewayContext 中的实际值
// 对于不含 ${} 的纯静态字符串，直接返回原值，零正则开销
func (i *Interpolator) Interpolate(gctx *core.GatewayContext, template string) string {
	if !strings.Contains(template, "${") {
		return template
	}
	return placeholderRe.ReplaceAllStringFunc(template, func(match string) string {
		groups := placeholderRe.FindStringSubmatch(match)
		if len(groups) < 3 {
			return match
		}
		prefix, key := groups[1], groups[2]
		return i.resolve(gctx, prefix, key)
	})
}

func (i *Interpolator) resolve(gctx *core.GatewayContext, prefix, key string) string {
	switch prefix {
	case "header":
		vals := gctx.GetHeader(key)
		if len(vals) > 0 {
			return vals[0]
		}
	case "query":
		vals := gctx.GetQuery(key)
		if len(vals) > 0 {
			return vals[0]
		}
	case "cookie":
		return gctx.GetCookie(key)
	case "system":
		return gctx.GetSystemValue(key)
	case "tag":
		return gctx.GetTagValue(key)
	}
	return ""
}
