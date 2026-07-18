package tagging

import (
	"regexp"
	"strings"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// Variable pattern: ${prefix.key}
var placeholderRe = regexp.MustCompile(`\$\{(\w+)\.([^}]+)\}`)

// Interpolator replaces ${prefix.key} placeholders with values.
//
// Supported prefixes:
//
//	${header.X-Name}  → gctx.GetHeader("X-Name")
//	${query.param}    → gctx.GetQuery("param")
//	${cookie.name}    → gctx.GetCookie("name")
//	${system.model}   → gctx.Model
//	${system.user}    → gctx.UserID
//	${system.apikey}  → gctx.APIKey
//	${tag.xxx}        → gctx.Tags["xxx"]
type Interpolator struct{}

// Interpolate replaces placeholders from GatewayContext.
// Static strings without ${} return as-is (no regex).
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
