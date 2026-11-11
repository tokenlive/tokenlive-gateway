package invoker

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/compositerouting"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
)

// Conservative first-release limits apply across the entire logical request.
const (
	compositeMaxModels           = 3
	compositeMaxUpstreamAttempts = 6
	compositeTotalTimeout        = 60 * time.Second
)

func (r *SmartRouter) invokeDeterministic(g *core.GatewayContext, pipe *core.Pipeline) error {
	if err := g.Ctx.Err(); err != nil {
		return err
	}
	in := compositerouting.Input{Tags: g.Tags, Headers: map[string]string{}}
	request := g.GovernanceRequest
	if request == nil {
		request = g.Request
	}
	if request != nil {
		for key, values := range request.Header {
			if compositerouting.AllowedHeader(key) {
				if len(values) != 1 {
					return &smartHTTPError{http.StatusBadRequest, "ambiguous business header"}
				}
				in.Headers[key] = values[0]
			}
		}
	}
	plan, err := compositerouting.Select(g.SmartConfig.PlanConfig(), in)
	if err != nil {
		return &smartHTTPError{http.StatusServiceUnavailable, "invalid composite routing configuration"}
	}
	g.SmartRouting.Mode, g.SmartRouting.MatchedRuleID = plan.Mode, plan.MatchedRuleID
	g.SmartRouting.MatchedModel, g.SmartRouting.Reason = plan.Target, plan.Reason
	req, err := parseSmartTextRequest(g.RequestType, g.RawBody, true)
	if err != nil {
		return err
	}
	var auth core.InboundFilter
	for _, filter := range pipe.InboundFilters {
		if filter.Name() == "auth" {
			auth = filter
			break
		}
	}
	accounting, err := r.childAccounting(pipe)
	if err != nil {
		return err
	}
	accounting.strictCapabilities = true
	models := []string{plan.Target}
	timeout := compositeTotalTimeout
	if g.Policy != nil && g.Policy.InvocationPolicy != nil {
		invocation := g.Policy.InvocationPolicy
		if invocation.FallbackPolicy != nil {
			models = append(models, invocation.FallbackPolicy.Targets...)
		}
		if invocation.RetryPolicy != nil && invocation.RetryPolicy.TotalTimeout > 0 {
			timeout = time.Duration(invocation.RetryPolicy.TotalTimeout) * time.Millisecond
		}
	}
	ctx, cancel := context.WithTimeout(g.Ctx, timeout)
	defer cancel()
	originalCtx := g.Ctx
	g.Ctx = core.WithCompositeAttemptBudget(ctx, compositeMaxUpstreamAttempts)
	defer func() { g.Ctx = originalCtx }()
	seen := map[string]bool{}
	lastErr := error(&smartHTTPError{http.StatusServiceUnavailable, "no eligible composite target"})
	modelAttempts := 0
	for _, model := range models {
		if err := g.Ctx.Err(); err != nil {
			return err
		}
		key := strings.ToLower(strings.TrimSpace(model))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		child, reason, err := r.newChild(g, model, g.RawBody, req, auth)
		if err != nil {
			g.SmartRouting.Escalations = append(g.SmartRouting.Escalations, core.SmartEscalation{Model: model, Reason: reason})
			lastErr = err
			if errors.Is(err, core.ErrFatalNoAvailableEndpoint) || !core.CanFallbackModel(g, core.ErrNoAvailableEndpoint) {
				return err
			}
			continue
		}
		if modelAttempts >= compositeMaxModels {
			core.ReleaseContext(child)
			return &smartHTTPError{http.StatusServiceUnavailable, "composite model attempt budget exhausted"}
		}
		modelAttempts++
		child.IsStream = req.Stream
		writer := &compositeStreamWriter{ResponseWriter: g.ResponseWriter}
		if req.Stream {
			child.ResponseWriter = writer
		}
		result := accounting.invoke(child, pipe.SelectInvoker(child))
		err = result.err()
		g.SmartRouting.ExecutedModel = model
		g.History = append(g.History, child.History...)
		g.AttemptCount += child.AttemptCount
		g.FallbackChain = append(g.FallbackChain, model)
		committed := req.Stream && writer.committed.Load()
		if req.Stream {
			g.TTFT = child.TTFT
			g.TransmittedChars = child.TransmittedChars
			for key, value := range child.Tags {
				g.Tags[key] = value
			}
			// Engine uses TTFT to protect already committed streams from JSON writes.
			if committed && g.TTFT == 0 {
				g.TTFT = time.Nanosecond
			}
		}
		if err == nil || committed {
			copySmartAnswer(g, child)
		}
		// Reuse existing conservative fallback semantics. New modes do not silently
		// turn generic HTTP errors into replayable model calls.
		canFallback := !committed && result.admissionErr == nil && result.settlementErr == nil && core.CanFallbackModel(g, err) && core.CanFallbackModel(child, err) && !errors.Is(err, core.ErrCompositeAttemptBudget)
		core.ReleaseContext(child)
		if err == nil {
			return nil
		}
		lastErr = err
		if !canFallback {
			return err
		}
		g.SmartRouting.Escalations = append(g.SmartRouting.Escalations, core.SmartEscalation{Model: model, Reason: "no_available_endpoint"})
	}
	return lastErr
}

// Track the actual downstream commitment, rather than relying on first-token
// timing. Flushing headers alone also makes transparent model switching unsafe.
type compositeStreamWriter struct {
	http.ResponseWriter
	committed atomic.Bool
}

func (w *compositeStreamWriter) WriteHeader(status int) {
	w.committed.Store(true)
	w.ResponseWriter.WriteHeader(status)
}
func (w *compositeStreamWriter) Write(p []byte) (int, error) {
	w.committed.Store(true)
	return w.ResponseWriter.Write(p)
}
func (w *compositeStreamWriter) Flush() {
	w.committed.Store(true)
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
func (w *compositeStreamWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
