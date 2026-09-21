package invoker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/outbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
)

type SmartConfigSource interface {
	GetSmartRouting(context.Context, string) (*core.SmartRoutingConfig, error)
}

// SmartRouter orchestrates logical models; ClusterInvoker still owns endpoints.
type SmartRouter struct {
	source    SmartConfigSource
	validator inbound.ModelValidator
	policies  policy.PolicyProvider
	judge     core.Invoker
	limits    *inbound.RateLimitFilter
	settler   *outbound.TokenSettlementFilter
	discovery core.Discovery
}

type smartTerminalError struct{ cause error }

func (e *smartTerminalError) Error() string { return e.cause.Error() }
func (e *smartTerminalError) Unwrap() error { return e.cause }

func NewSmartRouter(source SmartConfigSource, validator inbound.ModelValidator, policies policy.PolicyProvider, resolver core.InvokerDependencyResolver) *SmartRouter {
	judge := NewClusterInvoker(resolver.Discovery(),
		resolver.ResolveRouters([]string{"capability", "circuit_breaker", "tenant_endpoint", "tag", "priority"}),
		map[string]core.LoadBalancer{"round_robin": resolver.ResolveLoadBalancer("round_robin")},
		&policy.RetryPolicy{}, resolver.CircuitBreakerManager(), resolver.StateStore(), resolver.Logger(), nil)
	judge.SetEnableActive(resolver.EnableActiveHealthCheck())
	return &SmartRouter{
		source: source, validator: validator, policies: policies, judge: judge,
		limits:    inbound.NewRateLimitFilter(resolver.StateStore()),
		settler:   outbound.NewTokenSettlementFilter(resolver.StateStore(), nil, resolver.Logger()),
		discovery: resolver.Discovery(),
	}
}

func (r *SmartRouter) Prepare(g *core.GatewayContext) error {
	cfg, err := r.source.GetSmartRouting(g.Ctx, g.Model)
	if err != nil {
		return &smartHTTPError{http.StatusServiceUnavailable, "smart routing configuration unavailable"}
	}
	if cfg == nil {
		return nil
	}
	cfg = cfg.Clone()
	cfg.ApplyDefaults()
	if err := cfg.Validate(g.Model); err != nil {
		return &smartHTTPError{http.StatusServiceUnavailable, "invalid smart routing configuration"}
	}
	if _, err := parseSmartRequest(g.RequestType, g.RawBody); err != nil {
		return err
	}
	g.SmartConfig = cfg
	g.SmartRouting = &core.SmartRoutingRecord{Model: g.Model, Version: cfg.Version, JudgeModel: cfg.JudgeModel, PromptVersion: smartPromptVersion}
	g.TrackLimitReservations = true
	g.Sensitive = true
	return nil
}

func (r *SmartRouter) Invoke(g *core.GatewayContext, pipe *core.Pipeline) (bool, error) {
	if g.SmartConfig == nil {
		return false, nil
	}
	defer func() { g.SmartRouting.DurationMs = time.Since(g.StartTime).Milliseconds() }()
	if err := g.Ctx.Err(); err != nil {
		return true, err
	}
	req, err := parseSmartRequest(g.RequestType, g.RawBody)
	if err != nil {
		return true, err
	}
	cfg, record := g.SmartConfig, g.SmartRouting
	var auth core.InboundFilter
	for _, filter := range pipe.InboundFilters {
		if filter.Name() == "auth" {
			auth = filter
			break
		}
	}
	judgeStart := time.Now()
	score, reason, usage, judgeErr := r.classify(g, req, auth)
	record.JudgeDurationMs = time.Since(judgeStart).Milliseconds()
	record.JudgeUsage = usage
	if err := g.Ctx.Err(); err != nil {
		return true, err
	}
	record.Reason = reason
	var terminal *smartTerminalError
	if errors.As(judgeErr, &terminal) {
		return true, terminal.cause
	}
	start := len(cfg.Ranges) - 1
	if judgeErr == nil {
		record.Score = &score
		start = smartRangeIndex(cfg.Ranges, score)
		if start < 0 {
			return true, &smartHTTPError{http.StatusServiceUnavailable, "invalid smart routing partition"}
		}
		record.MatchedModel = cfg.Ranges[start].Model
	}
	seen := make(map[string]bool)
	lastErr := error(&smartHTTPError{http.StatusServiceUnavailable, "no available smart routing target"})
	step := 1
	if judgeErr != nil {
		step = -1
	}
	for index := start; index >= 0 && index < len(cfg.Ranges); index += step {
		if err := g.Ctx.Err(); err != nil {
			return true, err
		}
		model := cfg.Ranges[index].Model
		if seen[model] {
			continue
		}
		seen[model] = true
		child, skipReason, err := r.newChild(g, model, g.RawBody, req, auth)
		if err != nil {
			record.Escalations = append(record.Escalations, core.SmartEscalation{Model: model, Reason: skipReason})
			lastErr = err
			continue
		}
		if record.MatchedModel == "" {
			record.MatchedModel = model
		}
		err = r.limits.OnRequest(child)
		if err != nil {
			child.Err = err
			settleErr := r.settler.SettleLimits(child)
			core.ReleaseContext(child)
			if settleErr != nil {
				return true, settleErr
			}
			return true, err // A hard local quota denial must not trigger escalation.
		}
		invoke := pipe.Invoker
		if child.Policy != nil && child.Policy.InvocationPolicy != nil {
			if configured := pipe.Invokers[child.Policy.InvocationPolicy.Type]; configured != nil {
				invoke = configured
			}
		}
		invoke, err = capacityCheckedInvoker(invoke)
		if err == nil {
			err = invoke.Invoke(child)
		}
		child.Err = err
		if settleErr := r.settler.SettleLimits(child); settleErr != nil {
			core.ReleaseContext(child)
			return true, settleErr
		}
		record.ExecutedModel = model
		g.History = append(g.History, child.History...)
		g.AttemptCount += child.AttemptCount
		g.FallbackChain = append(g.FallbackChain, model)
		if err == nil {
			copySmartAnswer(g, child)
			core.ReleaseContext(child)
			return true, nil
		}
		retryable := smartRetryable(child, err)
		core.ReleaseContext(child)
		record.Escalations = append(record.Escalations, core.SmartEscalation{Model: model, Reason: "target_unavailable"})
		lastErr = err
		// On judge failure we picked the highest eligible target; never downgrade after invoking it.
		if judgeErr != nil || !retryable {
			return true, err
		}
	}
	return true, lastErr
}

func (r *SmartRouter) classify(parent *core.GatewayContext, req *smartRequest, auth core.InboundFilter) (int, string, *core.SmartJudgeUsage, error) {
	cfg := parent.SmartConfig
	body, err := buildSmartJudgeRequest(req, cfg)
	if err != nil {
		return 0, "judge_input_limit", nil, err
	}
	judgeReq, err := parseSmartRequest(core.RequestTypeChatCompletion, body)
	if err != nil {
		return 0, "judge_invalid_request", nil, err
	}
	child, _, err := r.newChild(parent, cfg.JudgeModel, body, judgeReq, auth)
	if err != nil {
		return 0, "judge_unavailable", nil, err
	}
	defer core.ReleaseContext(child)
	ctx, cancel := context.WithTimeout(parent.Ctx, time.Duration(cfg.JudgeTimeoutMs)*time.Millisecond)
	defer cancel()
	child.Ctx = ctx
	child.Request = child.Request.WithContext(ctx)
	child.MaxResponseBytes = 64 * 1024
	if buffer, ok := child.ResponseWriter.(*smartResponseBuffer); ok {
		buffer.limit = child.MaxResponseBytes
	}
	child.Policy.InvocationPolicy = &policy.InvocationPolicy{Type: "cluster", RetryPolicy: &policy.RetryPolicy{Retry: 0, TotalTimeout: cfg.JudgeTimeoutMs}}
	limitErr := r.limits.OnRequest(child)
	err = limitErr
	if err == nil {
		var invoke core.Invoker
		invoke, err = capacityCheckedInvoker(r.judge)
		if err == nil {
			err = invoke.Invoke(child)
		}
	}
	child.Err = err
	if settleErr := r.settler.SettleLimits(child); settleErr != nil {
		return 0, "judge_quota_error", nil, &smartTerminalError{settleErr}
	}
	if limitErr != nil {
		return 0, "judge_limit_rejected", nil, &smartTerminalError{limitErr}
	}
	if err != nil {
		reason := "judge_unavailable"
		if ctx.Err() != nil {
			reason = "judge_timeout"
		}
		return 0, reason, nil, err
	}
	data, err := smartResponseBytes(child)
	if err != nil || len(data) > int(child.MaxResponseBytes) {
		return 0, "judge_invalid_response", nil, errors.New("invalid judge response")
	}
	var envelope struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			Input  int `json:"prompt_tokens"`
			Output int `json:"completion_tokens"`
			Cached struct {
				Tokens int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(data, &envelope) != nil || len(envelope.Choices) != 1 {
		return 0, "judge_invalid_response", nil, errors.New("invalid judge completion")
	}
	var usage *core.SmartJudgeUsage
	if envelope.Usage != nil {
		usage = &core.SmartJudgeUsage{InputTokens: envelope.Usage.Input, OutputTokens: envelope.Usage.Output, CachedTokens: envelope.Usage.Cached.Tokens}
	}
	if envelope.Choices[0].FinishReason == "length" {
		return 0, "judge_invalid_response", usage, errors.New("truncated judge completion")
	}
	score, reason, err := parseSmartScore([]byte(envelope.Choices[0].Message.Content))
	if err != nil {
		return 0, "judge_invalid_score", usage, err
	}
	return score, reason, usage, nil
}

func (r *SmartRouter) newChild(parent *core.GatewayContext, model string, body []byte, req *smartRequest, auth core.InboundFilter) (*core.GatewayContext, string, error) {
	tenant := parent.Tenant
	if tenant == "" {
		tenant = parent.UserTenant
	}
	valid, err := r.validator.ValidateModel(parent.Ctx, model, tenant, parent.UserID)
	if err != nil || !valid {
		return nil, "target_not_authorized", &smartHTTPError{http.StatusForbidden, "smart routing target unavailable or not authorized"}
	}
	nested, err := r.source.GetSmartRouting(parent.Ctx, model)
	if err != nil || nested != nil {
		return nil, "invalid_target", &smartHTTPError{http.StatusServiceUnavailable, "smart target must be an ordinary model"}
	}
	endpoints, err := r.discovery.List(parent.Ctx, model)
	if err != nil {
		return nil, "target_unavailable", &smartHTTPError{http.StatusServiceUnavailable, "smart routing target unavailable"}
	}
	compatible := false
	for _, ep := range endpoints {
		if smartEndpointCompatible(ep, req) {
			compatible = true
			break
		}
	}
	if !compatible {
		return nil, "target_incompatible", &smartHTTPError{http.StatusServiceUnavailable, "no compatible smart routing endpoint"}
	}
	p := &policy.Policy{}
	if r.policies != nil {
		p, err = r.policies.GetPolicy(parent.Ctx, parent.Tenant, parent.UserID, model)
		if err != nil || p == nil {
			return nil, "target_policy_unavailable", &smartHTTPError{http.StatusForbidden, "smart routing target policy unavailable"}
		}
	}
	// The policy service owns its snapshot; only mutate our shallow outer copy.
	ownedPolicy := *p
	if p.InvocationPolicy != nil {
		ip := *p.InvocationPolicy
		ip.FallbackPolicy = nil
		ownedPolicy.InvocationPolicy = &ip
	}
	request, err := http.NewRequestWithContext(parent.Ctx, http.MethodPost, "http://internal/v1/chat/completions", nil)
	if err != nil {
		return nil, "invalid_request", err
	}
	buffer := newSmartResponseBuffer()
	child := core.AcquireContext(buffer, request)
	governanceRequest := parent.GovernanceRequest
	if governanceRequest == nil {
		governanceRequest = parent.Request
	}
	if governanceRequest != nil {
		child.GovernanceRequest = governanceRequest.Clone(parent.Ctx)
		child.GovernanceRequest.Body = nil
	}
	child.Model, child.OriginalModel = model, model
	child.RequestType = core.RequestTypeChatCompletion
	child.RawBody = smartModelBody(body, model)
	child.Policy = &ownedPolicy
	child.APIKey, child.APIKeyID, child.APIKeyHash = parent.APIKey, parent.APIKeyID, parent.APIKeyHash
	child.Tenant, child.UserID, child.UserTenant = parent.Tenant, parent.UserID, parent.UserTenant
	child.WorkspaceID = parent.WorkspaceID
	child.SessionID = parent.SessionID
	child.Sensitive = true
	child.TrackLimitReservations = true
	child.LimitKeys = make(map[string]bool, len(parent.LimitKeys))
	for key, value := range parent.LimitKeys {
		child.LimitKeys[key] = value
	}
	for key, value := range parent.Tags {
		child.Tags[key] = value
	}
	if auth != nil {
		if err := auth.OnRequest(child); err != nil {
			core.ReleaseContext(child)
			return nil, "target_not_authorized", err
		}
	}
	return child, "", nil
}

func copySmartAnswer(parent, child *core.GatewayContext) {
	parent.SelectedInvoker, parent.SelectedEndpoint = child.SelectedInvoker, child.SelectedEndpoint
	parent.UpstreamConnect, parent.UpstreamResponse = child.UpstreamConnect, child.UpstreamResponse
	parent.UpstreamBody = append([]byte(nil), child.UpstreamBody...)
	parent.InputTokens, parent.OutputTokens = child.InputTokens, child.OutputTokens
	parent.CachedTokens, parent.CacheCreationTokens = child.CachedTokens, child.CacheCreationTokens
	parent.Response = child.Response
	if parent.Response == nil && len(parent.UpstreamBody) == 0 {
		if data, err := smartResponseBytes(child); err == nil {
			parent.UpstreamBody = data
		}
	}
}

func smartRetryable(g *core.GatewayContext, err error) bool {
	if g.Ctx.Err() != nil || errors.Is(err, core.ErrClientDisconnected) || g.FatalErr != nil || isExplicitlyNonRetryable(err) {
		return false
	}
	if g.UpstreamResponse != nil {
		status := g.UpstreamResponse.StatusCode
		return status == 429 || status >= 500
	}
	return true
}

// The immutable endpoint subset is checked on every attempt, before priority.
type smartCapacityRouter struct{}

func (smartCapacityRouter) Name() string { return "smart_capacity" }
func (smartCapacityRouter) Route(g *core.GatewayContext, endpoints []*core.Endpoint) []*core.Endpoint {
	req, err := parseSmartRequest(g.RequestType, g.RawBody)
	if err != nil {
		return nil
	}
	var result []*core.Endpoint
	for _, ep := range endpoints {
		if smartEndpointCompatible(ep, req) {
			result = append(result, ep)
		}
	}
	return result
}

func smartEndpointCompatible(ep *core.Endpoint, req *smartRequest) bool {
	if ep == nil || !ep.SupportsRequestType(core.RequestTypeChatCompletion) {
		return false
	}
	output := req.MaxOutput
	if output == 0 {
		output = ep.MaxOutputTokens
	}
	if ep.MaxOutputTokens > 0 && output > ep.MaxOutputTokens {
		return false
	}
	// Conservative UTF-8 byte bound plus message framing; no prompt truncation.
	input := int64(len(req.Messages)) + 128
	return ep.ContextLength <= 0 || output <= ep.ContextLength && input <= ep.ContextLength-output
}

func capacityCheckedInvoker(invoke core.Invoker) (core.Invoker, error) {
	switch v := invoke.(type) {
	case *ClusterInvoker:
		c := *v
		c.routerChain = append([]core.Router{smartCapacityRouter{}}, v.routerChain...)
		return &c, nil
	case *HedgingInvoker:
		c := *v
		c.routerChain = append([]core.Router{smartCapacityRouter{}}, v.routerChain...)
		fallback, err := capacityCheckedInvoker(v.fallbackInvoker)
		if err != nil {
			return nil, err
		}
		c.fallbackInvoker = fallback
		return &c, nil
	default:
		return nil, fmt.Errorf("unsupported smart target invoker")
	}
}
