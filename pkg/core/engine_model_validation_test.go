package core_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/compositerouting"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/filters/inbound"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/policy"
	"github.com/tokenlive/tokenlive-gateway/pkg/store"
	"go.uber.org/zap"
)

// 保留原始回归：未知模型不能因缺少定价策略而返回 403。
func TestEngineModelValidation_UnknownModelBeforePricing(t *testing.T) {
	runEngineModelValidationCase(t, engineModelValidationProtocols()[0], engineModelValidationCase{
		model:       "Gemini-4-neon",
		policy:      &policy.Policy{Permissions: []string{"*"}},
		wantStatus:  http.StatusBadRequest,
		wantMessage: "unknown model: Gemini-4-neon",
		wantCalls:   engineModelValidationCalls{validation: 1},
	})
}

func TestEngineModelValidation_UnknownModelPolicies(t *testing.T) {
	for _, protocol := range engineModelValidationProtocols() {
		t.Run(protocol.name, func(t *testing.T) {
			cases := []engineModelValidationCase{
				{
					name: "no_policy", model: "Gemini-4-neon",
					wantStatus: http.StatusForbidden, wantMessage: "no policy matched",
				},
				{
					name: "wildcard_without_billing", model: "Gemini-4-neon",
					policy:      &policy.Policy{Permissions: []string{"*"}},
					wantStatus:  http.StatusBadRequest,
					wantMessage: "unknown model: Gemini-4-neon",
					wantCalls:   engineModelValidationCalls{validation: 1},
				},
				{
					name: "wildcard_with_billing", model: "Gemini-4-neon",
					policy:      &policy.Policy{Permissions: []string{"*"}, Billing: &policy.BillingPolicy{}},
					wantStatus:  http.StatusBadRequest,
					wantMessage: "unknown model: Gemini-4-neon",
					wantCalls:   engineModelValidationCalls{validation: 1},
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) { runEngineModelValidationCase(t, protocol, tc) })
			}
		})
	}
}

func TestEngineModelValidation_KnownModelsAndAuthPrecedence(t *testing.T) {
	for _, protocol := range engineModelValidationProtocols() {
		t.Run(protocol.name, func(t *testing.T) {
			cases := []engineModelValidationCase{
				{
					name: "known_without_billing", model: "model-fixture",
					policy:      &policy.Policy{Permissions: []string{"*"}},
					wantStatus:  http.StatusForbidden,
					wantMessage: "Model pricing not configured. Please contact your administrator.",
					wantCalls:   engineModelValidationCalls{validation: 1},
				},
				{
					name: "known_with_billing", model: "model-fixture",
					policy:     &policy.Policy{Permissions: []string{"*"}, Billing: &policy.BillingPolicy{}},
					wantStatus: http.StatusOK, wantModel: "model-fixture",
					wantCalls: engineModelValidationCalls{validation: 1, credits: 1, limit: 1, invoker: 1},
				},
				{
					name: "model_permission_denied", model: "Gemini-4-neon",
					policy:      &policy.Policy{Permissions: []string{"model-fixture"}},
					wantStatus:  http.StatusForbidden,
					wantMessage: "access denied to model: Gemini-4-neon",
				},
				{
					name: "missing_authentication", model: "Gemini-4-neon", unauthenticated: true,
					policy:      &policy.Policy{Permissions: []string{"*"}},
					wantStatus:  http.StatusUnauthorized,
					wantMessage: "missing authentication user or tenant",
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) { runEngineModelValidationCase(t, protocol, tc) })
			}
		})
	}
}

// 只验证逻辑模型通过真实校验，不模拟 SmartRouter 的子模型路由。
func TestEngineModelValidation_CanonicalAndLogicalModels(t *testing.T) {
	protocol := engineModelValidationProtocols()[0]
	cases := []struct {
		name, model, canonical string
		endpointFree           bool
	}{
		{"canonical", "model-fixture", "model-fixture", false},
		{"alias", "alias-fixture", "model-fixture", false},
		{"mixed_case_alias", "AlIaS-FiXtUrE", "model-fixture", false},
		{"mixed_case_canonical", "MoDeL-FiXtUrE", "model-fixture", false},
		{"endpoint_free_smart", "smart-fixture", "smart-fixture", true},
		{"endpoint_free_composite", "composite-fixture", "composite-fixture", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newEngineModelValidationFixture(t, &policy.Policy{
				Permissions: []string{tc.canonical}, Billing: &policy.BillingPolicy{},
			})
			if tc.endpointFree {
				assert.Empty(t, fixture.manager.GetEndpoints(context.Background(), tc.canonical), "logical model has no direct endpoints")
			}
			response := fixture.handle(protocol, tc.model, false)
			assertEngineModelValidationResponse(t, response, protocol, http.StatusOK, "", tc.canonical)
			fixture.assertCalls(t, engineModelValidationCalls{validation: 1, credits: 1, limit: 1, invoker: 1})
		})
	}
}

func TestEngineModelValidation_SnapshotRemoval(t *testing.T) {
	protocol := engineModelValidationProtocols()[0]
	for _, model := range []string{"model-fixture", "smart-fixture", "composite-fixture"} {
		t.Run(model, func(t *testing.T) {
			fixture := newEngineModelValidationFixture(t, &policy.Policy{
				Permissions: []string{model}, Billing: &policy.BillingPolicy{},
			})
			response := fixture.handle(protocol, model, false)
			assertEngineModelValidationResponse(t, response, protocol, http.StatusOK, "", model)
			fixture.assertCalls(t, engineModelValidationCalls{validation: 1, credits: 1, limit: 1, invoker: 1})

			fixture.manager.UpdateYAMLConfig(&config.GatewayConfig{})
			response = fixture.handle(protocol, model, false)
			assertEngineModelValidationResponse(t, response, protocol, http.StatusBadRequest, "unknown model: "+model, "")
			// 同一 Engine 的下一次请求必须使用更新后的快照，不能再次进入下游。
			fixture.assertCalls(t, engineModelValidationCalls{validation: 2, credits: 1, limit: 1, invoker: 1})
		})
	}
}

type engineModelValidationProtocol struct {
	name, path, bodyFormat string
	requestType            core.RequestType
}

func engineModelValidationProtocols() []engineModelValidationProtocol {
	return []engineModelValidationProtocol{
		{"Chat", "/v1/chat/completions", `{"model":%q,"messages":[{"role":"user","content":"hello"}]}`, core.RequestTypeChatCompletion},
		{"Responses", "/v1/responses", `{"model":%q,"input":"hello"}`, core.RequestTypeResponses},
		{"Messages", "/v1/messages", `{"model":%q,"messages":[{"role":"user","content":"hello"}],"max_tokens":16}`, core.RequestTypeMessages},
	}
}

type engineModelValidationCase struct {
	name, model, wantMessage, wantModel string
	policy                              *policy.Policy
	unauthenticated                     bool
	wantStatus                          int
	wantCalls                           engineModelValidationCalls
}

type engineModelValidationCalls struct {
	validation, credits, limit, invoker int
}

func runEngineModelValidationCase(t *testing.T, protocol engineModelValidationProtocol, tc engineModelValidationCase) {
	t.Helper()
	fixture := newEngineModelValidationFixture(t, tc.policy)
	response := fixture.handle(protocol, tc.model, tc.unauthenticated)
	assertEngineModelValidationResponse(t, response, protocol, tc.wantStatus, tc.wantMessage, tc.wantModel)
	fixture.assertCalls(t, tc.wantCalls)
}

func assertEngineModelValidationResponse(t *testing.T, response *httptest.ResponseRecorder, protocol engineModelValidationProtocol, status int, message, model string) {
	t.Helper()
	t.Logf("response: HTTP %d %s", response.Code, strings.TrimSpace(response.Body.String()))
	assert.Equal(t, status, response.Code, "HTTP status")
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"), "Content-Type")
	wantBody := fmt.Sprintf(`{"id":"fixture-response","model":%q}`, model)
	if status != http.StatusOK {
		wantBody = fmt.Sprintf(`{"error":{"code":%d,"message":%q,"type":"gateway_error"}}`, status, message)
		if protocol.requestType == core.RequestTypeMessages {
			errorTypes := map[int]string{
				http.StatusBadRequest:   "invalid_request_error",
				http.StatusUnauthorized: "authentication_error",
				http.StatusForbidden:    "permission_error",
			}
			wantBody = fmt.Sprintf(`{"type":"error","error":{"type":%q,"message":%q}}`, errorTypes[status], message)
		}
	}
	assert.JSONEq(t, wantBody, response.Body.String(), "response JSON contract")
}

type engineModelValidationFixture struct {
	engine    *core.Engine
	manager   *config.ConfigManager
	validator *countingEngineModelValidator
	credits   *countingEngineCreditsChecker
	limit     *countingEngineRateLimitFilter
	invoker   *countingEngineModelInvoker
}

func newEngineModelValidationFixture(t *testing.T, requestPolicy *policy.Policy) *engineModelValidationFixture {
	t.Helper()
	logger := &log.Logger{Logger: zap.NewNop()}
	gwCfg := &config.GatewayConfig{
		Models: map[string]config.ModelConfig{
			"model-fixture":     engineModelValidationOrdinaryModel("model-fixture"),
			"alternate-fixture": engineModelValidationOrdinaryModel("alternate-fixture"),
			"smart-fixture": {
				ModelType: "smart", RequestTypes: []string{"chat_completion"},
				SmartRouting: &core.SmartRoutingConfig{
					Version: 1, JudgeModel: "model-fixture",
					Ranges: []core.SmartRoutingRange{
						{Min: 0, Max: 50, Model: "model-fixture"},
						{Min: 50, Max: 100, Model: "alternate-fixture"},
					},
				},
			},
			"composite-fixture": {
				ModelType: "smart", RequestTypes: []string{"chat_completion"},
				SmartRouting: &core.SmartRoutingConfig{
					SchemaVersion: 2, Version: 1, Mode: compositerouting.Priority, PrimaryModel: "model-fixture",
				},
			},
		},
		Providers: map[string]config.ProviderConfig{
			"provider-fixture": {Protocol: "openai", APIKey: "upstream-fixture-key"},
		},
		Aliases: map[string]string{"alias-fixture": "model-fixture"},
	}
	manager := config.NewConfigManager(gwCfg, nil, zap.NewNop())
	modelSvc := service.NewModelService(nil, logger, viper.New())
	modelSvc.SetConfigManager(manager)
	aliases := service.NewAliasService(nil, logger, manager)
	state := store.NewMemoryStateStore()
	fixture := &engineModelValidationFixture{
		manager:   manager,
		validator: &countingEngineModelValidator{delegate: modelSvc},
		credits:   &countingEngineCreditsChecker{},
		limit:     &countingEngineRateLimitFilter{delegate: inbound.NewRateLimitFilter(state)},
		invoker:   &countingEngineModelInvoker{},
	}
	engineCfg := &core.EngineConfig{Pipelines: make(map[string]*core.PipelineConfig)}
	for _, protocol := range engineModelValidationProtocols() {
		name := string(protocol.requestType)
		engineCfg.Pipelines[name] = &core.PipelineConfig{
			Name: name, RequestTypes: []core.RequestType{protocol.requestType},
			// 故意不按顺序配置，由真实 Engine 按各过滤器的 Order 排序。
			InboundFilters: []string{"rate_limit", "credits_check", "validate", "auth"},
			Invoker:        core.InvokerConfig{Type: "cluster"},
		}
	}
	fixture.engine = core.NewEngine(engineCfg, &chaosDiscovery{}, state,
		&mockPolicyProvider{policy: requestPolicy}, zap.NewNop())
	t.Cleanup(func() {
		if err := fixture.engine.Close(); err != nil {
			t.Errorf("close engine fixture: %v", err)
		}
	})
	fixture.engine.SetAliasService(aliases)
	fixture.engine.SetInvokerBuilder(&testInvokerBuilder{invoker: fixture.invoker})
	fixture.engine.RegisterFilter("validate", inbound.NewValidateFilter(fixture.validator))
	fixture.engine.RegisterFilter("auth", inbound.NewAuthFilter())
	fixture.engine.RegisterFilter("credits_check", inbound.NewCreditsCheckFilter(fixture.credits))
	fixture.engine.RegisterFilter("rate_limit", fixture.limit)
	if err := fixture.engine.Init(); err != nil {
		t.Fatalf("initialize engine fixture: %v", err)
	}
	return fixture
}

func engineModelValidationOrdinaryModel(model string) config.ModelConfig {
	return config.ModelConfig{
		RequestTypes: []string{"chat_completion", "responses", "messages"},
		Endpoints: []config.EndpointConfig{{
			Code: "endpoint-" + model, Provider: "provider-fixture",
			URL: "https://upstream-fixture.invalid/v1", RealModel: model,
		}},
	}
}

func (f *engineModelValidationFixture) handle(protocol engineModelValidationProtocol, model string, unauthenticated bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, protocol.path, strings.NewReader(fmt.Sprintf(protocol.bodyFormat, model)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer fixture-key")
	if !unauthenticated {
		request.Header.Set("X-User-ID", "user-fixture")
	}
	response := httptest.NewRecorder()
	f.engine.HandleRequest(response, request)
	return response
}

func (f *engineModelValidationFixture) assertCalls(t *testing.T, want engineModelValidationCalls) {
	t.Helper()
	got := engineModelValidationCalls{f.validator.calls, f.credits.calls, f.limit.calls, f.invoker.calls}
	t.Logf("calls: validation=%d credits=%d limit=%d invoker=%d", got.validation, got.credits, got.limit, got.invoker)
	// Billing 为 nil 时余额检查器本就不会调用，必须同时确认真实模型校验发生。
	assert.Equal(t, want, got, "delegated filter and downstream calls")
}

type countingEngineModelValidator struct {
	delegate inbound.ModelValidator
	calls    int
}

func (v *countingEngineModelValidator) ValidateModel(ctx context.Context, model, tenant, userID string) (bool, error) {
	v.calls++
	return v.delegate.ValidateModel(ctx, model, tenant, userID)
}

type countingEngineCreditsChecker struct {
	calls int
}

func (c *countingEngineCreditsChecker) CheckCredits(context.Context, string) error {
	c.calls++
	return nil
}

type countingEngineRateLimitFilter struct {
	delegate *inbound.RateLimitFilter
	calls    int
}

func (f *countingEngineRateLimitFilter) Name() string { return f.delegate.Name() }
func (f *countingEngineRateLimitFilter) Order() int   { return f.delegate.Order() }
func (f *countingEngineRateLimitFilter) OnRequest(gctx *core.GatewayContext) error {
	f.calls++
	return f.delegate.OnRequest(gctx)
}

// 假调用器隔离上游边界，fixture 中的 URL 永远不会被访问。
type countingEngineModelInvoker struct {
	calls int
}

func (i *countingEngineModelInvoker) Invoke(gctx *core.GatewayContext) error {
	i.calls++
	gctx.Response = map[string]interface{}{"id": "fixture-response", "model": gctx.Model}
	return nil
}

func (i *countingEngineModelInvoker) Endpoint() *core.Endpoint { return nil }
