package policy

import (
	"encoding/json"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPolicyMatcher_Priority(t *testing.T) {
	// policies 传入时，其在 Slice 里的顺序已经天然代表了从低到高的优先级顺位
	policies := []*Policy{
		{
			LimitPolicies: []*LimitPolicy{
				{
					Name: "global-limit",
					Type: "request",
					SlidingWindows: []*SlidingWindow{
						{Threshold: 10, TimeWindowInMs: 1000},
					},
				},
			},
		},
		{
			LimitPolicies: []*LimitPolicy{
				{
					Name: "user-limit",
					Type: "request",
					SlidingWindows: []*SlidingWindow{
						{Threshold: 100, TimeWindowInMs: 1000},
					},
				},
			},
		},
		{
			LimitPolicies: []*LimitPolicy{
				{
					Name: "global-limit", // 同名，应该覆盖 level_0 中的 global-limit
					Type: "request",
					SlidingWindows: []*SlidingWindow{
						{Threshold: 1000, TimeWindowInMs: 1000},
					},
				},
			},
		},
		{
			LoadBalancePolicy: &LoadBalancePolicy{
				Type: "least_connections",
			},
			InvocationPolicy: &InvocationPolicy{
				Type: "failover",
				RetryPolicy: &RetryPolicy{
					Retry: 5,
				},
			},
		},
	}

	pm := NewPolicyMatcher()

	// 模拟四级归并覆盖合并
	merged, err := pm.Match("", "u1", "gpt-4", policies)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if merged.LoadBalancePolicy == nil || merged.LoadBalancePolicy.Type != "least_connections" {
		t.Errorf("expected LB 'least_connections', got %v", merged.LoadBalancePolicy)
	}

	if merged.InvocationPolicy == nil || merged.InvocationPolicy.RetryPolicy == nil || merged.InvocationPolicy.RetryPolicy.Retry != 5 {
		t.Errorf("expected retry 5, got %v", merged.InvocationPolicy)
	}

	// 验证列表合并（Name-based Merge）
	actualLimits := make(map[string]int64)
	for _, lp := range merged.LimitPolicies {
		if len(lp.SlidingWindows) > 0 {
			actualLimits[lp.Name] = lp.SlidingWindows[0].Threshold
		}
	}

	if actualLimits["global-limit"] != 1000 {
		t.Errorf("expected global-limit to be overridden to 1000, got %d", actualLimits["global-limit"])
	}

	if actualLimits["user-limit"] != 100 {
		t.Errorf("expected user-limit to be preserved as 100, got %d", actualLimits["user-limit"])
	}
}

func TestMergePolicies_PointerOverriding(t *testing.T) {
	p1 := &Policy{
		Permissions: []string{"low"},
		InvocationPolicy: &InvocationPolicy{
			Type: "failover",
			RetryPolicy: &RetryPolicy{
				Retry: 2,
			},
		},
		LoadBalancePolicy: &LoadBalancePolicy{
			Type: "round_robin",
		},
	}

	p2 := &Policy{
		Permissions:      []string{"mid"},
		InvocationPolicy: nil,
		LoadBalancePolicy: &LoadBalancePolicy{
			Type: "weighted_rr",
		},
	}

	p3 := &Policy{
		Permissions: []string{"high"},
		InvocationPolicy: &InvocationPolicy{
			Type: "failover",
			RetryPolicy: &RetryPolicy{
				Retry: 4,
			},
		},
		LoadBalancePolicy: nil,
	}

	merged := MergePolicies(p1, p2, p3)
	if merged == nil {
		t.Fatal("expected non-nil merged policy")
	}

	if merged.InvocationPolicy == nil || merged.InvocationPolicy.RetryPolicy == nil || merged.InvocationPolicy.RetryPolicy.Retry != 4 {
		t.Errorf("expected merged retry = 4, got %v", merged.InvocationPolicy)
	}

	if merged.LoadBalancePolicy == nil || merged.LoadBalancePolicy.Type != "weighted_rr" {
		t.Errorf("expected merged LB = 'weighted_rr', got %v", merged.LoadBalancePolicy)
	}

	if len(merged.Permissions) != 1 || merged.Permissions[0] != "high" {
		t.Errorf("expected merged permissions ['high'], got %v", merged.Permissions)
	}
}

func TestPolicyMatcher_ConcurrentMatch(t *testing.T) {
	policies := []*Policy{
		{
			Permissions: []string{"global_fallback"},
		},
	}
	pm := NewPolicyMatcher()

	var wg sync.WaitGroup
	wg.Add(4)

	for g := 0; g < 4; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				p, err := pm.Match("", "u1", "gpt-4", policies)
				if err != nil {
					t.Errorf("routine %d: expected no error, got %v", id, err)
				}
				if p == nil || len(p.Permissions) == 0 || p.Permissions[0] != "global_fallback" {
					t.Errorf("routine %d: unexpected policy: %+v", id, p)
				}
			}
		}(g)
	}

	wg.Wait()
}

func TestUnmarshalLLMPolicies(t *testing.T) {
	rawJSON := `{
		"invocation_policy": {
			"type": "failover",
			"retry_policy": {
				"retry": 3,
				"connect_timeout": 2000,
				"ttft_timeout": 5000,
				"total_timeout": 60000
			}
		},
		"limit_policies": [
			{
				"name": "tpm-limit",
				"type": "token",
				"estimator": {
					"type": "length_ratio",
					"ratio": 0.5
				}
			}
		],
		"circuit_break_policies": [
			{
				"name": "cb-gpt-4",
				"slow_call_metric": "TTFT",
				"degrade_config": {
						"response_code": 503,
						"response_body": "{\"error\":\"service unavailable\"}"
					}
			}
		],
		"billing": {
			"input_price": 0.0015,
			"output_price": 0.002
		}
	}`

	var p Policy
	err := json.Unmarshal([]byte(rawJSON), &p)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if p.InvocationPolicy.RetryPolicy.TtftTimeout != 5000 {
		t.Errorf("expected ttftTimeout 5000, got %d", p.InvocationPolicy.RetryPolicy.TtftTimeout)
	}
	if p.LimitPolicies[0].Estimator.Type != "length_ratio" {
		t.Errorf("expected estimator length_ratio, got %s", p.LimitPolicies[0].Estimator.Type)
	}
	if p.CircuitBreakPolicies[0].SlowCallMetric != "TTFT" {
		t.Errorf("expected slowCallMetric TTFT, got %s", p.CircuitBreakPolicies[0].SlowCallMetric)
	}
	if p.CircuitBreakPolicies[0].DegradeConfig.ResponseBody != "{\"error\":\"service unavailable\"}" {
		t.Errorf("expected degrade response_body, got %s", p.CircuitBreakPolicies[0].DegradeConfig.ResponseBody)
	}
	if p.Billing == nil || p.Billing.InputPrice != 0.0015 || p.Billing.OutputPrice != 0.002 {
		t.Errorf("unexpected billing: %+v", p.Billing)
	}
}

func TestRetryPolicy_UnmarshalAndMatchError(t *testing.T) {
	// 1. 验证 JSON 反序列化混合数组兼容性
	rawIntJSON := `{
		"error_codes": [500, 503],
		"error_messages": ["overloaded", "rate limit exceeded"],
		"code_policy": {
			"parser": "JsonPath",
			"expression": "$.error.code",
			"statuses": ["200"],
			"content_types": ["application/json"]
		}
	}`
	var r1 RetryPolicy
	if err := json.Unmarshal([]byte(rawIntJSON), &r1); err != nil {
		t.Fatalf("failed to unmarshal int error_codes: %v", err)
	}
	if len(r1.ErrorCodes) != 2 || r1.ErrorCodes[0] != "500" || r1.ErrorCodes[1] != "503" {
		t.Errorf("expected error codes [\"500\", \"503\"], got %v", r1.ErrorCodes)
	}

	rawStringJSON := `{
		"error_codes": ["500", "rate_limit_exceeded"],
		"error_messages": ["overloaded"],
		"code_policy": {
			"parser": "JsonPath",
			"expression": "$.error.code"
		}
	}`
	var r2 RetryPolicy
	if err := json.Unmarshal([]byte(rawStringJSON), &r2); err != nil {
		t.Fatalf("failed to unmarshal string error_codes: %v", err)
	}
	if len(r2.ErrorCodes) != 2 || r2.ErrorCodes[0] != "500" || r2.ErrorCodes[1] != "rate_limit_exceeded" {
		t.Errorf("expected error codes [\"500\", \"rate_limit_exceeded\"], got %v", r2.ErrorCodes)
	}

	// 2. 验证 MatchError 逻辑
	// A. 匹配普通状态码 (直接匹配)
	policyDirect := &RetryPolicy{
		ErrorCodes: []string{"502", "503"},
	}
	if !policyDirect.MatchError(502, "application/json", "", nil) {
		t.Error("expected 502 to match direct code policy")
	}
	if policyDirect.MatchError(500, "application/json", "", nil) {
		t.Error("expected 500 to NOT match direct code policy")
	}

	// B. 匹配 CodePolicy (JsonPath 错误码解析)
	policyJsonPath := &RetryPolicy{
		ErrorCodes: []string{"insufficient_quota", "rate_limit_exceeded"},
		CodePolicy: &ErrorParserPolicy{
			Parser:     "JsonPath",
			Expression: "$.error.code",
			Statuses:   []string{"200"},
		},
	}
	body := []byte(`{"error": {"code": "rate_limit_exceeded", "message": "Too many requests"}}`)
	if !policyJsonPath.MatchError(200, "application/json", "", body) {
		t.Error("expected JsonPath parsed error code to match")
	}
	bodyQuota := []byte(`{"error": {"code": "insufficient_quota"}}`)
	if !policyJsonPath.MatchError(200, "application/json", "", bodyQuota) {
		t.Error("expected JsonPath parsed quota error to match")
	}
	bodyOk := []byte(`{"error": {"code": "success"}}`)
	if policyJsonPath.MatchError(200, "application/json", "", bodyOk) {
		t.Error("expected JsonPath parsed success to NOT match")
	}

	// C. 匹配 ErrorMessages (正则与子串)
	policyMsg := &RetryPolicy{
		ErrorMessages: []string{"timeout.*exceeded", "rate limit"},
	}
	if !policyMsg.MatchError(500, "application/json", "request timeout has exceeded, please retry", nil) {
		t.Error("expected regex message pattern to match")
	}
	if !policyMsg.MatchError(500, "application/json", "hit rate limit", nil) {
		t.Error("expected substring message to match")
	}
	if policyMsg.MatchError(500, "application/json", "internal server error", nil) {
		t.Error("expected unmatched message to NOT match")
	}

	// 3. 验证接口断言
	var _ ErrorPolicy = (*RetryPolicy)(nil)
	var _ ErrorPolicy = (*CircuitBreakPolicy)(nil)

	// 4. 验证 CircuitBreakPolicy.MatchError 特有逻辑
	// A. 当无任何匹配规则时，默认直接判定匹配成功 (true)，以保证向后兼容默认全部熔断
	emptyCB := &CircuitBreakPolicy{}
	if !emptyCB.MatchError(502, "application/json", "some error", nil) {
		t.Error("expected empty CircuitBreakPolicy to match true for compatibility")
	}

	// B. 配置具体匹配规则
	rulesCB := &CircuitBreakPolicy{
		ErrorCodes:    []string{"500", "503"},
		ErrorMessages: []string{"overloaded"},
	}
	if !rulesCB.MatchError(503, "application/json", "", nil) {
		t.Error("expected 503 to match CB error code rule")
	}
	if !rulesCB.MatchError(200, "application/json", "server overloaded", nil) {
		t.Error("expected overloaded message to match CB rule")
	}
	if rulesCB.MatchError(502, "application/json", "bad gateway", nil) {
		t.Error("expected 502 with 'bad gateway' to NOT match CB rule")
	}
}

func TestRetryPolicy_MatchErrorWithReason(t *testing.T) {
	// A. 匹配普通状态码
	policyDirect := &RetryPolicy{
		ErrorCodes: []string{"502", "503"},
	}
	matched, reason := policyDirect.MatchErrorWithReason(502, "application/json", "", nil)
	if !matched || reason != "matched status code '502' in error codes list" {
		t.Errorf("expected matched status code 502, got matched=%v, reason='%s'", matched, reason)
	}

	// B. 匹配 CodePolicy (JsonPath 错误码解析)
	policyJsonPath := &RetryPolicy{
		ErrorCodes: []string{"insufficient_quota"},
		CodePolicy: &ErrorParserPolicy{
			Parser:     "JsonPath",
			Expression: "$.error.code",
			Statuses:   []string{"200"},
		},
	}
	body := []byte(`{"error": {"code": "insufficient_quota"}}`)
	matched, reason = policyJsonPath.MatchErrorWithReason(200, "application/json", "", body)
	if !matched || reason != "matched parsed error code 'insufficient_quota' via code policy" {
		t.Errorf("expected matched quota via code policy, got matched=%v, reason='%s'", matched, reason)
	}

	// C. 匹配 ErrorMessages (正则与子串)
	policyMsg := &RetryPolicy{
		ErrorMessages: []string{"timeout.*exceeded", "rate limit"},
	}
	matched, reason = policyMsg.MatchErrorWithReason(500, "application/json", "request timeout has exceeded", nil)
	if !matched || reason != "matched error message pattern 'timeout.*exceeded' (error: 'request timeout has exceeded')" {
		t.Errorf("expected matched message regex, got matched=%v, reason='%s'", matched, reason)
	}

	// E. 未配置任何规则时的默认兼容匹配
	emptyCB := &CircuitBreakPolicy{}
	matched, reason = emptyCB.MatchErrorWithReason(502, "application/json", "some error", nil)
	if !matched || reason != "no rules configured, matching all errors by default" {
		t.Errorf("expected matched empty policy, got matched=%v, reason='%s'", matched, reason)
	}

	// F. 修复：即使配置了 MessagePolicy，如果网络层超时（没有 Body/状态码非200等），依然应该能对底层 errMsg 包含匹配
	policyCombined := &RetryPolicy{
		ErrorMessages: []string{"context deadline exceeded"},
		MessagePolicy: &ErrorParserPolicy{
			Parser:     "JsonPath",
			Expression: "$.error.message",
			Statuses:   []string{"200"},
			ContentTypes: &CaseInsensitiveSet{
				data: map[string]struct{}{"application/json": {}},
			},
		},
	}
	matched, reason = policyCombined.MatchErrorWithReason(0, "", "upstream request: context deadline exceeded", nil)
	if !matched || reason != "matched error message pattern 'context deadline exceeded' (error: 'upstream request: context deadline exceeded')" {
		t.Errorf("expected matched fallback errMsg when messagePolicy fails, got matched=%v, reason='%s'", matched, reason)
	}
}

func TestRetryPolicy_CamelCaseUnmarshal(t *testing.T) {
	camelJSON := `{
		"errorCodes": [500, 502],
		"errorMessages": ["unknown model"],
		"messagePolicy": {
			"parser": "JsonPath",
			"expression": "$.error.message",
			"contentTypes": ["application/json"]
		},
		"connectTimeout": 5000,
		"ttftTimeout": 2000,
		"totalTimeout": 15
	}`

	var r RetryPolicy
	if err := json.Unmarshal([]byte(camelJSON), &r); err != nil {
		t.Fatalf("failed to unmarshal camel case JSON: %v", err)
	}

	if len(r.ErrorCodes) != 2 || r.ErrorCodes[0] != "500" || r.ErrorCodes[1] != "502" {
		t.Errorf("expected errorCodes [\"500\", \"502\"], got %v", r.ErrorCodes)
	}
	if len(r.ErrorMessages) != 1 || r.ErrorMessages[0] != "unknown model" {
		t.Errorf("expected errorMessages [\"unknown model\"], got %v", r.ErrorMessages)
	}
	if r.MessagePolicy == nil || r.MessagePolicy.Parser != "JsonPath" || r.MessagePolicy.Expression != "$.error.message" {
		t.Errorf("expected messagePolicy fields, got %+v", r.MessagePolicy)
	}
	if r.MessagePolicy.ContentTypes == nil || !r.MessagePolicy.ContentTypes.Contains("application/json") {
		t.Errorf("expected messagePolicy contentTypes 'application/json', got %v", r.MessagePolicy.ContentTypes)
	}
	if r.ConnectTimeout != 5000 {
		t.Errorf("expected connectTimeout 5000, got %d", r.ConnectTimeout)
	}
	if r.TtftTimeout != 2000 {
		t.Errorf("expected ttftTimeout 2000, got %d", r.TtftTimeout)
	}
	if r.TotalTimeout != 15000 {
		t.Errorf("expected totalTimeout 15000, got %d", r.TotalTimeout)
	}
}

func TestErrorPolicy_HelperMethods(t *testing.T) {
	p := &RetryPolicy{
		ErrorCodes:    []string{"429", "500"},
		ErrorMessages: []string{"rate limit", "timeout.*exceeded"},
		CodePolicy: &ErrorParserPolicy{
			Parser:     "JsonPath",
			Expression: "$.error.code",
			Statuses:   []string{"200"},
		},
	}

	// 1. ContainsErrorCode
	if !ContainsErrorCode(p, "429") {
		t.Error("expected ContainsErrorCode to return true for '429'")
	}
	if ContainsErrorCode(p, "404") {
		t.Error("expected ContainsErrorCode to return false for '404'")
	}

	// 2. ContainsErrorMessage
	if !ContainsErrorMessage(p, "too many requests, rate limit exceeded") {
		t.Error("expected ContainsErrorMessage to match rate limit")
	}
	if !ContainsErrorMessage(p, "request timeout has exceeded") {
		t.Error("expected ContainsErrorMessage to match regex pattern")
	}
	if ContainsErrorMessage(p, "success") {
		t.Error("expected ContainsErrorMessage to not match success")
	}

	// 4. IsBodyRequired
	if !IsBodyRequired(p) {
		t.Error("expected IsBodyRequired to return true when CodePolicy is set")
	}
	emptyP := &RetryPolicy{}
	if IsBodyRequired(emptyP) {
		t.Error("expected IsBodyRequired to return false for empty policy")
	}

	// 5. Match (初筛)
	if !Match(p, 200, "application/json") {
		t.Error("expected Match to return true for 200/json")
	}
	if Match(p, 404, "text/html") {
		t.Error("expected Match to return false for 404/html")
	}
}

func TestErrorPolicy_RegexCache_Concurrency(t *testing.T) {
	p := &CircuitBreakPolicy{
		ErrorMessages: []string{"error-a.*exceeded", "error-b.*failed"},
	}

	var wg sync.WaitGroup
	concurrency := 20
	iterations := 100

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				matched := ContainsErrorMessage(p, "error-a was exceeded")
				if !matched {
					t.Errorf("expected match to be true")
				}
				matched2 := ContainsErrorMessage(p, "error-b has failed")
				if !matched2 {
					t.Errorf("expected match2 to be true")
				}
				matched3 := ContainsErrorMessage(p, "other error")
				if matched3 {
					t.Errorf("expected match3 to be false")
				}
			}
		}()
	}
	wg.Wait()
}

func TestCaseInsensitiveSet_SerializationAndMatching(t *testing.T) {
	// 1. 测试从 JSON 反序列化大小写不敏感
	rawJSON := `["Application/JSON", "text/HTML"]`
	var set CaseInsensitiveSet
	if err := json.Unmarshal([]byte(rawJSON), &set); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	// 确认大小写不敏感匹配
	if !set.Contains("application/json") {
		t.Error("expected contains 'application/json'")
	}
	if !set.Contains("APPLICATION/JSON") {
		t.Error("expected contains 'APPLICATION/JSON'")
	}
	if !set.Contains("text/html") {
		t.Error("expected contains 'text/html'")
	}
	if set.Contains("text/plain") {
		t.Error("expected not contains 'text/plain'")
	}

	// 2. 测试序列化回 JSON 数组格式
	data, err := json.Marshal(&set)
	if err != nil {
		t.Fatalf("failed to marshal JSON: %v", err)
	}
	var output []string
	if err := json.Unmarshal(data, &output); err != nil {
		t.Fatalf("failed to unmarshal Marshaled data: %v", err)
	}
	if len(output) != 2 {
		t.Errorf("expected 2 elements, got %d", len(output))
	}
	// 确认转换结果全部已化为小写
	hasJSON := false
	hasHTML := false
	for _, val := range output {
		if val == "application/json" {
			hasJSON = true
		}
		if val == "text/html" {
			hasHTML = true
		}
	}
	if !hasJSON || !hasHTML {
		t.Errorf("marshaled list is missing expected lowercased items: %v", output)
	}

	// 3. 测试 YAML 序列化/反序列化
	rawYAML := "- APPLICATION/JSON\n- text/HTML\n"
	var ySet CaseInsensitiveSet
	if err := yaml.Unmarshal([]byte(rawYAML), &ySet); err != nil {
		t.Fatalf("failed to unmarshal YAML: %v", err)
	}
	if !ySet.Contains("application/json") {
		t.Error("expected contains 'application/json' in YAML")
	}

	yData, err := yaml.Marshal(&ySet)
	if err != nil {
		t.Fatalf("failed to marshal YAML: %v", err)
	}
	var yOutput []string
	if err := yaml.Unmarshal(yData, &yOutput); err != nil {
		t.Fatalf("failed to unmarshal Marshaled YAML data: %v", err)
	}
	if len(yOutput) != 2 || yOutput[0] != "application/json" && yOutput[1] != "application/json" {
		t.Errorf("unexpected marshaled yaml items: %v", yOutput)
	}
}

func TestErrorParserPolicy_Match_WithMediaTypeStripping(t *testing.T) {
	// 4. 测试匹配包含 charset 的 Content-Type 并做剥离
	p := &ErrorParserPolicy{
		Parser:     "JsonPath",
		Expression: "$.error.code",
		Statuses:   []string{"200"},
		ContentTypes: &CaseInsensitiveSet{
			data: map[string]struct{}{
				"application/json": {},
			},
		},
	}

	if !p.Match("200", "application/json; charset=utf-8", "200") {
		t.Error("expected match to be true for 'application/json; charset=utf-8'")
	}
	if !p.Match("200", "APPLICATION/JSON; CHARSET=UTF-8", "200") {
		t.Error("expected match to be true for case-insensitive 'APPLICATION/JSON; CHARSET=UTF-8'")
	}
	if p.Match("200", "text/html; charset=utf-8", "200") {
		t.Error("expected match to be false for 'text/html; charset=utf-8'")
	}
}

func TestMergePolicies_VersionMerging(t *testing.T) {
	p1 := &Policy{
		RoutePolicies: []*RoutePolicy{
			{
				Name:    "route-a",
				Version: 100,
				Order:   10,
			},
		},
		TaggingPolicies: []*TaggingPolicy{
			{
				Name:    "tag-a",
				Version: 200,
				Order:   5,
			},
		},
		LoadBalancePolicy: &LoadBalancePolicy{
			Type:    "round_robin",
			Version: 300,
		},
	}

	p2 := &Policy{
		RoutePolicies: []*RoutePolicy{
			{
				Name:    "route-a",
				Version: 101, // 覆盖 p1 的版本
				Order:   20,
			},
		},
		TaggingPolicies: []*TaggingPolicy{
			{
				Name:    "tag-a",
				Version: 201, // 覆盖 p1 的版本
				Order:   15,
			},
		},
		LoadBalancePolicy: &LoadBalancePolicy{
			Type:    "weighted_rr",
			Version: 301, // 覆盖 p1 的版本
		},
	}

	merged := MergePolicies(p1, p2)

	// 验证 RoutePolicy 的 Version 成功覆盖
	if len(merged.RoutePolicies) != 1 {
		t.Fatalf("expected 1 route policy, got %d", len(merged.RoutePolicies))
	}
	if merged.RoutePolicies[0].Version != 101 {
		t.Errorf("expected merged route version 101, got %d", merged.RoutePolicies[0].Version)
	}

	// 验证 TaggingPolicy 的 Version 成功覆盖
	if len(merged.TaggingPolicies) != 1 {
		t.Fatalf("expected 1 tagging policy, got %d", len(merged.TaggingPolicies))
	}
	if merged.TaggingPolicies[0].Version != 201 {
		t.Errorf("expected merged tagging version 201, got %d", merged.TaggingPolicies[0].Version)
	}

	// 验证 LoadBalancePolicy 的 Version 成功覆盖 (LoadBalance是直接对象覆盖)
	if merged.LoadBalancePolicy == nil {
		t.Fatal("expected non-nil load balance policy")
	}
	if merged.LoadBalancePolicy.Version != 301 {
		t.Errorf("expected merged load balance version 301, got %d", merged.LoadBalancePolicy.Version)
	}
}
