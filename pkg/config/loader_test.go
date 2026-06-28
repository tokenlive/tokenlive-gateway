package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestViper() *viper.Viper {
	v := viper.New()

	v.Set("models.gpt-4.request_types", []string{"chat_completion"})
	v.Set("models.gpt-4.endpoints", []map[string]interface{}{
		{"provider": "openai-official", "url": "https://api.openai.com/v1", "priority": 1, "weight": 80},
		{"provider": "openai-official", "url": "https://api.openai.com/v1", "priority": 2, "real_model": "gpt-4-0613", "weight": 20},
	})

	v.Set("models.claude-sonnet.request_types", []string{"chat_completion"})
	v.Set("models.claude-sonnet.endpoints", []map[string]interface{}{
		{"provider": "anthropic-official", "url": "https://api.anthropic.com", "priority": 1},
	})

	// providers 只定义基础设施
	v.Set("providers.openai-official.protocol", "openai")
	v.Set("providers.openai-official.api_key", "sk-openai")
	v.Set("providers.openai-official.timeout", "60s")

	v.Set("providers.anthropic-official.protocol", "anthropic")
	v.Set("providers.anthropic-official.api_key", "sk-ant")

	v.Set("fallbacks.gpt-4", []string{"gpt-3.5-turbo"})
	return v
}

func TestLoad(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)

	assert.Len(t, cfg.Models, 2)
	assert.Len(t, cfg.Models["gpt-4"].Endpoints, 2)
	assert.Len(t, cfg.Providers, 2)
	assert.Equal(t, "openai", cfg.Providers["openai-official"].Protocol)
}

func TestValidate_OK(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)
	assert.NoError(t, Validate(cfg))
}

func TestValidate_UnknownProvider(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)

	// 添加一个引用不存在 provider 的 endpoint
	cfg.Models["bad-model"] = ModelConfig{
		RequestTypes: []string{"chat_completion"},
		Endpoints: []EndpointConfig{
			{Provider: "nonexistent", URL: "https://example.com"},
		},
	}
	assert.Error(t, Validate(cfg))
}

func TestValidate_MissingProvider(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)

	cfg.Models["bad-model"] = ModelConfig{
		RequestTypes: []string{"chat_completion"},
		Endpoints: []EndpointConfig{
			{URL: "https://example.com"}, // provider 为空
		},
	}
	assert.Error(t, Validate(cfg))
}

func TestValidate_MissingURL(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)

	cfg.Models["bad-model"] = ModelConfig{
		RequestTypes: []string{"chat_completion"},
		Endpoints: []EndpointConfig{
			{Provider: "openai-official"}, // url 为空
		},
	}
	assert.Error(t, Validate(cfg))
}

func TestResolve_Inheritance(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)

	resolved := Resolve(cfg)
	assert.Len(t, resolved, 2)
	assert.Len(t, resolved["gpt-4"], 2)
	assert.Len(t, resolved["claude-sonnet"], 1)

	// 找到 priority=2 的条目，它覆盖了 real_model
	var overridden ResolvedEndpoint
	for _, r := range resolved["gpt-4"] {
		if r.Priority == 2 {
			overridden = r
			break
		}
	}
	assert.Equal(t, "gpt-4-0613", overridden.RealModel)
	assert.Equal(t, "sk-openai", overridden.APIKey) // 继承自 provider
	assert.Equal(t, int64(60000), overridden.Timeout)
	assert.Equal(t, 20, overridden.Weight)

	// 找到 priority=1 的条目
	var primary ResolvedEndpoint
	for _, r := range resolved["gpt-4"] {
		if r.Priority == 1 {
			primary = r
			break
		}
	}
	assert.Equal(t, "", primary.RealModel) // 未设置 real_model
	assert.Equal(t, 80, primary.Weight)
}

func TestResolve_PreservesEndpointIDAndCode(t *testing.T) {
	v := newTestViper()
	v.Set("models.gpt-4.endpoints", []map[string]interface{}{
		{
			"id":       "endpoint-id-1",
			"code":     "endpoint-code-1",
			"provider": "openai-official",
			"url":      "https://api.openai.com/v1",
			"priority": 1,
			"weight":   80,
		},
	})

	cfg, err := Load(v)
	require.NoError(t, err)

	resolved := Resolve(cfg)
	require.Len(t, resolved["gpt-4"], 1)
	assert.Equal(t, "endpoint-id-1", resolved["gpt-4"][0].ID)
	assert.Equal(t, "endpoint-code-1", resolved["gpt-4"][0].Code)
}

func TestResolve_DefaultWeight(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)

	resolved := Resolve(cfg)
	assert.Len(t, resolved["claude-sonnet"], 1)
	claude := resolved["claude-sonnet"][0]
	assert.Equal(t, defaultWeight, claude.Weight)
}

func TestResolve_EndpointOverridesProviderAPIKey(t *testing.T) {
	v := newTestViper()
	v.Set("models.gpt-4.endpoints", []map[string]interface{}{
		{"provider": "openai-official", "url": "https://api.openai.com/v1", "api_key": "sk-override"},
	})

	cfg, err := Load(v)
	require.NoError(t, err)

	resolved := Resolve(cfg)
	assert.Len(t, resolved["gpt-4"], 1)
	gpt4 := resolved["gpt-4"][0]
	assert.Equal(t, "sk-override", gpt4.APIKey)
}

func TestResolve_EndpointOverridesProviderProtocol(t *testing.T) {
	v := newTestViper()
	v.Set("models.gpt-4.endpoints", []map[string]interface{}{
		{"provider": "openai-official", "url": "https://api.openai.com/v1", "protocol": "anthropic"},
	})

	cfg, err := Load(v)
	require.NoError(t, err)

	resolved := Resolve(cfg)
	assert.Len(t, resolved["gpt-4"], 1)
	gpt4 := resolved["gpt-4"][0]
	assert.Equal(t, "anthropic", gpt4.ProviderProtocol)
}

func TestResolve_EndpointOverridesProviderTimeout(t *testing.T) {
	v := newTestViper()
	v.Set("models.gpt-4.endpoints", []map[string]interface{}{
		{"provider": "openai-official", "url": "https://api.openai.com/v1", "timeout": "30s"},
	})

	cfg, err := Load(v)
	require.NoError(t, err)

	resolved := Resolve(cfg)
	assert.Len(t, resolved["gpt-4"], 1)
	gpt4 := resolved["gpt-4"][0]
	assert.Equal(t, int64(30000), gpt4.Timeout)
}

func TestKnownModels(t *testing.T) {
	v := newTestViper()
	cfg, err := Load(v)
	require.NoError(t, err)

	known := KnownModels(cfg)
	assert.True(t, known["gpt-4"])
	assert.True(t, known["claude-sonnet"])
	assert.False(t, known["nonexistent"])
}

func TestLoad_WithPipelines(t *testing.T) {
	v := newTestViper()

	// 模拟写入 pipelines 的配置
	v.Set("pipelines.chat_completion.name", "chat_completion")
	v.Set("pipelines.chat_completion.request_types", []string{"chat_completion"})
	v.Set("pipelines.chat_completion.inbound_filters", []string{"auth", "validate"})
	v.Set("pipelines.chat_completion.outbound_filters", []string{"metrics", "access_log"})
	v.Set("pipelines.chat_completion.invoker.type", "cluster")
	v.Set("pipelines.chat_completion.invoker.routers", []string{"capability", "circuit_breaker"})
	v.Set("pipelines.chat_completion.invoker.load_balancer", "weighted_rr")
	v.Set("pipelines.chat_completion.invoker.retry.retry", 5)
	v.Set("pipelines.chat_completion.invoker.retry.backoff_type", "exponential")
	v.Set("pipelines.chat_completion.invoker.retry.base_ms", 150)
	v.Set("pipelines.chat_completion.invoker.retry.error_codes", []string{"502", "504"})

	cfg, err := Load(v)
	require.NoError(t, err)
	require.NotNil(t, cfg.Pipelines)

	p, ok := cfg.Pipelines["chat_completion"]
	require.True(t, ok)
	assert.Equal(t, "chat_completion", p.Name)
	assert.Equal(t, []string{"auth", "validate"}, p.InboundFilters)
	assert.Equal(t, "cluster", p.Invoker.Type)
	assert.Equal(t, []string{"capability", "circuit_breaker"}, p.Invoker.Routers)
	assert.Equal(t, "weighted_rr", p.Invoker.LoadBalancer)

	require.NotNil(t, p.Invoker.Retry)
	assert.Equal(t, 5, p.Invoker.Retry.Retry)
	assert.Equal(t, "exponential", p.Invoker.Retry.BackoffType)
	assert.Equal(t, 150, p.Invoker.Retry.BaseMs)
	assert.Equal(t, []string{"502", "504"}, p.Invoker.Retry.ErrorCodes)
}
