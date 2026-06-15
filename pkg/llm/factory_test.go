package llm_test

import (
	"testing"

	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/llm"
	_ "github.com/tokenlive/tokenlive-gateway/pkg/llm/providers"
)

func TestNewProvider_OpenAI(t *testing.T) {
	p, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "openai",
		BaseURL: "https://api.openai.com/v1",
		APIKey:  "sk-test",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if p.Name() != "openai" {
		t.Errorf("expected name 'openai', got '%s'", p.Name())
	}
	if p.Type() != core.ProviderOpenAI {
		t.Errorf("expected type openai, got %s", p.Type())
	}
}

func TestNewProvider_Anthropic(t *testing.T) {
	p, err := llm.NewProvider("anthropic", llm.ProviderConfig{
		Name:    "anthropic",
		BaseURL: "https://api.anthropic.com",
		APIKey:  "sk-ant-test",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if p.Type() != core.ProviderAnthropic {
		t.Errorf("expected type anthropic, got %s", p.Type())
	}
}

func TestNewProvider_UnknownType(t *testing.T) {
	_, err := llm.NewProvider("unknown", llm.ProviderConfig{
		Name:    "unknown",
		BaseURL: "http://localhost",
		APIKey:  "test",
	})
	if err == nil {
		t.Fatal("expected error for unknown provider type")
	}
}

func TestNewProvider_EmptyBaseURL(t *testing.T) {
	_, err := llm.NewProvider("openai", llm.ProviderConfig{
		Name:    "openai",
		BaseURL: "",
		APIKey:  "sk-test",
	})
	if err == nil {
		t.Fatal("expected error for empty base URL")
	}
}
