package router

import (
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/tokenlive/tokenlive-gateway/internal/handler"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
)

func TestInitLLMRouterRegistersImageGeneration(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := viper.New()
	cfg.Set("llm.enable_auth", false)
	cfg.Set("llm.enable_logging", false)

	engine := gin.New()
	InitLLMRouter(RouterDeps{
		Logger:     &log.Logger{Logger: zap.NewNop()},
		Config:     cfg,
		LLMHandler: handler.NewLLMHandler(nil, nil, nil, nil),
	}, engine.Group("/v1"))

	for _, route := range engine.Routes() {
		if route.Method == "POST" && route.Path == "/v1/images/generations" {
			return
		}
	}
	t.Fatal("POST /v1/images/generations was not registered")
}
