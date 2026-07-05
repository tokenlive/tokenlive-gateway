package router

import (
	"github.com/tokenlive/tokenlive-gateway/internal/handler"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/spf13/viper"
)

type RouterDeps struct {
	Logger        *log.Logger
	Config        *viper.Viper
	LLMHandler    *handler.LLMHandler
	ApiKeyService *service.ApiKeyService
}
