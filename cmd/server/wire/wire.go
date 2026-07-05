//go:build wireinject
// +build wireinject

package wire

import (
	"github.com/tokenlive/tokenlive-gateway/internal/handler"
	"github.com/tokenlive/tokenlive-gateway/internal/repository"
	"github.com/tokenlive/tokenlive-gateway/internal/router"
	"github.com/tokenlive/tokenlive-gateway/internal/server"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/app"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/server/http"

	"github.com/google/wire"
	"github.com/spf13/viper"
)

var repositorySet = wire.NewSet(
	repository.NewDB,
	repository.LoadRedisConfig,
	repository.NewRedis,
	repository.NewClickHouse,
	//repository.NewMongo,
	repository.NewRepository,
	repository.NewTransaction,
)

var serviceSet = wire.NewSet(
	service.NewApiKeyService,
	service.NewModelService,
	service.NewAliasService,
)

var handlerSet = wire.NewSet(
	handler.NewLLMHandler,
	NewGatewayConfigManager,
	NewGatewayEngine,
	ProvideGatewayProvider,
)

var serverSet = wire.NewSet(
	server.NewHTTPServer,
	server.NewJobServer,
)

// build App
func newApp(
	httpServer *http.Server,
	jobServer *server.JobServer,
	// task *server.Task,
) *app.App {
	return app.NewApp(
		app.WithServer(httpServer, jobServer),
		app.WithName("github.com/tokenlive/tokenlive-gateway"),
	)
}

func NewWire(*viper.Viper, *log.Logger) (*app.App, func(), error) {
	panic(wire.Build(
		repositorySet,
		serviceSet,
		handlerSet,
		serverSet,
		wire.Struct(new(router.RouterDeps), "*"),
		newApp,
	))
}
