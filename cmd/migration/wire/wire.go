//go:build wireinject
// +build wireinject

package wire

import (
	"github.com/tokenlive/tokenlive-gateway/internal/repository"
	"github.com/tokenlive/tokenlive-gateway/internal/server"
	"github.com/tokenlive/tokenlive-gateway/pkg/app"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/google/wire"
	"github.com/spf13/viper"
)

var repositorySet = wire.NewSet(
	repository.NewDB,
	//repository.NewRedis,
	repository.NewRepository,
)
var serverSet = wire.NewSet(
	server.NewMigrateServer,
)

// build App
func newApp(
	migrateServer *server.MigrateServer,
) *app.App {
	return app.NewApp(
		app.WithServer(migrateServer),
		app.WithName("demo-migrate"),
	)
}

func NewWire(*viper.Viper, *log.Logger) (*app.App, func(), error) {
	panic(wire.Build(
		repositorySet,
		serverSet,
		newApp,
	))
}
