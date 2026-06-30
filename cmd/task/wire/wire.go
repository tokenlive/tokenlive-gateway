//go:build wireinject
// +build wireinject

package wire

import (
	"github.com/tokenlive/tokenlive-gateway/internal/server"
	"github.com/tokenlive/tokenlive-gateway/pkg/app"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/google/wire"
	"github.com/spf13/viper"
)

var serverSet = wire.NewSet(
	server.NewTaskServer,
)

// build App
func newApp(
	task *server.TaskServer,
) *app.App {
	return app.NewApp(
		app.WithServer(task),
		app.WithName("demo-task"),
	)
}

func NewWire(*viper.Viper, *log.Logger) (*app.App, func(), error) {
	panic(wire.Build(
		serverSet,
		newApp,
	))
}
