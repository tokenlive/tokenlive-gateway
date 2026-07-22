package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/tokenlive/tokenlive-gateway/cmd/server/wire"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"go.uber.org/zap"
)

// @title           AI Gateway
// @version         1.0.0
// @description     AI Gateway
// @termsOfService  http://swagger.io/terms/
// @contact.name   API Support
// @contact.url    http://www.swagger.io/support
// @contact.email  support@swagger.io
// @license.name  Apache 2.0
// @license.url   http://www.apache.org/licenses/LICENSE-2.0.html
// @host      localhost:8000
// @securityDefinitions.apiKey Bearer
// @in header
// @name Authorization
// @externalDocs.description  OpenAPI
// @externalDocs.url          https://swagger.io/resources/open-api/
func main() {
	var envConf = flag.String("conf", "config/local.yml", "config path, eg: -conf ./config/local.yml")
	flag.Parse()
	conf := config.NewConfig(*envConf)

	logger := log.NewLog(conf)
	zap.ReplaceGlobals(logger.Logger)

	app, cleanup, err := wire.NewWire(conf, logger)
	if err != nil {
		panic(err)
	}
	// Defer cleanup only after successful init so failed startup skips resource cleanup.
	defer func() {
		logger.Info("shutting down, cleaning up resources...")
		cleanup()
		logger.Info("cleanup completed, server stopped gracefully")
	}()

	scheme := "http"
	if conf.GetBool("http.tls.enabled") {
		scheme = "https"
	}
	logger.Info("server start", zap.String("host", fmt.Sprintf("%s://%s:%d", scheme, conf.GetString("http.host"), conf.GetInt("http.port"))))
	logger.Info("docs addr", zap.String("addr", fmt.Sprintf("%s://%s:%d/swagger/index.html", scheme, conf.GetString("http.host"), conf.GetInt("http.port"))))
	if err = app.Run(context.Background()); err != nil {
		panic(err)
	}
}
