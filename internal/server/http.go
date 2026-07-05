package server

import (
	apiV1 "github.com/tokenlive/tokenlive-gateway/api/v1"
	"github.com/tokenlive/tokenlive-gateway/docs"
	"github.com/tokenlive/tokenlive-gateway/internal/middleware"
	"github.com/tokenlive/tokenlive-gateway/internal/router"
	"github.com/tokenlive/tokenlive-gateway/pkg/server/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerfiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func NewHTTPServer(
	deps router.RouterDeps,
) *http.Server {
	if deps.Config.GetString("env") == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}
	s := http.NewServer(
		gin.Default(),
		deps.Logger,
		http.WithServerHost(deps.Config.GetString("http.host")),
		http.WithServerPort(deps.Config.GetInt("http.port")),
		http.WithTLS(
			deps.Config.GetBool("http.tls.enabled"),
			deps.Config.GetString("http.tls.cert_file"),
			deps.Config.GetString("http.tls.key_file"),
		),
	)

	// swagger doc
	docs.SwaggerInfo.BasePath = "/v1"
	if deps.Config.GetBool("http.tls.enabled") {
		docs.SwaggerInfo.Schemes = []string{"https"}
	} else {
		docs.SwaggerInfo.Schemes = []string{"http"}
	}
	s.GET("/swagger/*any", ginSwagger.WrapHandler(
		swaggerfiles.Handler,
		//ginSwagger.URL(fmt.Sprintf("http://localhost:%d/swagger/doc.json", deps.Config.GetInt("app.http.port"))),
		ginSwagger.DefaultModelsExpandDepth(-1),
		ginSwagger.PersistAuthorization(true),
	))

	s.Use(
		middleware.CORSMiddleware(),
		middleware.ResponseLogMiddleware(deps.Logger),
		middleware.RequestLogMiddleware(deps.Logger),
	)
	s.GET("/", func(ctx *gin.Context) {
		deps.Logger.WithContext(ctx).Info("hello")
		apiV1.HandleSuccess(ctx, map[string]interface{}{
			":)": "Thank you for using tokenlive!",
		})
	})

	// 系统基础路由
	s.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	if deps.Config.GetBool("llm.enable_metrics") {
		s.GET("/metrics", gin.WrapH(promhttp.Handler()))
	}

	v1 := s.Group("/v1")

	// 注册 LLM 路由
	if deps.LLMHandler != nil {
		router.InitLLMRouter(deps, v1)
		v1beta := s.Group("/v1beta")
		router.InitGeminiRouter(deps, v1beta)
	}

	return s
}
