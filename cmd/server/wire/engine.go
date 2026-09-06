package wire

import (
	"github.com/tokenlive/tokenlive-gateway/internal/bootstrap"
	"github.com/tokenlive/tokenlive-gateway/internal/service"
	"github.com/tokenlive/tokenlive-gateway/pkg/compensation"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/core"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// NewGatewayDataStores creates StateStore and CompensationQueue.
func NewGatewayDataStores(v *viper.Viper, rdb *redis.Client) (core.StateStore, compensation.Queue, error) {
	return bootstrap.NewGatewayDataStores(v, rdb)
}

// NewGatewayConfigManager builds a layered ConfigManager from viper.
func NewGatewayConfigManager(v *viper.Viper, logger *log.Logger, rdb *redis.Client) (*config.ConfigManager, error) {
	return bootstrap.NewGatewayConfigManager(v, logger, rdb)
}

// ProvideGatewayProvider creates the unified GatewayProvider from config.
func ProvideGatewayProvider(v *viper.Viper, rdb *redis.Client) (config.GatewayProvider, error) {
	return bootstrap.ProvideGatewayProvider(v, rdb)
}

// NewGatewayEngine creates the Engine from viper and shared dependencies.
// Wire only needs *core.Engine; PolicyService is discarded here (embed path uses bootstrap directly).
func NewGatewayEngine(
	v *viper.Viper,
	logger *log.Logger,
	modelService *service.ModelService,
	apiKeyService *service.ApiKeyService,
	configMgr *config.ConfigManager,
	rdb *redis.Client,
	chConn clickhouse.Conn,
	provider config.GatewayProvider,
) (*core.Engine, func(), error) {
	engine, _, cleanup, err := bootstrap.NewGatewayEngine(v, logger, modelService, apiKeyService, configMgr, rdb, chConn, provider, nil)
	return engine, cleanup, err
}
