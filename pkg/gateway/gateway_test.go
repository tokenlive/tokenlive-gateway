package gateway_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/config"
	"github.com/tokenlive/tokenlive-gateway/pkg/gateway"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
)

func TestNewAndRegisterGin_LocalMinimal(t *testing.T) {
	gin.SetMode(gin.TestMode)

	root := findModuleRoot(t)
	confPath := filepath.Join(root, "config", "local.yml")
	if _, err := os.Stat(confPath); err != nil {
		t.Skip("config/local.yml not found")
	}

	// Minimal isolated config: local models, no redis/admin required for provider default path.
	v := viper.New()
	v.SetConfigFile(confPath)
	require.NoError(t, v.ReadInConfig())
	v.Set("gateway.config_source", "local")
	v.Set("gateway.state_store", "memory")
	v.Set("data.redis.addr", "")
	v.Set("llm.enable_auth", false)
	v.Set("access_log.clickhouse.enabled", false)

	logger := log.NewLog(v)
	gw, cleanup, err := gateway.New(v, logger, &gateway.Options{SkipClickHouse: true})
	require.NoError(t, err)
	require.NotNil(t, gw)
	require.NotNil(t, gw.Engine)
	defer cleanup()

	r := gin.New()
	gw.RegisterGin(r)

	// Route table should include chat completions.
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	// Without body we still expect handler to run (not 404).
	require.NotEqual(t, http.StatusNotFound, w.Code)
}

func TestProvideInjectedProvider(t *testing.T) {
	gin.SetMode(gin.TestMode)
	root := findModuleRoot(t)
	confPath := filepath.Join(root, "config", "local.yml")
	if _, err := os.Stat(confPath); err != nil {
		t.Skip("config/local.yml not found")
	}

	v := viper.New()
	v.SetConfigFile(confPath)
	require.NoError(t, v.ReadInConfig())
	v.Set("gateway.config_source", "local")
	v.Set("gateway.state_store", "memory")
	v.Set("data.redis.addr", "")
	v.Set("llm.enable_auth", false)
	v.Set("access_log.clickhouse.enabled", false)

	// Inject nil-safe redis provider with empty client — same as local fallback.
	injected := config.NewRedisGatewayProviderWithAPIKeyPepper(nil, "")

	logger := log.NewLog(v)
	gw, cleanup, err := gateway.New(v, logger, &gateway.Options{
		Provider:       injected,
		SkipClickHouse: true,
	})
	require.NoError(t, err)
	defer cleanup()
	require.Equal(t, injected, gw.Provider)
}

func findModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
