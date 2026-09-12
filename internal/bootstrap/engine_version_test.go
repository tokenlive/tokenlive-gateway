package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/tokenlive/tokenlive-gateway/pkg/log"
	"github.com/tokenlive/tokenlive-gateway/pkg/versionreport"
	"go.uber.org/zap"
)

// These tests build only in-memory engines; no project configuration or .env is read.
func versionReportConfig(t *testing.T) *viper.Viper {
	t.Helper()
	for _, key := range []string{
		"APP_CONF", "ADMIN_SERVER_URL", "GATEWAY_SYNC_TOKEN", "GATEWAY_VERSION_NAMESPACE",
		"OTEL_METRICS_EXPORTER", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_SDK_DISABLED",
	} {
		t.Setenv(key, "")
	}
	v := viper.New()
	v.Set("gateway.config_source", "local")
	v.Set("gateway.state_store", "memory")
	v.Set("models", map[string]any{})
	v.Set("runtime.version", "v2.3.4")
	v.Set("runtime.build_kind", "release")
	return v
}

func newVersionTestEngine(t *testing.T, v *viper.Viper, client *redis.Client) func() {
	t.Helper()
	engine, _, cleanup, err := NewGatewayEngine(v, &log.Logger{Logger: zap.NewNop()}, nil, nil, nil, client, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, engine)
	var once sync.Once
	stop := func() { once.Do(cleanup) }
	t.Cleanup(stop)
	return stop
}

func TestVersionReportStartsAfterEngineBuildAndUsesOneProcessIdentity(t *testing.T) {
	var previousID string
	for _, fromEnv := range []bool{false, true} {
		t.Run(map[bool]string{false: "viper", true: "environment"}[fromEnv], func(t *testing.T) {
			v := versionReportConfig(t)
			calls := make(chan versionreport.Node, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/gateway/version" {
					http.NotFound(w, r)
					return
				}
				if r.Header.Get("X-Sync-Token") != "test-token" {
					t.Error("missing sync token")
				}
				var n versionreport.Node
				if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
					t.Error(err)
				}
				calls <- n
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			if fromEnv {
				t.Setenv("ADMIN_SERVER_URL", server.URL)
				t.Setenv("GATEWAY_SYNC_TOKEN", "test-token")
				t.Setenv("GATEWAY_VERSION_NAMESPACE", "Production_A")
			} else {
				v.Set("gateway.admin_url", server.URL)
				v.Set("gateway.sync_token", "test-token")
			}
			cleanup := newVersionTestEngine(t, v, nil)
			select {
			case n := <-calls:
				require.Equal(t, 1, n.SchemaVersion)
				require.Equal(t, "v2.3.4", n.Version)
				require.Equal(t, "release", n.BuildKind)
				if fromEnv {
					require.Equal(t, "Production_A", n.Namespace)
				} else {
					require.Equal(t, "default", n.Namespace)
				}
				id, err := uuid.Parse(n.InstanceID)
				require.NoError(t, err)
				require.Equal(t, id.String(), n.InstanceID)
				if previousID != "" {
					require.Equal(t, previousID, n.InstanceID, "a process must generate its ID only once")
				}
				previousID = n.InstanceID
			case <-time.After(time.Second):
				t.Fatal("successful engine build did not start its version reporter")
			}
			cleanup()
		})
	}
}

func TestVersionReportEmbeddedDisablesRedisAndHTTP(t *testing.T) {
	v := versionReportConfig(t)
	v.Set("gateway.config_source", "embedded")
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/gateway/version" {
			calls.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	v.Set("gateway.admin_url", server.URL)
	v.Set("gateway.sync_token", "test-token")
	cleanup := newVersionTestEngine(t, v, client)
	require.Never(t, func() bool {
		keys, err := client.Keys(context.Background(), "tokenlive:gateway-versions:*").Result()
		require.NoError(t, err)
		return len(keys) > 0 || calls.Load() != 0
	}, 100*time.Millisecond, 5*time.Millisecond)
	cleanup()
	require.NoError(t, client.Ping(context.Background()).Err(), "borrowed Redis client must remain usable")
}

func TestVersionReportCleanupCancelsInflightHTTP(t *testing.T) {
	v := versionReportConfig(t)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/gateway/version" {
			http.NotFound(w, r)
			return
		}
		started <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	v.Set("gateway.admin_url", server.URL)
	v.Set("gateway.sync_token", "test-token")
	cleanup := newVersionTestEngine(t, v, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("no report")
	}
	done := make(chan struct{})
	go func() {
		cleanup()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("engine cleanup did not cancel the active HTTP report")
	}
}

func TestVersionReportDisabledWithoutChannelOrToken(t *testing.T) {
	v := versionReportConfig(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	v.Set("gateway.admin_url", server.URL)
	cleanup := newVersionTestEngine(t, v, nil)
	cleanup()
	require.Zero(t, calls.Load(), "missing token must not send anonymous reports")
}

func TestVersionReportRedisBuildDefaultsAndCleanup(t *testing.T) {
	v := versionReportConfig(t)
	v.Set("runtime.version", "")
	v.Set("runtime.build_kind", "")
	t.Setenv("GATEWAY_VERSION_NAMESPACE", "Test_Namespace")
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	var httpCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	v.Set("gateway.admin_url", server.URL)
	v.Set("gateway.sync_token", "test-token")
	cleanup := newVersionTestEngine(t, v, client)
	var keys []string
	require.Eventually(t, func() bool {
		var err error
		keys, err = client.Keys(context.Background(), "tokenlive:gateway-versions:Test_Namespace:*").Result()
		require.NoError(t, err)
		return len(keys) == 1
	}, time.Second, 5*time.Millisecond)
	var node versionreport.Node
	payload, err := client.Get(context.Background(), keys[0]).Bytes()
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &node))
	require.Equal(t, "dev", node.Version)
	require.Equal(t, "dev", node.BuildKind)
	require.Equal(t, "Test_Namespace", node.Namespace)
	require.Equal(t, "tokenlive:gateway-versions:Test_Namespace:"+node.InstanceID, keys[0])
	require.Equal(t, 3*time.Minute, mr.TTL(keys[0]))
	cleanup()
	require.Zero(t, httpCalls.Load())
	require.NoError(t, client.Ping(context.Background()).Err())
	mr.FastForward(3 * time.Minute)
	require.False(t, mr.Exists(keys[0]), "stopped reporter records must expire")
}

func TestVersionReportEmbeddedHTTPOnlyIsDisabled(t *testing.T) {
	v := versionReportConfig(t)
	v.Set("gateway.config_source", "embedded")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/gateway/version" {
			calls.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("ADMIN_SERVER_URL", server.URL)
	t.Setenv("GATEWAY_SYNC_TOKEN", "test-token")
	cleanup := newVersionTestEngine(t, v, nil)
	require.Never(t, func() bool { return calls.Load() != 0 }, 100*time.Millisecond, 5*time.Millisecond)
	cleanup()
}

func TestVersionReportFailedEngineBuildDoesNotReport(t *testing.T) {
	v := versionReportConfig(t)
	v.Set("models", map[string]any{"invalid-model": map[string]any{}})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	v.Set("gateway.admin_url", server.URL)
	v.Set("gateway.sync_token", "test-token")
	_, _, _, err := NewGatewayEngine(v, &log.Logger{Logger: zap.NewNop()}, nil, nil, nil, nil, nil, nil, nil)
	require.Error(t, err)
	require.Never(t, func() bool { return calls.Load() != 0 }, 100*time.Millisecond, 5*time.Millisecond)
}
