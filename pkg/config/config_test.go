package config

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOsExpandEnvBehavior(t *testing.T) {
	// 测试 os.ExpandEnv 对带默认值语法的表现
	os.Setenv("TEST_VAR", "my_value")

	// 如果 TEST_VAR 存在，且使用 ${TEST_VAR:default}
	// os.ExpandEnv 会去查找名为 "TEST_VAR:default" 的环境变量，找不到就会置空（或者在某些 go 版本保留原本）
	res := os.ExpandEnv("${TEST_VAR:default}")
	t.Logf("Result of os.ExpandEnv with default: %q", res)
}

func customExpandEnv(s string) string {
	return os.Expand(s, func(k string) string {
		if parts := strings.SplitN(k, ":", 2); len(parts) == 2 {
			val, exists := os.LookupEnv(parts[0])
			if !exists || val == "" {
				return parts[1]
			}
			return val
		}
		return os.Getenv(k)
	})
}

func TestCustomExpandEnv(t *testing.T) {
	// 清理环境变量
	os.Unsetenv("DB_DRIVER")
	os.Unsetenv("DB_DSN")
	os.Unsetenv("REDIS_PASSWORD")

	// 1. 测试未设置环境变量时，返回默认值
	input := `
driver: ${DB_DRIVER:sqlite}
dsn: ${DB_DSN:storage/gateway.db?_busy_timeout=5000}
redis_pwd: ${REDIS_PASSWORD:}
otel: "${OTEL_ENDPOINT:192.168.68.6:4317}"
mongo: ${MONGO_URI:mongodb://root:123456@localhost:27017}
`
	expected := `
driver: sqlite
dsn: storage/gateway.db?_busy_timeout=5000
redis_pwd: 
otel: "192.168.68.6:4317"
mongo: mongodb://root:123456@localhost:27017
`
	assert.Equal(t, expected, customExpandEnv(input))

	// 2. 测试设置了环境变量时，使用环境变量的值
	os.Setenv("DB_DRIVER", "mysql")
	os.Setenv("DB_DSN", "root:secret@tcp(127.0.0.1:3306)/db")
	os.Setenv("REDIS_PASSWORD", "123456")
	os.Setenv("OTEL_ENDPOINT", "10.0.0.1:4317")
	os.Setenv("MONGO_URI", "mongodb://admin:admin@127.0.0.1:27017")

	expectedEnv := `
driver: mysql
dsn: root:secret@tcp(127.0.0.1:3306)/db
redis_pwd: 123456
otel: "10.0.0.1:4317"
mongo: mongodb://admin:admin@127.0.0.1:27017
`
	assert.Equal(t, expectedEnv, customExpandEnv(input))
}
