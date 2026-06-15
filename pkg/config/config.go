package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

func NewConfig(p string) *viper.Viper {
	loadDotEnv()
	envConf := os.Getenv("APP_CONF")
	if envConf == "" {
		envConf = p
	}
	fmt.Println("load conf file:", envConf)
	return getConfig(envConf)
}

func getConfig(path string) *viper.Viper {
	conf := viper.New()

	// 读取配置文件内容并展开环境变量（支持 ${VAR:default} 语法）
	data, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Errorf("read config file: %w", err))
	}
	expanded := expandEnv(string(data))

	conf.SetConfigType("yaml")
	if err := conf.ReadConfig(strings.NewReader(expanded)); err != nil {
		panic(err)
	}
	return conf
}

func expandEnv(s string) string {
	return os.Expand(s, func(k string) string {
		if parts := strings.SplitN(k, ":", 2); len(parts) == 2 {
			val := os.Getenv(parts[0])
			if val == "" {
				return parts[1]
			}
			return val
		}
		return os.Getenv(k)
	})
}

// loadDotEnv 递归向上查找并加载根目录下的 .env 文件写入到环境变量中
func loadDotEnv() {
	dir, err := os.Getwd()
	if err != nil {
		return
	}
	for {
		envPath := filepath.Join(dir, ".env")
		if _, err := os.Stat(envPath); err == nil {
			data, err := os.ReadFile(envPath)
			if err != nil {
				return
			}
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				parts := strings.SplitN(line, "=", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					val := strings.TrimSpace(parts[1])
					if (strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) ||
						(strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) {
						val = val[1 : len(val)-1]
					}
					if os.Getenv(key) == "" {
						_ = os.Setenv(key, val)
					}
				}
			}
			return
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
}
