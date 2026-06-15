package log

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestNewLog_SplitLevel(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "log_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	mainLogPath := filepath.Join(tmpDir, "server.log")
	errLogPath := filepath.Join(tmpDir, "server.err.log")

	conf := viper.New()
	conf.Set("log.log_level", "info")
	conf.Set("log.mode", "file")
	conf.Set("log.log_file_name", mainLogPath)
	conf.Set("log.err_file_name", errLogPath)
	conf.Set("log.max_size", 1)
	conf.Set("log.max_backups", 5)
	conf.Set("log.max_age", 1)
	conf.Set("log.compress", false)

	logger := NewLog(conf)
	if logger == nil {
		t.Fatal("failed to initialize logger")
	}
	defer func() {
		_ = logger.Sync()
	}()

	logger.Debug("this-is-debug")
	logger.Info("this-is-info")
	logger.Warn("this-is-warn")
	logger.Error("this-is-error")

	// 确保内容已刷盘
	_ = logger.Sync()

	// 读取主日志文件
	mainFile, err := os.Open(mainLogPath)
	if err != nil {
		t.Fatalf("failed to open main log: %v", err)
	}
	defer mainFile.Close()
	mainBytes, err := io.ReadAll(mainFile)
	if err != nil {
		t.Fatalf("failed to read main log: %v", err)
	}
	mainContent := string(mainBytes)

	// 读取错误日志文件
	errFile, err := os.Open(errLogPath)
	if err != nil {
		t.Fatalf("failed to open err log: %v", err)
	}
	defer errFile.Close()
	errBytes, err := io.ReadAll(errFile)
	if err != nil {
		t.Fatalf("failed to read err log: %v", err)
	}
	errContent := string(errBytes)

	// 验证过滤逻辑
	// 1. debug 日志因为级别是 info，在两个文件里都不应该出现
	if strings.Contains(mainContent, "this-is-debug") || strings.Contains(errContent, "this-is-debug") {
		t.Error("debug logs should not be written as log level is info")
	}

	// 2. info 日志只应在 main.log 出现，不应在 err.log 出现
	if !strings.Contains(mainContent, "this-is-info") {
		t.Error("info logs should be present in main log")
	}
	if strings.Contains(errContent, "this-is-info") {
		t.Error("info logs should not be present in error log")
	}

	// 3. warn/error 日志在两个文件中都应该出现
	if !strings.Contains(mainContent, "this-is-warn") || !strings.Contains(mainContent, "this-is-error") {
		t.Error("warn and error logs should be present in main log")
	}
	if !strings.Contains(errContent, "this-is-warn") || !strings.Contains(errContent, "this-is-error") {
		t.Error("warn and error logs should be present in error log")
	}

	// 4. 验证不含终端 ANSI 颜色转义符
	if strings.Contains(mainContent, "\x1b") || strings.Contains(errContent, "\x1b") {
		t.Error("logs written to files should not contain ANSI color escape codes")
	}
	if strings.Contains(mainContent, "[3") || strings.Contains(errContent, "[3") {
		t.Error("logs written to files should not contain ANSI level color prefix")
	}
}
