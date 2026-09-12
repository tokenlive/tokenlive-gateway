package main

import (
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestVersionCLIExitsBeforeLoadingConfiguration(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestVersionCLIHelperProcess$")
	cmd.Dir = t.TempDir()
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"TOKENLIVE_VERSION_TEST_HELPER=1",
		"APP_CONF=" + filepath.Join(cmd.Dir, "must-not-load.yml"),
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("-version failed instead of exiting before config: %v\n%s", err, output)
	}
	if strings.TrimSpace(string(output)) != "tokenlive-gateway dev (dev)" {
		t.Fatalf("unexpected version output: %q", output)
	}
}

func TestVersionCLIHelperProcess(t *testing.T) {
	if os.Getenv("TOKENLIVE_VERSION_TEST_HELPER") != "1" {
		return
	}
	if version := os.Getenv("TOKENLIVE_VERSION_TEST_VERSION"); version != "" {
		VERSION = version
		BUILD_KIND = "release"
	}
	flag.CommandLine = flag.NewFlagSet("tokenlive-gateway", flag.ExitOnError)
	os.Args = []string{"tokenlive-gateway", "-version", "-conf", "/configuration-must-not-be-loaded.yml"}
	main()
	os.Exit(0)
}

func TestVersionRuntimeIdentity(t *testing.T) {
	oldVersion, oldKind := VERSION, BUILD_KIND
	defer func() { VERSION, BUILD_KIND = oldVersion, oldKind }()
	VERSION, BUILD_KIND = "v1.2.3", "release"
	v := viper.New()
	v.Set("runtime.version", "configured-value-must-not-override-binary")
	v.Set("runtime.build_kind", "dev")
	setRuntimeIdentity(v)
	if v.GetString("runtime.version") != "v1.2.3" || v.GetString("runtime.build_kind") != "release" {
		t.Fatal("runtime identity did not use binary build metadata")
	}
}

func TestVersionCLIReportsReleaseBuildIdentity(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestVersionCLIHelperProcess$")
	cmd.Dir = t.TempDir()
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"TOKENLIVE_VERSION_TEST_HELPER=1",
		"TOKENLIVE_VERSION_TEST_VERSION=v1.2.3",
		"APP_CONF=" + filepath.Join(cmd.Dir, "must-not-load.yml"),
	}
	output, err := cmd.CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "tokenlive-gateway v1.2.3 (release)" {
		t.Fatalf("wrong release build identity: %v\n%s", err, output)
	}
}
