#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

assert_contains() {
  [[ "$1" == *"$2"* ]] || { echo "missing build argument: $2" >&2; exit 1; }
}

output="$(RELEASE_TAG=v9.8.7 BUILD_KIND=release make -n build)"
assert_contains "$output" "main.VERSION=v9.8.7"
assert_contains "$output" "main.BUILD_KIND=release"

output="$(env -u RELEASE_TAG -u BUILD_KIND make -n build)"
assert_contains "$output" "main.VERSION=dev"
assert_contains "$output" "main.BUILD_KIND=dev"

output="$(env -u BUILD_KIND RELEASE_TAG=v9.8.7 make -n build)"
assert_contains "$output" "main.VERSION=v9.8.7"
assert_contains "$output" "main.BUILD_KIND=dev"

# Image aliases may name an image, never the executable's version.
if RELEASE_TAG=latest make -n build >/dev/null 2>&1; then
  echo "latest was accepted as a runtime version" >&2
  exit 1
fi

# Execute the Dockerfile's actual Go build RUN against a fake compiler. This
# checks ARG scoping/expansion without needing a daemon or running a container.
python3 - <<'PY'
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

dockerfile = pathlib.Path("deploy/build/Dockerfile").read_text()
instructions = dockerfile.replace("\\\n", " ").splitlines()
for overrides, version, kind in (
    ({}, "dev", "dev"),
    ({"VERSION": "v9.8.7", "BUILD_KIND": "release"}, "v9.8.7", "release"),
    ({"RELEASE_TAG": "v9.8.6"}, "v9.8.6", "dev"),
    ({"VERSION": "latest"}, None, None),
):
    with tempfile.TemporaryDirectory(prefix="gateway-version-test-") as temporary:
        root = pathlib.Path(temporary)
        go = root / "go"
        go.write_text(
            f"#!{sys.executable}\nimport json,pathlib,sys\n"
            "pathlib.Path('go-args.json').write_text(json.dumps(sys.argv[1:]))\n"
        )
        go.chmod(0o755)
        env = {
            "PATH": f"{root}:/usr/bin:/bin",
            "APP_RELATIVE_PATH": "./cmd/server",
        }
        stage = 0
        command = None
        for line in instructions:
            if line.startswith("FROM "):
                stage += 1
            if stage != 1:
                continue
            if line.startswith("ARG "):
                key, _, default = line[4:].partition("=")
                default = re.sub(
                    r"\$\{(\w+)\}", lambda match: env.get(match[1], ""), default
                )
                env[key] = overrides.get(key, env.get(key, default))
            if line.startswith("RUN ") and "go build " in line:
                command = line[4:]
        assert command is not None, "Dockerfile has no executable Go build step"
        result = subprocess.run(
            ["sh", "-c", command], cwd=root, env=env, capture_output=True, text=True
        )
        if version is None:
            assert result.returncode != 0, "Dockerfile accepted latest as runtime version"
            assert not (root / "go-args.json").exists()
        else:
            assert result.returncode == 0, result.stderr
            args = json.loads((root / "go-args.json").read_text())
            flags = next(arg for arg in args if arg.startswith("-ldflags="))
            assert f"main.VERSION={version}" in flags, args
            assert f"main.BUILD_KIND={kind}" in flags, args

# Execute the release workflow's binary step as shell, with action expressions
# resolved to fixture values. Only the compiler is replaced.
workflow = pathlib.Path(".github/workflows/release.yml").read_text().splitlines()
start = next(i for i, line in enumerate(workflow) if line.strip() == "- name: Build binary")
end = next(i for i in range(start + 1, len(workflow)) if workflow[i].startswith("    - "))
step = workflow[start:end]
run = next(i for i, line in enumerate(step) if line.strip() == "run: |")
for version in ("v9.8.7", "latest"):
    values = {
        "env.APP_NAME": "tokenlive-gateway",
        "matrix.goos": "linux",
        "matrix.goarch": "amd64",
        "matrix.ext": "",
        "steps.version.outputs.VERSION": version,
    }
    def resolve(text):
        return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda match: values[match[1]], text)
    with tempfile.TemporaryDirectory(prefix="gateway-workflow-test-") as temporary:
        root = pathlib.Path(temporary)
        go = root / "go"
        go.write_text(
            f"#!{sys.executable}\nimport json,pathlib,sys\n"
            "pathlib.Path('go-args.json').write_text(json.dumps(sys.argv[1:]))\n"
        )
        go.chmod(0o755)
        env = {"PATH": f"{root}:/usr/bin:/bin"}
        for line in step[:run]:
            if line.startswith("        ") and ":" in line:
                key, value = line.strip().split(":", 1)
                env[key] = resolve(value.strip())
        command = resolve("\n".join(line[8:] for line in step[run + 1:]))
        result = subprocess.run(
            ["bash", "-e", "-c", command], cwd=root, env=env, capture_output=True, text=True
        )
        if version == "latest":
            assert result.returncode != 0, "Release workflow accepted latest as runtime version"
            assert not (root / "go-args.json").exists()
        else:
            assert result.returncode == 0, result.stderr
            args = json.loads((root / "go-args.json").read_text())
            flags = args[args.index("-ldflags") + 1]
            assert "main.VERSION=v9.8.7" in flags, args
            assert "main.BUILD_KIND=release" in flags, args
PY

echo "version build tests passed"
