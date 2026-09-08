#!/bin/sh
set -eu

tool_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

python3 -m venv "$tool_dir/.venv"
PIP_CACHE_DIR="$tool_dir/.pip-cache" "$tool_dir/.venv/bin/python" -m pip install --disable-pip-version-check -r "$tool_dir/python-app/requirements.lock"
npm --cache "$tool_dir/.npm-cache" --prefix "$tool_dir/node-app" ci
(
  cd "$tool_dir/go-app"
  GOCACHE="$tool_dir/.go-build-cache" GOMODCACHE="$tool_dir/.go-mod-cache" \
    go build -mod=readonly -o eventglass-go-fixture .
)
