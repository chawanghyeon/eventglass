#!/bin/sh
set -eu

tool_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$tool_dir/../.." && pwd)
go_bin="$root/.tools/go1.26.5/bin/go"
if [ ! -x "$go_bin" ]; then
  echo "Go 1.26.5 is required; run ./scripts/bootstrap first" >&2
  exit 1
fi

python3 -m venv "$tool_dir/.venv"
PIP_CACHE_DIR="$tool_dir/.pip-cache" "$tool_dir/.venv/bin/python" -m pip install --disable-pip-version-check -r "$tool_dir/python-app/requirements.lock"
npm --cache "$tool_dir/.npm-cache" --prefix "$tool_dir/node-app" ci
npm --cache "$tool_dir/.npm-cache" --prefix "$tool_dir/browser-app" ci
npm --prefix "$tool_dir/browser-app" run build
if [ "${CI:-}" = true ]; then
  PLAYWRIGHT_BROWSERS_PATH="$tool_dir/.playwright-browsers" \
    "$tool_dir/browser-app/node_modules/.bin/playwright" install --with-deps chromium
else
  PLAYWRIGHT_BROWSERS_PATH="$tool_dir/.playwright-browsers" \
    "$tool_dir/browser-app/node_modules/.bin/playwright" install chromium
fi
(
  cd "$tool_dir/go-app"
  GOCACHE="$tool_dir/.go-build-cache" GOMODCACHE="$tool_dir/.go-mod-cache" \
    "$go_bin" build -mod=readonly -o eventglass-go-fixture .
)
