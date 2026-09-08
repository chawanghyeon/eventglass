#!/bin/sh
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
if [ "$#" -ne 0 ]; then
  echo "usage: EVENTGLASS_BIN=/optional/path $0" >&2
  exit 2
fi
cd "$root"
output=$(mktemp "${TMPDIR:-/tmp}/eventglass-sdk-live.XXXXXX")
trap 'rm -f "$output"' EXIT HUP INT TERM
set +e
cargo test --locked --test sdk_live real_sdks_reach_running_eventglass -- --ignored --exact --nocapture >"$output" 2>&1
status=$?
set -e
cat "$output"
if [ "$status" -ne 0 ]; then
  exit "$status"
fi
if ! grep -Fq "test result: ok. 1 passed; 0 failed; 0 ignored;" "$output"; then
  echo "sdk live gate failed closed: exact ignored test did not run once" >&2
  exit 1
fi
