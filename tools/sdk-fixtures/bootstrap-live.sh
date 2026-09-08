#!/bin/sh
set -eu

tool_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
"$tool_dir/bootstrap.sh"
exec python3 "$tool_dir/live.py" --check-dependencies
