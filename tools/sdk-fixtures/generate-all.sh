#!/bin/sh
set -eu

tool_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "$tool_dir/.venv/bin/python" -u "$tool_dir/generate.py" --all
