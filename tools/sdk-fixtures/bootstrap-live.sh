#!/bin/sh
set -eu

tool_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH='' cd -- "$tool_dir/../.." && pwd)
"$tool_dir/bootstrap.sh"
exec "$root/scripts/check" sdk
