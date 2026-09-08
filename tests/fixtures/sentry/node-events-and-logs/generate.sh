#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../../.." && pwd)
exec "$root/tools/sdk-fixtures/.venv/bin/python" "$root/tools/sdk-fixtures/generate.py" --case node-events-and-logs

