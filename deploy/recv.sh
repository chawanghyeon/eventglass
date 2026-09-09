#!/bin/sh
set -eu

[ "$(id -u)" -eq 0 ] || exit 1
exec 9>/run/lock/eventglass-deploy.lock
flock -w 120 9
exec /usr/bin/python3 /usr/local/lib/eventglass-deploy/receiver.py "${1:-all}"
