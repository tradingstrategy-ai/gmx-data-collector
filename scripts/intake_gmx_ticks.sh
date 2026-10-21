#!/usr/bin/env bash
# Cron entry point; credentials are gh's existing login, never collector .env.
set -euo pipefail
umask 0007
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 /absolute/python /canonical/gmx [release-tag|latest]" >&2
    exit 2
fi
REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"
PYTHONPATH="$REPO_ROOT/src" "$1" -m gmx_historical_data.tick_release intake \
    --root "$2" --release "${3:-latest}"
