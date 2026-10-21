#!/usr/bin/env bash
set -euo pipefail

ROOT=/srv/gmx
REPO="$ROOT/repo"
cd "$REPO"
export PYTHONPATH="$REPO:$REPO/src${PYTHONPATH:+:$PYTHONPATH}"
if [[ -r "$ROOT/collector.env" ]]; then
    set -a
    # This file is host-managed and mode 0600; the collector never echoes it.
    # shellcheck disable=SC1091
    source "$ROOT/collector.env"
    set +a
fi
exec "$ROOT/venv/bin/python" scripts/collect_daily_snapshot.py --output-dir "$ROOT/user_data"
