#!/usr/bin/env bash
set -euo pipefail

ROOT="${GMX_ROOT:-/srv/gmx}"
DATA_ROOT="$ROOT/user_data/data/gmx"
AREAS=(futures snapshots tickers apy volumes ticks tick_volume checkpoints)

for area in "${AREAS[@]}"; do
    source="$ROOT/$area"
    target="$DATA_ROOT/$area"
    if [[ ! -d "$source" ]]; then
        printf 'Required existing GMX data directory is missing: %s\n' "$source" >&2
        exit 1
    fi
    if [[ -L "$target" ]]; then
        if [[ "$(readlink -f -- "$target")" == "$(readlink -f -- "$source")" ]]; then
            continue
        fi
        printf 'Refusing to replace a symlink with a different target: %s\n' "$target" >&2
        exit 1
    fi
    if [[ -e "$target" ]]; then
        if [[ ! -d "$target" || -n "$(find "$target" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
            printf 'Refusing to replace a non-empty collector path: %s\n' "$target" >&2
            exit 1
        fi
        rmdir -- "$target"
    fi
    mkdir -p -- "$(dirname -- "$target")"
    ln -s -- "$source" "$target"
done
