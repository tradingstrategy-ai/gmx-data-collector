#!/usr/bin/env bash
# Download GMX data from the latest GitHub release.
#
# Usage:
#   ./scripts/download_gmx_data.sh                          # latest, full
#   ./scripts/download_gmx_data.sh --asset light            # latest, no futures/
#   ./scripts/download_gmx_data.sh --release data-2026-04-27
#   ./scripts/download_gmx_data.sh --output-dir ./mydata    # default: ./
#
# Requires: gh CLI authenticated (`gh auth login`).
set -euo pipefail

command -v gh >/dev/null 2>&1 || { echo "Error: gh CLI not found. Install from https://cli.github.com" >&2; exit 1; }

REPO="tradingstrategy-ai/gmx-data-collector"
ASSET="full"
RELEASE=""
OUTPUT_DIR="."

while [ $# -gt 0 ]; do
    case "$1" in
        --asset)        ASSET="$2";       shift 2 ;;
        --release)      RELEASE="$2";     shift 2 ;;
        --output-dir)   OUTPUT_DIR="$2";  shift 2 ;;
        -h|--help)
            sed -n '2,10p' "$0"
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [ "$ASSET" != "full" ] && [ "$ASSET" != "light" ]; then
    echo "Error: --asset must be 'full' or 'light'" >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"
[ -w "$OUTPUT_DIR" ] || { echo "Error: cannot write to ${OUTPUT_DIR}" >&2; exit 1; }

WORK_DIR=$(mktemp -d) || { echo "Error: failed to create temp directory" >&2; exit 1; }
trap 'rm -rf "$WORK_DIR"' EXIT

DOWNLOAD_ARGS=(--repo "$REPO" --pattern "gmx-${ASSET}.tar.gz" --dir "$WORK_DIR" --clobber)
if [ -n "$RELEASE" ]; then
    DOWNLOAD_ARGS+=("$RELEASE")
fi

echo "Downloading gmx-${ASSET}.tar.gz from ${RELEASE:-latest release}..."
gh release download "${DOWNLOAD_ARGS[@]}"

TARBALL="${WORK_DIR}/gmx-${ASSET}.tar.gz"
[ -f "$TARBALL" ] || { echo "Asset not found in release" >&2; exit 1; }

echo "Extracting into ${OUTPUT_DIR}/..."
tar -xzf "$TARBALL" -C "$OUTPUT_DIR"

COUNT=$(find "$OUTPUT_DIR/user_data/data/gmx" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "Done. ${COUNT} files extracted under ${OUTPUT_DIR}/user_data/data/gmx/."
