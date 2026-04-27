# GitHub Releases Migration — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `data/daily-collection` git-tree storage with GitHub Releases. Daily data ships as release assets (`gmx-full.tar.gz`, `gmx-light.tar.gz`, `data_report.txt`), pruned after 14 days. Repo working tree stays code-only.

**Architecture:** One daily workflow (`release-data.yml`, 02:00 UTC) downloads the previous release, runs incremental collectors, packages two tarballs by category, creates `data-YYYY-MM-DD` release, prunes releases older than 14 days. Consumer-facing `download_gmx_data.sh` and a refactored `quickstart.py` use `gh release download` instead of `git clone`.

**Tech Stack:** GitHub Actions, `gh` CLI, `tar`, Python 3.12, `poetry`, `subprocess`, `rich`, existing collectors (`scripts/collect_daily_snapshot.py`, `gmx_historical_data.subsquid_volume`).

**Spec:** [`docs/superpowers/specs/2026-04-27-github-releases-migration-design.md`](../specs/2026-04-27-github-releases-migration-design.md)

---

## Chunk 1: Measure & Seed

Before any code changes — confirm the tarball-size assumption holds. If `gmx-full.tar.gz` exceeds 2 GB (GitHub asset cap), the workflow needs to split by timeframe, which fans out the rest of the plan.

### Task 1: Measure current branch contents

**Files:**
- Read-only inspection — no writes.

- [ ] **Step 1: Worktree the data branch into /tmp**

```bash
git fetch origin data/daily-collection
git worktree add /tmp/gmx-seed data/daily-collection
```

- [ ] **Step 2: Build candidate tarballs locally**

```bash
cd /tmp/gmx-seed
tar -czf /tmp/gmx-full.tar.gz user_data/data/gmx/
tar -czf /tmp/gmx-light.tar.gz \
    user_data/data/gmx/apy/ \
    user_data/data/gmx/snapshots/ \
    user_data/data/gmx/tickers/ \
    user_data/data/gmx/volumes/
```

- [ ] **Step 3: Record sizes**

```bash
ls -lh /tmp/gmx-full.tar.gz /tmp/gmx-light.tar.gz
du -sh /tmp/gmx-seed/user_data/data/gmx/*/
```

Expected: both well under 2 GB based on repo size of 8.7 GB (most of which is git pack history, not tree).

**Decision gate:**
- If `gmx-full.tar.gz` < 1.8 GB → proceed with this plan as written.
- If 1.8 GB ≤ size < 2 GB → still proceed but flag at next iteration as a near-term risk.
- If ≥ 2 GB → STOP. Pause and revise plan to split by timeframe (mirrors Hyperliquid `hl-1m.tar.gz`, `hl-1h.tar.gz`, etc.) before continuing.

- [ ] **Step 4: Clean up worktree (keep tarballs for Task 2)**

```bash
git worktree remove /tmp/gmx-seed
ls /tmp/gmx-full.tar.gz /tmp/gmx-light.tar.gz  # verify still present
```

No commit — this task only validates an assumption.

---

### Task 2: Create seed release (manual, one-time)

**Files:**
- Read-only on the repo. Creates a release on the GitHub side only.

**Pre-requisite:** Task 1 passed the decision gate.

- [ ] **Step 1: Re-create the worktree to grab `data_report.txt`**

```bash
git worktree add /tmp/gmx-seed data/daily-collection
cp /tmp/gmx-seed/data_report.txt /tmp/data_report.txt
git worktree remove /tmp/gmx-seed
```

- [ ] **Step 2: Verify gh auth**

```bash
gh auth status
```

Expected: authenticated to `github.com` with `repo` scope.

- [ ] **Step 3: Create the seed release**

Use today's UTC date for the tag.

```bash
DATE=$(date -u +%Y-%m-%d)
gh release create "data-${DATE}" \
    --repo tradingstrategy-ai/gmx-data-collector \
    --title "GMX Data ${DATE} (seed)" \
    --notes "Seed release. Migrated from data/daily-collection branch contents at HEAD." \
    /tmp/gmx-full.tar.gz /tmp/gmx-light.tar.gz /tmp/data_report.txt
```

Expected: `gh` prints the release URL.

- [ ] **Step 4: Verify the release**

```bash
gh release view "data-${DATE}" --repo tradingstrategy-ai/gmx-data-collector
gh release download "data-${DATE}" --repo tradingstrategy-ai/gmx-data-collector \
    --dir /tmp/gmx-verify --pattern "*"
ls -lh /tmp/gmx-verify/
```

Expected: three assets present, sizes match what was uploaded.

- [ ] **Step 5: Clean up**

```bash
rm -rf /tmp/gmx-verify /tmp/gmx-full.tar.gz /tmp/gmx-light.tar.gz /tmp/data_report.txt
```

**Manual review checkpoint:** show the release URL to user before proceeding to Task 3.

---

## Chunk 2: Consumer-facing code

Add the download script and refactor `quickstart.py`. The seed release from Task 2 is the test target.

### Task 3: Add `scripts/download_gmx_data.sh`

**Files:**
- Create: `scripts/download_gmx_data.sh`

- [ ] **Step 1: Create the script**

```bash
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
            sed -n '2,11p' "$0"
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
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

DOWNLOAD_ARGS=(--repo "$REPO" --pattern "gmx-${ASSET}.tar.gz" --dir "$TMPDIR" --clobber)
if [ -n "$RELEASE" ]; then
    DOWNLOAD_ARGS+=("$RELEASE")
fi

echo "Downloading gmx-${ASSET}.tar.gz from ${RELEASE:-latest release}..."
gh release download "${DOWNLOAD_ARGS[@]}"

TARBALL="${TMPDIR}/gmx-${ASSET}.tar.gz"
[ -f "$TARBALL" ] || { echo "Asset not found in release" >&2; exit 1; }

echo "Extracting into ${OUTPUT_DIR}/..."
tar -xzf "$TARBALL" -C "$OUTPUT_DIR"

COUNT=$(find "$OUTPUT_DIR/user_data/data/gmx" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "Done. ${COUNT} files extracted under ${OUTPUT_DIR}/user_data/data/gmx/."
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/download_gmx_data.sh
```

- [ ] **Step 3: Test against the seed release**

```bash
mkdir -p /tmp/gmx-dl-test
./scripts/download_gmx_data.sh --asset light --output-dir /tmp/gmx-dl-test
ls /tmp/gmx-dl-test/user_data/data/gmx/
```

Expected: directories `apy`, `snapshots`, `tickers`, `volumes` present (not `futures`).

- [ ] **Step 4: Test full asset**

```bash
rm -rf /tmp/gmx-dl-test
mkdir -p /tmp/gmx-dl-test
./scripts/download_gmx_data.sh --output-dir /tmp/gmx-dl-test
ls /tmp/gmx-dl-test/user_data/data/gmx/
```

Expected: `futures/` directory present in addition to the four above.

- [ ] **Step 5: Cleanup + commit**

```bash
rm -rf /tmp/gmx-dl-test
git add scripts/download_gmx_data.sh
git commit -m "feat(release-migration): add download_gmx_data.sh consumer script"
```

**Manual review checkpoint.**

---

### Task 4: Add tests for `quickstart.py` rewrite

**Files:**
- Create: `tests/test_quickstart.py`

Tests must exist BEFORE the rewrite so we know the new behaviour matches expectations.

- [ ] **Step 1: Create the test file with placeholder failing tests**

```python
"""Tests for quickstart release-based seeding."""

from __future__ import annotations

import shutil
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest
from rich.console import Console

from gmx_historical_data.quickstart import (
    DEFAULT_RELEASE_TAG,
    seed_from_release,
)


@pytest.fixture
def fake_release_tarball(tmp_path: Path) -> Path:
    """Build a fake gmx-light.tar.gz under tmp_path/dl/."""
    src = tmp_path / "src" / "user_data" / "data" / "gmx"
    (src / "snapshots").mkdir(parents=True)
    (src / "snapshots" / "2026-04-26.parquet").write_bytes(b"snap")
    (src / "tickers").mkdir()
    (src / "tickers" / "2026-04-26.parquet").write_bytes(b"tick")

    dl_dir = tmp_path / "dl"
    dl_dir.mkdir()
    tarball = dl_dir / "gmx-light.tar.gz"
    with tarfile.open(tarball, "w:gz") as tf:
        tf.add(tmp_path / "src" / "user_data", arcname="user_data")
    return tarball


def _fake_gh_download(dl_dir: Path, src_tarball: Path):
    """Replace gh release download with a copy from src_tarball."""
    def runner(cmd, *args, **kwargs):
        if cmd[:3] == ["gh", "release", "download"]:
            shutil.copy2(src_tarball, dl_dir / src_tarball.name)
            return subprocess.CompletedProcess(cmd, 0, "", "")
        raise AssertionError(f"unexpected command: {cmd}")
    return runner


def test_seed_from_release_copies_missing_files(tmp_path, fake_release_tarball):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    with patch("gmx_historical_data.quickstart.subprocess.run",
               side_effect=_fake_gh_download(fake_release_tarball.parent, fake_release_tarball)):
        result = seed_from_release(output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True))

    assert result["copied"] == 2
    assert result["skipped"] == 0
    assert (output_dir / "data" / "gmx" / "snapshots" / "2026-04-26.parquet").exists()


def test_seed_from_release_skips_existing(tmp_path, fake_release_tarball):
    output_dir = tmp_path / "out"
    existing = output_dir / "data" / "gmx" / "snapshots" / "2026-04-26.parquet"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"original")

    with patch("gmx_historical_data.quickstart.subprocess.run",
               side_effect=_fake_gh_download(fake_release_tarball.parent, fake_release_tarball)):
        result = seed_from_release(output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True))

    assert result["skipped"] == 1
    assert result["copied"] == 1  # tickers file copied, snapshots file skipped
    assert existing.read_bytes() == b"original"  # not overwritten


def test_seed_from_release_handles_gh_failure(tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    def failing_runner(cmd, *args, **kwargs):
        raise subprocess.CalledProcessError(1, cmd, stderr="release not found")

    with patch("gmx_historical_data.quickstart.subprocess.run", side_effect=failing_runner):
        result = seed_from_release(output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True))

    assert "error" in result
    assert result["copied"] == 0
```

- [ ] **Step 2: Run tests, verify they fail with `ImportError`**

```bash
poetry run pytest tests/test_quickstart.py -v
```

Expected: ImportError or AttributeError on `seed_from_release` / `DEFAULT_RELEASE_TAG`.

No commit yet — implementation in next task.

---

### Task 5: Refactor `quickstart.py` to use releases

**Files:**
- Modify: `src/gmx_historical_data/quickstart.py`
- Modify: `src/gmx_historical_data/cli.py:54-56,1497,1499,1571`
- Modify: `scripts/collect_daily_snapshot.py:53-55,695-696,703`

- [ ] **Step 1: Rewrite `quickstart.py`**

Replace the entire file with this content:

```python
"""Quickstart seeding from the latest GitHub Release.

Shared helpers used by both ``scripts/collect_daily_snapshot.py`` and
``gmx_historical_data collect --quickstart``. Downloads ``gmx-full.tar.gz``
from a GitHub Release of this repo and merge-copies its ``user_data/`` tree
into a local destination. Merge-only means files that already exist locally
are skipped — never overwritten — so the operation is idempotent and
reversible (``rm -rf user_data/``).
"""

import shutil
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from rich.console import Console

#: GitHub repository serving the releases.
DEFAULT_REPO = "tradingstrategy-ai/gmx-data-collector"

#: Sentinel meaning "the latest release" — passed to ``gh release download``
#: as no ``--tag`` argument.
DEFAULT_RELEASE_TAG = "latest"

#: Asset name to download. ``full`` includes ``futures/`` feathers; ``light``
#: omits them. The seed flow uses full so freqtrade backtests work end-to-end.
DEFAULT_ASSET = "gmx-full.tar.gz"


def seed_from_release(
    output_dir: Path,
    tag: str,
    console: Console,
    asset: str = DEFAULT_ASSET,
    repo: str = DEFAULT_REPO,
) -> dict:
    """Seed ``output_dir`` from a GitHub Release asset.

    Downloads ``asset`` from ``repo``'s release ``tag`` (or the latest release
    if ``tag == DEFAULT_RELEASE_TAG``), extracts it to a temp directory, then
    merge-copies the contained ``user_data/`` tree into ``output_dir``.
    Existing local files are never overwritten; only missing files are copied.

    :param output_dir: Local destination root (e.g. ``./user_data``).
    :param tag: Release tag, or ``DEFAULT_RELEASE_TAG`` for the latest release.
    :param console: Rich console used for progress output.
    :param asset: Release asset filename (``gmx-full.tar.gz`` or
        ``gmx-light.tar.gz``).
    :param repo: GitHub ``owner/name`` to pull from.
    :return: Summary dict with keys ``copied``, ``skipped``, ``bytes``.
        On failure the dict also contains ``error``.
    """
    tmp = Path(tempfile.mkdtemp(prefix="gmx-quickstart-"))
    try:
        cmd = ["gh", "release", "download"]
        if tag != DEFAULT_RELEASE_TAG:
            cmd.append(tag)
        cmd += ["--repo", repo, "--pattern", asset, "--dir", str(tmp), "--clobber"]

        console.print(
            f"  Downloading [cyan]{asset}[/cyan] from "
            f"{repo}@{tag}..."
        )
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            console.print(f"  [yellow]Warning:[/yellow] gh release download failed: {stderr}")
            return {"copied": 0, "skipped": 0, "bytes": 0, "error": stderr}

        tarball = tmp / asset
        if not tarball.is_file():
            console.print(f"  [yellow]Warning:[/yellow] asset {asset} not found in release")
            return {"copied": 0, "skipped": 0, "bytes": 0, "error": "asset missing"}

        extract_root = tmp / "extracted"
        extract_root.mkdir()
        with tarfile.open(tarball, "r:gz") as tf:
            tf.extractall(extract_root)  # noqa: S202 — trusted source

        src_root = extract_root / "user_data"
        if not src_root.is_dir():
            console.print("  [yellow]Warning:[/yellow] tarball has no user_data/ root")
            return {"copied": 0, "skipped": 0, "bytes": 0, "error": "no user_data"}

        copied = 0
        skipped = 0
        total_bytes = 0
        for src_file in src_root.rglob("*"):
            if not src_file.is_file():
                continue
            rel = src_file.relative_to(src_root)
            dest = output_dir / rel
            if dest.exists():
                skipped += 1
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest)
            copied += 1
            total_bytes += src_file.stat().st_size
        return {"copied": copied, "skipped": skipped, "bytes": total_bytes}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def print_coverage_summary(output_dir: Path, console: Console) -> None:
    """Print a short coverage summary of the seeded ``user_data`` tree.

    Reads nothing heavier than feather metadata for one representative
    OHLCV file; everything else is inferred from parquet filenames
    (``YYYY-MM-DD.parquet``).

    :param output_dir: Local root, e.g. ``./user_data``.
    :param console: Rich console for output.
    """
    gmx_root = output_dir / "data" / "gmx"
    if not gmx_root.is_dir():
        console.print("  [yellow]No data/gmx/ tree found under output dir.[/yellow]")
        return

    today = datetime.now(UTC).date()
    console.print("\n[bold]Seeded coverage[/bold]")

    for label, subdir in (("Snapshots", "snapshots"), ("Tickers", "tickers"), ("APY", "apy")):
        files = (
            sorted((gmx_root / subdir).glob("*.parquet")) if (gmx_root / subdir).is_dir() else []
        )
        if not files:
            console.print(f"  {label:10s} (none)")
            continue
        dates = [f.stem for f in files]
        console.print(f"  {label:10s} {len(files)} files — {dates[0]} → {dates[-1]}")
        if label == "Snapshots":
            try:
                latest = datetime.strptime(dates[-1], "%Y-%m-%d").date()
                gap = (today - latest).days
                colour = "red" if gap > 1 else "green"
                console.print(
                    f"  {'Gap':10s} [bold {colour}]{gap} day(s) to today ({today})[/bold {colour}]"
                )
            except ValueError:
                pass

    futures_dir = gmx_root / "futures"
    if futures_dir.is_dir():
        feather_files = sorted(futures_dir.glob("*.feather"))
        console.print(f"  {'Futures':10s} {len(feather_files)} feather files")
        sample = futures_dir / "ETH_USDC_USDC-1d-futures.feather"
        if sample.exists():
            try:
                df = pd.read_feather(sample)
                if not df.empty and "date" in df.columns:
                    console.print(
                        f"  {'  ETH 1d':10s} {len(df)} rows — "
                        f"{df['date'].min()} → {df['date'].max()}"
                    )
            except Exception as exc:  # noqa: BLE001
                console.print(f"  [yellow]Could not read sample: {exc}[/yellow]")
    else:
        console.print(f"  {'Futures':10s} (none)")
```

- [ ] **Step 2: Run tests, verify they pass**

```bash
poetry run pytest tests/test_quickstart.py -v
```

Expected: 3 passed.

- [ ] **Step 3: Update `src/gmx_historical_data/cli.py`**

At line ~54, change the import block from:
```python
from gmx_historical_data.quickstart import (
    DEFAULT_BRANCH,
    print_coverage_summary,
    seed_from_branch,
)
```
to:
```python
from gmx_historical_data.quickstart import (
    DEFAULT_RELEASE_TAG,
    print_coverage_summary,
    seed_from_release,
)
```

At line ~1497, change the CLI option default:
```python
DEFAULT_RELEASE_TAG,
```
and update help text at line ~1499:
```python
help=f"Release tag to seed from (default: {DEFAULT_RELEASE_TAG} = most recent).",
```

At line ~1571, change call site:
```python
summary = seed_from_release(seed_dir, quickstart_ref, console)
```

- [ ] **Step 4: Update `scripts/collect_daily_snapshot.py`**

At line ~53-55, change import:
```python
from gmx_historical_data.quickstart import (
    DEFAULT_RELEASE_TAG,
    print_coverage_summary,
    seed_from_release,
)
```

At line ~695-696:
```python
default=DEFAULT_RELEASE_TAG,
help=f"Release tag to seed from (default: {DEFAULT_RELEASE_TAG} = most recent).",
```

At line ~703:
```python
summary = seed_from_release(args.output_dir, args.quickstart_ref, console)
```

- [ ] **Step 5: Verify nothing else references the old names**

```bash
grep -rn "DEFAULT_BRANCH\|seed_from_branch\|resolve_origin_url\|DEFAULT_REMOTE_URL" \
    --include='*.py' src/ scripts/ tests/
```

Expected: no matches outside of `quickstart.py` history (none in the rewritten file).

- [ ] **Step 6: Run full test suite**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/test_quickstart.py -v
poetry run pytest tests/ -x --ignore=tests/integration -q 2>&1 | tail -30
```

Expected: quickstart tests pass; rest of suite unchanged (no new failures introduced).

- [ ] **Step 7: Commit**

```bash
git add src/gmx_historical_data/quickstart.py \
        src/gmx_historical_data/cli.py \
        scripts/collect_daily_snapshot.py \
        tests/test_quickstart.py
git commit -m "refactor(quickstart): seed from GitHub Releases instead of git branch"
```

**Manual review checkpoint.**

---

## Chunk 3: Workflow & cutover

### Task 6: Add `release-data.yml` workflow (dispatch-only initially)

**Files:**
- Create: `.github/workflows/release-data.yml`

- [ ] **Step 1: Create the workflow file**

```yaml
name: Release GMX Daily Data

# IMPORTANT: starts as workflow_dispatch only. The schedule trigger is enabled
# in Task 8 after a successful manual dispatch verifies the pipeline end-to-end.

on:
  workflow_dispatch:
    inputs:
      retention_days:
        description: 'Delete releases older than this many days (default: 14).'
        required: false
        default: '14'
  # schedule:
  #   - cron: '0 2 * * *'  # 02:00 UTC daily — UNCOMMENTED IN TASK 8

permissions:
  contents: write

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

jobs:
  release:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
      - name: Checkout code from master
        uses: actions/checkout@v4
        with:
          ref: master

      - name: Free disk space
        run: |
          sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc /opt/hostedtoolcache/CodeQL
          df -h

      - name: Set up uv
        uses: astral-sh/setup-uv@v5
        with:
          python-version: '3.12'
          cache-dependency-glob: 'pyproject.toml'

      - name: Install Python dependencies
        run: |
          uv pip install --system pandas pyarrow polars rich requests eth-utils "web3-ethereum-defi>=1.1"

      - name: Restore previous release data
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          mkdir -p user_data
          TMPDIR=$(mktemp -d)
          echo "Downloading gmx-full.tar.gz from latest release (if any)..."
          if gh release download \
              --repo ${{ github.repository }} \
              --pattern "gmx-full.tar.gz" \
              --dir "${TMPDIR}" \
              --clobber 2>/dev/null; then
            tar -xzf "${TMPDIR}/gmx-full.tar.gz" -C .
            COUNT=$(find user_data -type f 2>/dev/null | wc -l | tr -d ' ')
            echo "Restored ${COUNT} files from previous release."
          else
            echo "No previous release found — starting fresh."
          fi
          rm -rf "${TMPDIR}"

      - name: Collect daily snapshot
        env:
          JSON_RPC_ARBITRUM: ${{ secrets.JSON_RPC_ARBITRUM }}
          HYPERSYNC_API_TOKEN: ${{ secrets.HYPERSYNC_API_TOKEN }}
        run: |
          PYTHONPATH=src python scripts/collect_daily_snapshot.py --output-dir ./user_data

      - name: Collect 24h volume snapshot
        run: |
          PYTHONPATH=src python - <<'EOF'
          import sys
          from datetime import datetime, UTC
          from pathlib import Path
          from gmx_historical_data.subsquid_volume import fetch_daily_volumes
          import pandas as pd

          date_str = datetime.now(UTC).strftime("%Y-%m-%d")
          volumes_dir = Path("./user_data/data/gmx/volumes")
          volumes_dir.mkdir(parents=True, exist_ok=True)

          print(f"Fetching 24h volumes for {date_str}...")
          volumes = fetch_daily_volumes(chain="arbitrum")
          if not volumes:
              print("No volume data returned.")
              sys.exit(0)

          rows = [{"date": date_str, "market_address": addr, "volume_usd": str(vol)} for addr, vol in volumes.items()]
          df = pd.DataFrame(rows)
          out = volumes_dir / f"{date_str}.parquet"
          df.to_parquet(out, index=False)
          print(f"Saved {len(df)} markets -> {out}")
          EOF

      - name: Show report in job summary
        if: always()
        run: |
          if [ -f data_report.txt ]; then
            echo '```' >> $GITHUB_STEP_SUMMARY
            cat data_report.txt >> $GITHUB_STEP_SUMMARY
            echo '```' >> $GITHUB_STEP_SUMMARY
          else
            echo "No report generated" >> $GITHUB_STEP_SUMMARY
          fi

      - name: Package tarballs
        run: |
          set -e
          if [ ! -d user_data/data/gmx ]; then
            echo "user_data/data/gmx/ missing after collection — aborting" >&2
            exit 1
          fi

          tar -czf /tmp/gmx-full.tar.gz user_data/data/gmx/
          tar -czf /tmp/gmx-light.tar.gz \
              user_data/data/gmx/apy/ \
              user_data/data/gmx/snapshots/ \
              user_data/data/gmx/tickers/ \
              user_data/data/gmx/volumes/

          ls -lh /tmp/gmx-*.tar.gz

          # Asset size guardrails
          for tarball in /tmp/gmx-full.tar.gz /tmp/gmx-light.tar.gz; do
            size=$(stat -c %s "$tarball")
            limit=$((2 * 1024 * 1024 * 1024))  # 2 GB
            if [ "$size" -ge "$limit" ]; then
              echo "ERROR: $tarball is ${size} bytes — exceeds 2 GB GitHub asset cap" >&2
              exit 1
            fi
          done

      - name: Create GitHub Release
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          DATE=$(date -u +%Y-%m-%d)
          TAG="data-${DATE}"
          COUNT=$(find user_data/data/gmx -type f 2>/dev/null | wc -l | tr -d ' ')

          # Idempotent: if today's release already exists (re-run), delete it first
          gh release delete "${TAG}" --repo ${{ github.repository }} --yes 2>/dev/null || true
          git push origin --delete "${TAG}" 2>/dev/null || true

          NOTES_FILE=$(mktemp)
          cat > "$NOTES_FILE" <<NOTES
          Daily automated GMX data snapshot.

          Contents (${COUNT} files total):
          - \`gmx-full.tar.gz\` — apy/, snapshots/, tickers/, volumes/, futures/
          - \`gmx-light.tar.gz\` — apy/, snapshots/, tickers/, volumes/ (no futures/)
          - \`data_report.txt\` — collection summary

          Consume with \`scripts/download_gmx_data.sh\` (see repo README).
          NOTES

          ASSETS=(/tmp/gmx-full.tar.gz /tmp/gmx-light.tar.gz)
          if [ -f data_report.txt ]; then
            ASSETS+=(data_report.txt)
          fi

          gh release create "${TAG}" \
              --repo ${{ github.repository }} \
              --title "GMX Data ${DATE}" \
              --notes-file "$NOTES_FILE" \
              --latest \
              "${ASSETS[@]}"

          rm -f "$NOTES_FILE"

          echo ""
          echo "=== Release assets ==="
          gh release view "${TAG}" --repo ${{ github.repository }} \
              --json assets --jq '.assets[] | "\(.name)  \(.size / 1024 / 1024 | floor)MB"'

      - name: Prune old releases
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          RETENTION_DAYS="${{ github.event.inputs.retention_days || 14 }}"
          CUTOFF=$(date -u -d "${RETENTION_DAYS} days ago" +%Y-%m-%d)
          echo "Pruning data-* releases with tag date older than ${CUTOFF}..."

          # List releases, filter data-YYYY-MM-DD tags, drop ones older than cutoff
          gh release list --repo ${{ github.repository }} --limit 100 \
              --json tagName --jq '.[].tagName' | \
              grep -E '^data-[0-9]{4}-[0-9]{2}-[0-9]{2}$' | \
              while read -r TAG; do
                  TAG_DATE="${TAG#data-}"
                  if [ "$TAG_DATE" \< "$CUTOFF" ]; then
                      echo "  Deleting ${TAG} (date: ${TAG_DATE})"
                      gh release delete "${TAG}" --repo ${{ github.repository }} --yes --cleanup-tag
                  fi
              done
          echo "Prune complete."
```

- [ ] **Step 2: Lint check (no syntax errors)**

```bash
# Local actionlint if available, otherwise rely on GitHub's own validation
which actionlint && actionlint .github/workflows/release-data.yml || echo "actionlint not installed — skipping local lint"
```

- [ ] **Step 3: Commit (workflow file only — schedule still commented out)**

```bash
git add .github/workflows/release-data.yml
git commit -m "feat(release-migration): add release-data.yml workflow (dispatch-only)"
```

**Manual review checkpoint** before running.

---

### Task 7: Manual dispatch test of `release-data.yml`

**Files:** none modified.

- [ ] **Step 1: Push the branch + open PR**

The commits from Tasks 3-6 should be on a feature branch. Push and open a PR for review before merging. Wait for user approval.

```bash
git push -u origin feat/github-releases-migration
gh pr create --title "feat: migrate from data branch to GitHub Releases" \
             --body "Closes #11. See docs/superpowers/specs/2026-04-27-github-releases-migration-design.md."
```

- [ ] **Step 2: Once merged to master, dispatch the workflow**

```bash
gh workflow run release-data.yml --repo tradingstrategy-ai/gmx-data-collector
```

- [ ] **Step 3: Wait + monitor**

```bash
sleep 30
RUN_ID=$(gh run list --workflow=release-data.yml --repo tradingstrategy-ai/gmx-data-collector \
    --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN_ID" --repo tradingstrategy-ai/gmx-data-collector
```

Expected: green status. If failed, fetch logs:
```bash
gh run view "$RUN_ID" --repo tradingstrategy-ai/gmx-data-collector --log-failed
```

- [ ] **Step 4: Verify the new release**

```bash
TODAY=$(date -u +%Y-%m-%d)
gh release view "data-${TODAY}" --repo tradingstrategy-ai/gmx-data-collector
```

Expected: the release is marked `latest`, has 2-3 assets, sizes match the seed release ± 1 day's worth of growth.

- [ ] **Step 5: End-to-end consumer test**

```bash
mkdir -p /tmp/gmx-e2e
./scripts/download_gmx_data.sh --output-dir /tmp/gmx-e2e
ls /tmp/gmx-e2e/user_data/data/gmx/
diff <(find /tmp/gmx-e2e/user_data/data/gmx -type f | sort) <(echo expected file list...)
rm -rf /tmp/gmx-e2e
```

Expected: directory tree matches what the prior `data/daily-collection` HEAD contained (plus the new day's files).

**Manual review checkpoint.** Don't proceed to Task 8 until consumer test is green.

---

### Task 8: Enable schedule + disable old workflow schedules

**Files:**
- Modify: `.github/workflows/release-data.yml` (uncomment schedule)
- Modify: `.github/workflows/collect-gmx-data.yml` (remove schedule block, add deprecation header)
- Modify: `.github/workflows/collect-volume.yml` (remove schedule block, add deprecation header)

- [ ] **Step 1: Enable schedule on `release-data.yml`**

In `.github/workflows/release-data.yml`, change:
```yaml
  # schedule:
  #   - cron: '0 2 * * *'  # 02:00 UTC daily — UNCOMMENTED IN TASK 8
```
to:
```yaml
  schedule:
    - cron: '0 2 * * *'  # 02:00 UTC daily
```

- [ ] **Step 2: Disable schedule on `collect-gmx-data.yml`**

Change the top of the file from:
```yaml
name: Daily GMX Data Collection

on:
  schedule:
    - cron: '0 2 * * *'  # 02:00 UTC daily (after GMX daily candle close)
  workflow_dispatch:
```
to:
```yaml
name: Daily GMX Data Collection (DEPRECATED — see release-data.yml)

# DEPRECATED 2026-04-27: replaced by release-data.yml which ships data via
# GitHub Releases instead of pushing to the data/daily-collection branch.
# Kept as workflow_dispatch-only for emergency rollback. Do not re-enable
# the schedule unless release-data.yml is broken.

on:
  workflow_dispatch:
```

- [ ] **Step 3: Disable schedule on `collect-volume.yml`**

Same treatment:
```yaml
name: Daily GMX Volume Collection (DEPRECATED — see release-data.yml)

# DEPRECATED 2026-04-27: replaced by release-data.yml. Kept dispatch-only
# for emergency rollback.

on:
  workflow_dispatch:
```

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/release-data.yml \
        .github/workflows/collect-gmx-data.yml \
        .github/workflows/collect-volume.yml
git commit -m "feat(release-migration): enable release-data.yml schedule, deprecate old daily workflows"
```

**Manual review checkpoint.** After merge, the next 02:00 UTC will be the first scheduled run.

---

## Chunk 4: Documentation & verification

### Task 9: Update README

**Files:**
- Modify: `README.md` (section about data download / `data/daily-collection`)

- [ ] **Step 1: Locate the existing data section**

```bash
grep -n "data/daily-collection" README.md
```

- [ ] **Step 2: Replace the branch-based instructions with release-based**

Add a section near the top of the data-consumption docs:

```markdown
## Downloading GMX historical data

Daily snapshots are published as **GitHub Releases**. Use the helper script:

\`\`\`bash
# Latest, full snapshot (apy, snapshots, tickers, volumes, futures feathers)
./scripts/download_gmx_data.sh

# Latest, light snapshot (skip futures/ feathers)
./scripts/download_gmx_data.sh --asset light

# Specific historical release
./scripts/download_gmx_data.sh --release data-2026-04-27
\`\`\`

Requires \`gh\` CLI authenticated (\`gh auth login\`). Releases are kept for 14
days; older releases are pruned.

> **Note (2026-04-27):** The previous \`data/daily-collection\` branch is
> deprecated. It will not receive further updates. New consumers should use the
> Releases path above.
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(release-migration): point downloads at GitHub Releases instead of data branch"
```

**Manual review checkpoint.**

---

### Task 10: Three-day parity check

**Files:** none modified — verification only.

After Task 8's merge, the new workflow runs daily on schedule. The old workflows are dispatch-only. Verify the new pipeline holds for 3 days.

- [ ] **Day 1**

```bash
gh release view "data-$(date -u +%Y-%m-%d)" --repo tradingstrategy-ai/gmx-data-collector
```

Expected: release exists, `latest` flag, 2-3 assets, sane sizes.

- [ ] **Day 2 & 3**

Repeat the check. Sizes should grow by roughly one day's worth (cumulative feathers extend).

- [ ] **End-of-window action**

After 3 green days, the migration is considered stable. Update `MEMORY.md`:

```markdown
## GitHub Releases Data Pipeline (2026-04-27)
- Daily data shipped via Releases: tag `data-YYYY-MM-DD`, assets `gmx-full.tar.gz` + `gmx-light.tar.gz` + `data_report.txt`.
- Workflow: `.github/workflows/release-data.yml` at 02:00 UTC, 14-day retention.
- Consumer entry points: `scripts/download_gmx_data.sh`, `quickstart.seed_from_release()`.
- `data/daily-collection` branch deprecated 2026-04-27 — kept readable but no new pushes.
- Old workflows kept as `workflow_dispatch` for emergency rollback only.
```

- [ ] **Open follow-up issue**

Create an issue for the deferred Oracle-server-setup consumer migration:

```bash
gh issue create --repo tradingstrategy-ai/Oracle-server-setup \
    --title "feat: switch GMX consumer from data branch clone to gh release download" \
    --body "Mirror of tradingstrategy-ai/gmx-data-collector#11 — that repo now ships data via GitHub Releases. Update config/repos.yaml + Makefile clone path."
```

**Manual review checkpoint** — final sign-off on the migration.

---

## Remember

- Exact file paths always.
- Stop at decision gate in Task 1 if `gmx-full.tar.gz` ≥ 2 GB.
- Don't delete the `data/daily-collection` branch (undoable rule). It stays as historical reference.
- Don't delete old workflow files — they remain as `workflow_dispatch` rollback.
- Manual review checkpoints at every chunk boundary; do not skip.
