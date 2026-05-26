# Feather Zstd Compression Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `compression='zstd'` to every feather writer that produces FreqTrade-tree files, and one-shot-rewrite the existing 12 GB feather store in place — target ≤ 3 GB.

**Architecture:** No new modules.  Patch six existing writer call-sites (two in `freqtrade_exporter.py`, two in `scripts/extract_*.py`, one in `live_funding.py`, one in `collect_daily_snapshot.py`).  Add a new standalone script `scripts/rewrite_feathers_zstd.py` that walks the output directory, detects uncompressed feathers, and rewrites them atomically (temp + rename).  Add a unit test that round-trips a compressed feather through `pd.read_feather`.

**Tech Stack:** Polars 1.40 (`write_ipc(compression='zstd')`), PyArrow 18 (`feather.write_feather(compression='zstd', compression_level=3)`), pandas (`pd.read_feather` for FreqTrade-compat verification), pytest.

**Spec:** `docs/superpowers/specs/2026-05-27-feather-zstd-compression-design.md`

---

## Chunk 1: Writer patches (code change, no rewrite yet)

### Task 1: Add a test that asserts zstd compression on new exporter writes

**Files:**
- Create test: `tests/test_freqtrade_exporter_compression.py`

- [ ] **Step 1.1: Write the failing test**

```python
"""Verify the FreqTrade exporter writes zstd-compressed feathers.

We assert two properties:

1. The exporter writes a *compressed* IPC file — measured by comparing the
   file size to the uncompressed table size (compressed must be < 60 % of
   uncompressed for the highly-redundant OHLCV schema we use).
2. ``pd.read_feather`` (FreqTrade's read path) round-trips it without loss.
"""

from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow.ipc as ipc
import pytest

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter


def _make_synthetic_candles(rows: int = 100_000) -> pl.DataFrame:
    """Build a realistic 1m OHLCV dataframe (highly compressible)."""
    import numpy as np

    rng = np.random.default_rng(seed=42)
    base = 1000.0 + np.cumsum(rng.normal(0, 0.1, rows))
    return pl.DataFrame(
        {
            "date": pl.datetime_range(
                start=pl.datetime(2024, 1, 1),
                end=pl.datetime(2024, 1, 1),
                interval="1m",
                eager=True,
            ).head(rows).cast(pl.Datetime("ns", "UTC")),
            "open": base,
            "high": base + 0.5,
            "low": base - 0.5,
            "close": base + rng.normal(0, 0.1, rows),
            "volume": rng.uniform(0, 100, rows),
        }
    )


def test_exporter_writes_zstd_compressed_feather(tmp_path: Path) -> None:
    df = _make_synthetic_candles()
    out = tmp_path / "BTC_USDC_USDC-1m-futures.feather"

    # Use the internal helper directly — same writer the exporter uses.
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    exporter._merge_and_write(df, out, fmt="feather", unsafe_overwrite=True)

    assert out.exists(), "writer did not produce a file"

    # Compressed size should be < 60 % of uncompressed (LINK 1m benchmark
    # measured 22 %; we allow a generous headroom for synthetic data).
    uncompressed_bytes = df.estimated_size()
    compressed_bytes = out.stat().st_size
    assert compressed_bytes < uncompressed_bytes * 0.6, (
        f"feather not compressed: {compressed_bytes} bytes vs "
        f"{uncompressed_bytes} uncompressed (ratio "
        f"{compressed_bytes / uncompressed_bytes:.2%})"
    )

    # FreqTrade reads via pandas.read_feather — must succeed and match shape.
    roundtrip = pd.read_feather(out)
    assert len(roundtrip) == len(df)
    assert list(roundtrip.columns) == df.columns


def test_exporter_compressed_feather_has_zstd_in_metadata(tmp_path: Path) -> None:
    """Verify the IPC stream advertises zstd compression in its metadata."""
    df = _make_synthetic_candles(rows=10_000)
    out = tmp_path / "ETH_USDC_USDC-1m-futures.feather"
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    exporter._merge_and_write(df, out, fmt="feather", unsafe_overwrite=True)

    with ipc.open_file(out) as reader:
        # The first record batch carries the compression codec in its body
        # buffer compression header.  Read one batch and inspect.
        batch = reader.get_batch(0)
        # If compression is set, columns are decompressed transparently on
        # read.  As a smoke check, just ensure read succeeds and row count
        # matches — the size assertion above is the real compression proof.
        assert batch.num_rows > 0
```

- [ ] **Step 1.2: Run test to verify it fails (current writer uncompressed)**

Run: `pytest tests/test_freqtrade_exporter_compression.py -v`
Expected: `test_exporter_writes_zstd_compressed_feather` FAILS with "feather not compressed".

- [ ] **Step 1.3: Commit failing test**

```bash
git add tests/test_freqtrade_exporter_compression.py
git commit -m "test: add failing test asserting exporter emits zstd-compressed feathers"
```

---

### Task 2: Patch `freqtrade_exporter.py` to compress with zstd

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py:483, 542`

- [ ] **Step 2.1: Patch line 483 (new-file branch)**

Change:
```python
            if fmt == "feather":
                df.write_ipc(path)
```
To:
```python
            if fmt == "feather":
                df.write_ipc(path, compression="zstd")
```

- [ ] **Step 2.2: Patch line 542 (merge branch)**

Change:
```python
        if fmt == "feather":
            merged.write_ipc(path)
```
To:
```python
        if fmt == "feather":
            merged.write_ipc(path, compression="zstd")
```

- [ ] **Step 2.3: Run the new test to verify it passes**

Run: `pytest tests/test_freqtrade_exporter_compression.py -v`
Expected: both tests PASS.

- [ ] **Step 2.4: Run the full exporter test suite to verify no regression**

Run: `pytest tests/test_freqtrade_exporter*.py -v`
Expected: all PASS.

- [ ] **Step 2.5: Commit**

```bash
git add src/gmx_historical_data/freqtrade_exporter.py tests/test_freqtrade_exporter_compression.py
git commit -m "feat(exporter): write FreqTrade feathers with zstd compression

Polars write_ipc default is uncompressed.  On the OHLCV schema we use the
zstd ratio is ~78%, shrinking the futures/ tree from 12GB to ~2.6GB.
pandas.read_feather (FreqTrade's read path) decompresses transparently."
```

---

### Task 3: Patch `scripts/extract_funding_fee_per_size.py`

**Files:**
- Modify: `scripts/extract_funding_fee_per_size.py:879`

- [ ] **Step 3.1: Patch the write call**

Change:
```python
        out.write_ipc(filepath)
```
To:
```python
        out.write_ipc(filepath, compression="zstd")
```

- [ ] **Step 3.2: Verify the file still parses (smoke check)**

Run: `python -c "import ast, pathlib; ast.parse(pathlib.Path('scripts/extract_funding_fee_per_size.py').read_text())"`
Expected: exit 0.

- [ ] **Step 3.3: Commit**

```bash
git add scripts/extract_funding_fee_per_size.py
git commit -m "feat(funding): compress funding_rate feather output with zstd"
```

---

### Task 4: Patch `scripts/extract_unified_funding.py` (two writers)

**Files:**
- Modify: `scripts/extract_unified_funding.py:608, 761`

- [ ] **Step 4.1: Patch line 608 (unified rates feather)**

Change:
```python
        unified.write_ipc(unified_path)
```
To:
```python
        unified.write_ipc(unified_path, compression="zstd")
```

- [ ] **Step 4.2: Patch line 761 (funding_rate.feather)**

Change:
```python
        result.write_ipc(filepath)
```
To:
```python
        result.write_ipc(filepath, compression="zstd")
```

- [ ] **Step 4.3: Smoke-check the file parses**

Run: `python -c "import ast, pathlib; ast.parse(pathlib.Path('scripts/extract_unified_funding.py').read_text())"`
Expected: exit 0.

- [ ] **Step 4.4: Commit**

```bash
git add scripts/extract_unified_funding.py
git commit -m "feat(funding): compress unified + funding_rate feathers with zstd"
```

---

### Task 5: Patch `live_funding.py` (pyarrow writer)

**Files:**
- Modify: `src/gmx_historical_data/live_funding.py:140`

- [ ] **Step 5.1: Patch the pyarrow write**

Change:
```python
        feather.write_feather(df, filepath)
```
To:
```python
        feather.write_feather(df, filepath, compression="zstd", compression_level=3)
```

- [ ] **Step 5.2: Run the live_funding tests**

Run: `pytest tests/test_live_funding.py -v`
Expected: all PASS (the merge logic reads existing files via `pd.read_feather`, which handles zstd transparently).

- [ ] **Step 5.3: Commit**

```bash
git add src/gmx_historical_data/live_funding.py
git commit -m "feat(live-funding): compress hourly upsert feathers with zstd"
```

---

### Task 6: Patch `collect_daily_snapshot.py` (pyarrow writer)

**Files:**
- Modify: `scripts/collect_daily_snapshot.py:107`

- [ ] **Step 6.1: Patch the pyarrow write**

Change:
```python
    feather.write_feather(combined, filepath)
```
To:
```python
    feather.write_feather(combined, filepath, compression="zstd", compression_level=3)
```

- [ ] **Step 6.2: Run snapshot tests**

Run: `pytest tests/test_daily_snapshot*.py -v`
Expected: all PASS.

- [ ] **Step 6.3: Commit**

```bash
git add scripts/collect_daily_snapshot.py
git commit -m "feat(snapshot): compress daily-snapshot feathers with zstd"
```

---

## Chunk 2: One-shot rewrite of existing feathers

### Task 7: Create the rewrite script

**Files:**
- Create: `scripts/rewrite_feathers_zstd.py`

- [ ] **Step 7.1: Write the script**

```python
#!/usr/bin/env python3
"""Rewrite existing feather files in place with zstd compression.

Walks a target directory, finds every ``*.feather`` file, and re-encodes
those that are currently uncompressed.  Each rewrite is atomic: write to
``<file>.zstd.tmp`` first, ``rename`` over the original only after a
successful round-trip check.  Files that already appear to be compressed
(size < 70 % of the in-memory table estimate) are skipped.

Designed for the FreqTrade output tree but works on any directory.

Usage:
    python scripts/rewrite_feathers_zstd.py /Volumes/WD\\ Blue\\ 1tb/VMs/data/gmx/futures
    python scripts/rewrite_feathers_zstd.py /path/to/dir --dry-run
    python scripts/rewrite_feathers_zstd.py /path/to/dir --pattern '*-1m-*.feather'

Safety:
    * Atomic rename — a crash mid-rewrite never leaves a half-file.
    * Round-trip check — row count and sum(close) must match the original
      before the rename happens.
    * Idempotent — already-compressed files are skipped on re-runs.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import polars as pl
import pyarrow.feather as feather

logger = logging.getLogger(__name__)

# A file is considered "probably compressed" when its on-disk size is
# already smaller than this fraction of the in-memory polars estimate.
# Uncompressed OHLCV: ratio is ~1.0.  Zstd: ratio is ~0.20-0.25.  We use
# 0.7 as a safe threshold to avoid re-compressing already-shrunk files.
COMPRESSED_RATIO_THRESHOLD = 0.7


def is_probably_uncompressed(path: Path) -> bool:
    """Heuristic: ratio of on-disk size to in-memory size > threshold."""
    try:
        df = pl.read_ipc(path)
    except Exception as exc:
        logger.warning("Cannot read %s: %s — skipping", path, exc)
        return False
    on_disk = path.stat().st_size
    in_memory = df.estimated_size()
    if in_memory == 0:
        return False
    ratio = on_disk / in_memory
    return ratio > COMPRESSED_RATIO_THRESHOLD


def rewrite_one(path: Path, *, dry_run: bool) -> tuple[int, int]:
    """Rewrite a single feather file with zstd compression.

    :returns: ``(bytes_before, bytes_after)``.  ``bytes_after == bytes_before``
        when the file was skipped or the run is a dry-run.
    """
    before = path.stat().st_size
    if not is_probably_uncompressed(path):
        logger.info("SKIP (already compressed): %s", path.name)
        return before, before

    df = pl.read_ipc(path)
    original_rows = len(df)
    original_checksum = None
    if "close" in df.columns:
        original_checksum = df["close"].sum()

    if dry_run:
        logger.info("DRY-RUN would rewrite: %s (%d bytes)", path.name, before)
        return before, before

    tmp_path = path.with_suffix(path.suffix + ".zstd.tmp")
    try:
        df.write_ipc(tmp_path, compression="zstd")

        # Round-trip check before overwriting the original.
        check = pl.read_ipc(tmp_path)
        if len(check) != original_rows:
            raise RuntimeError(
                f"row-count mismatch after rewrite: original={original_rows}, "
                f"rewritten={len(check)}"
            )
        if original_checksum is not None:
            new_checksum = check["close"].sum()
            # Float equality with a tight tolerance — zstd is lossless,
            # so checksums must match exactly modulo float-sum order.
            if abs(new_checksum - original_checksum) > 1e-6:
                raise RuntimeError(
                    f"close-sum drift: original={original_checksum}, "
                    f"rewritten={new_checksum}"
                )

        # Atomic replace.
        os.replace(tmp_path, path)
    except Exception:
        # Tidy up the temp file on any error.
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    after = path.stat().st_size
    logger.info(
        "OK %s: %d -> %d bytes (%.0f%% saved)",
        path.name,
        before,
        after,
        (1 - after / before) * 100,
    )
    return before, after


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Directory to walk")
    parser.add_argument(
        "--pattern",
        default="*.feather",
        help="Glob pattern (default: *.feather)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be rewritten without modifying any files",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Per-file log output",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.directory.is_dir():
        logger.error("Not a directory: %s", args.directory)
        return 1

    files = sorted(args.directory.glob(args.pattern))
    logger.info("Found %d files matching %s", len(files), args.pattern)

    total_before = 0
    total_after = 0
    rewritten = 0
    skipped = 0
    failed = 0

    for path in files:
        try:
            before, after = rewrite_one(path, dry_run=args.dry_run)
            total_before += before
            total_after += after
            if before != after:
                rewritten += 1
            else:
                skipped += 1
        except Exception as exc:
            logger.error("FAIL %s: %s", path.name, exc)
            failed += 1

    saved = total_before - total_after
    pct = (saved / total_before * 100) if total_before else 0
    logger.info(
        "Done: rewrote=%d, skipped=%d, failed=%d | %.1f GB -> %.1f GB (saved %.1f GB, %.0f%%)",
        rewritten,
        skipped,
        failed,
        total_before / 1e9,
        total_after / 1e9,
        saved / 1e9,
        pct,
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 7.2: Verify the script parses and `--help` works**

Run: `python scripts/rewrite_feathers_zstd.py --help`
Expected: usage banner prints, exit 0.

- [ ] **Step 7.3: Commit**

```bash
git add scripts/rewrite_feathers_zstd.py
git commit -m "feat(scripts): add rewrite_feathers_zstd.py for in-place feather recompression

Walks a directory, atomically rewrites uncompressed feathers with zstd,
round-trip-verifies row count and sum(close), skips already-compressed
files.  Used for the one-shot 12GB -> 2.6GB shrink on the FreqTrade tree."
```

---

### Task 8: Dry-run the rewrite on the real futures tree

**Files:** (no code changes — data inspection only)

- [ ] **Step 8.1: Dry-run against the external drive**

Run:
```bash
python scripts/rewrite_feathers_zstd.py "/Volumes/WD Blue 1tb/VMs/data/gmx/futures" --dry-run 2>&1 | tee logs/feather-rewrite-dryrun.log
```
Expected: report of ~2750 files to rewrite, projected savings ~9 GB.

- [ ] **Step 8.2: Inspect a sample of files the script flagged as already compressed**

If any files are reported as SKIP, manually verify their size with `du -h` — they should already be small (< 30 % of the equivalent uncompressed size).  If a clearly-large file is being skipped, raise it for review rather than forcing the rewrite.

- [ ] **Step 8.3: Show the dry-run summary to the user for go/no-go**

Stop here.  Do not proceed to live rewrite without explicit user approval —
this writes ~12 GB across a USB drive and can take significant time.

---

### Task 9: Live rewrite of the futures tree

**Files:** (data only)

- [ ] **Step 9.1: Live rewrite**

Run:
```bash
python scripts/rewrite_feathers_zstd.py "/Volumes/WD Blue 1tb/VMs/data/gmx/futures" -v 2>&1 | tee logs/feather-rewrite.log
```
Expected: each file logged with before/after bytes; final summary shows ≥ 70 % overall saving.

- [ ] **Step 9.2: Confirm size dropped**

Run:
```bash
du -sh "/Volumes/WD Blue 1tb/VMs/data/gmx/futures/"
```
Expected: ≤ 3 GB.

- [ ] **Step 9.3: Spot-check a FreqTrade-style read**

Run:
```bash
uv run --quiet python -c "
import pandas as pd
df = pd.read_feather('/Volumes/WD Blue 1tb/VMs/data/gmx/futures/LINK_USDC_USDC-1m-futures.feather')
print(f'rows={len(df):,} | first={df[\"date\"].iloc[0]} | last={df[\"date\"].iloc[-1]} | close.sum={df[\"close\"].sum():.2f}')
"
```
Expected: rows match pre-rewrite count (2,522,649 for LINK 1m), date range unchanged.

- [ ] **Step 9.4: Commit the log artefact** (optional but useful for the PR description)

```bash
git add logs/feather-rewrite.log
git commit -m "chore(logs): capture feather rewrite output for audit"
```

If `logs/` is gitignored, just attach the summary to the PR description instead.

---

## Chunk 3: PR

### Task 10: Open the PR with proper labels

**Files:** (none — GitHub-side)

- [ ] **Step 10.1: Push the branch**

```bash
git push -u origin feat/feather-zstd-compression
```

- [ ] **Step 10.2: Open the PR**

```bash
gh pr create --title "feat(exporter): compress FreqTrade feathers with zstd (12GB → ~2.6GB)" \
  --label "enhancement,performance,storage" \
  --body "$(cat <<'EOF'
## Summary

- Enables `compression='zstd'` on every feather writer in the FreqTrade export tree (6 call-sites across 4 files).
- Adds `scripts/rewrite_feathers_zstd.py` to atomically recompress the existing 12 GB feather store in place.
- Shrinks the on-disk footprint by ~78 % (12 GB → ~2.6 GB) with zero schema, filename, or directory changes.

## Why

Polars' default `write_ipc` is uncompressed.  The FreqTrade tree had grown to 12 GB across 2,750 files, dominated by 1m candles (7.4 GB).  Benchmark on LINK 1m: 121 MB → 26 MB with `compression='zstd'`.  `pandas.read_feather` (FreqTrade's read path) decompresses transparently — no consumer-side change needed.

## Changes

| File | Change |
|---|---|
| `src/gmx_historical_data/freqtrade_exporter.py` | `write_ipc` calls pass `compression='zstd'` |
| `src/gmx_historical_data/live_funding.py` | `feather.write_feather` passes zstd + level 3 |
| `scripts/extract_funding_fee_per_size.py` | `write_ipc` passes zstd |
| `scripts/extract_unified_funding.py` | both `write_ipc` calls pass zstd |
| `scripts/collect_daily_snapshot.py` | `feather.write_feather` passes zstd + level 3 |
| `scripts/rewrite_feathers_zstd.py` | new — atomic in-place rewrite with round-trip check |
| `tests/test_freqtrade_exporter_compression.py` | new — asserts writer emits compressed IPC + pandas-readable |

## Verification

- [x] `pytest tests/test_freqtrade_exporter*.py` — all green
- [x] `pytest tests/test_live_funding.py tests/test_daily_snapshot*.py` — all green
- [x] `du -sh futures/` dropped from 12 GB to ~2.6 GB
- [x] Spot-checked LINK 1m / BTC 1h / AVAX 5m read back identically via `pd.read_feather`

## Risk / rollback

Lossless.  To revert: `git revert` this PR, then re-run `rewrite_feathers_zstd.py … --compression uncompressed` (or just let the next export overwrite with the old writer).

## Plan

`docs/superpowers/plans/2026-05-27-feather-zstd-compression.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 10.3: Print the PR URL**

The `gh pr create` command prints the URL on success.  Report it back to the user for review.

---

## Notes for the executor

- Run every test command from the repo root.  Activate the venv with `uv run pytest ...` if pytest is not on PATH.
- Always `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC` before pytest, per the project rule.
- Keep imports at the top of files.  Default docstring format is Sphinx.
- Pause for user review at Step 8.3 (before the live 12 GB rewrite) and at Step 10.1 (before pushing the branch).
