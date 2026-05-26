#!/usr/bin/env python3
"""Rewrite existing feather files in place with zstd compression.

Walks a target directory, finds every ``*.feather`` file, and re-encodes
those that are currently uncompressed.  Each rewrite is atomic: write to
``<file>.zstd.tmp`` first, ``rename`` over the original only after a
successful round-trip check.  Files that already appear to be compressed
(size < 70 % of the in-memory polars estimate) are skipped.

Designed for the FreqTrade output tree but works on any directory.

Usage::

    python scripts/rewrite_feathers_zstd.py /Volumes/WD\\ Blue\\ 1tb/VMs/data/gmx/futures
    python scripts/rewrite_feathers_zstd.py /path/to/dir --dry-run
    python scripts/rewrite_feathers_zstd.py /path/to/dir --pattern '*-1m-*.feather'

Safety:
    * Atomic rename — a crash mid-rewrite never leaves a half-file.
    * Round-trip check — row count and ``sum(close)`` must match the
      original before the rename happens.
    * Idempotent — already-compressed files are skipped on re-runs.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# A file is considered "probably compressed" when its on-disk size is
# already smaller than this fraction of the in-memory polars estimate.
# Uncompressed OHLCV: ratio is ~1.0.  Zstd: ratio is ~0.20-0.25.  We use
# 0.7 as a safe threshold to avoid re-compressing already-shrunk files.
COMPRESSED_RATIO_THRESHOLD = 0.7


def is_probably_uncompressed(path: Path) -> bool:
    """Heuristic: ratio of on-disk size to in-memory size > threshold.

    :param path: Feather file to inspect.
    :returns: ``True`` if the file appears uncompressed (and worth rewriting).
    """
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

    :param path: File to rewrite.
    :param dry_run: If ``True``, do not modify the file.
    :returns: ``(bytes_before, bytes_after)``.  ``bytes_after == bytes_before``
        when the file was skipped or the run is a dry-run.
    """
    before = path.stat().st_size
    if not is_probably_uncompressed(path):
        logger.info("SKIP (already compressed): %s", path.name)
        return before, before

    df = pl.read_ipc(path)
    original_rows = len(df)
    original_checksum: float | None = None
    if "close" in df.columns:
        original_checksum = float(df["close"].sum())

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
            new_checksum = float(check["close"].sum())
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
    """CLI entry point."""
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
