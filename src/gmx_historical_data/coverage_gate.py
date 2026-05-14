# src/gmx_historical_data/coverage_gate.py
"""Shared coverage gate for data collectors.

Each collector calls one of the two functions below before issuing a fetch.
If the on-disk file already covers the work, the collector can skip the
network call entirely.  See
``docs/superpowers/specs/2026-05-14-collectors-incremental-audit-design.md``
for the design.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
import polars as pl
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SkipDecision:
    """Outcome of a coverage check at one collector site.

    :ivar skip: ``True`` when the collector should bypass its fetch.
    :ivar reason: One of ``"missing"``, ``"too_small"``, ``"stale"``,
        ``"forced"``, ``"current"``.
    :ivar existing_rows: Row count of the on-disk file (``0`` for both
        genuinely-missing and corrupt files).
    :ivar expected_min_rows: The threshold the caller asked to enforce.
    """

    skip: bool
    reason: str
    existing_rows: int
    expected_min_rows: int


def is_current(
    path: Path,
    expected_min_rows: int,
    *,
    force: bool = False,
    fmt: Literal["parquet", "feather"] = "parquet",
) -> SkipDecision:
    """Decide whether a daily-stamped parquet already covers today's work.

    Reads only the file footer (parquet metadata) — never opens row groups.

    :param path: Target output file.
    :param expected_min_rows: Floor — files with fewer rows are treated as
        incomplete and re-fetched.
    :param force: When ``True``, always returns ``skip=False, reason='forced'``.
    :param fmt: ``'parquet'`` (default) or ``'feather'``.  Only ``'parquet'``
        is currently used by daily-stamped data.
    """
    if force and path.exists():
        # Existing-but-forced: surface the row count for the report.
        try:
            existing = pq.read_metadata(str(path)).num_rows if fmt == "parquet" else 0
        except Exception:
            existing = 0
        return SkipDecision(False, "forced", existing, expected_min_rows)
    if force:
        return SkipDecision(False, "forced", 0, expected_min_rows)
    if not path.exists():
        return SkipDecision(False, "missing", 0, expected_min_rows)

    try:
        if fmt == "parquet":
            rows = pq.read_metadata(str(path)).num_rows
        else:
            # Feather metadata read — fall back to full read of the index col.
            rows = pl.read_ipc(str(path), columns=[]).height
    except Exception as exc:
        logger.warning("coverage_gate: failed to read %s (%s) — treating as missing", path, exc)
        return SkipDecision(False, "missing", 0, expected_min_rows)

    if rows < expected_min_rows:
        return SkipDecision(False, "too_small", rows, expected_min_rows)
    return SkipDecision(True, "current", rows, expected_min_rows)
