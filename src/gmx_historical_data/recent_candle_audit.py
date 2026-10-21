"""Audit recent GMX candle files for fixed-interval gaps."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import polars as pl

from gmx_historical_data.ohlcv_validation import (
    CadenceBreak,
    find_cadence_breaks,
    parse_timeframe_interval,
)

_FILENAME_TIMEFRAME = re.compile(r"-(\d+(?:m|h|d))-futures\.feather$")


def audit_recent_candles(
    futures_dir: Path, window_start: datetime, window_end: datetime
) -> list[CadenceBreak]:
    """Return cadence breaks whose adjacent candles lie in the requested UTC window."""
    if window_start.tzinfo is None or window_end.tzinfo is None:
        raise ValueError("window_start and window_end must be timezone-aware")
    if window_end < window_start:
        raise ValueError("window_end must not precede window_start")

    breaks: list[CadenceBreak] = []
    for path in sorted(futures_dir.glob("*_USDC_USDC-*-futures.feather")):
        match = _FILENAME_TIMEFRAME.search(path.name)
        if match is None:
            continue
        frame = pl.read_ipc(path, columns=["date"])
        if frame.is_empty():
            continue
        ordered = frame.sort("date")
        recent = ordered.filter(
            pl.col("date").is_between(window_start, window_end, closed="both")
        )
        before = ordered.filter(pl.col("date") < window_start).tail(1)
        after = ordered.filter(pl.col("date") > window_end).head(1)
        recent = pl.concat([before, recent, after]).unique().sort("date")
        breaks.extend(
            find_cadence_breaks(
                recent,
                timestamp_column="date",
                expected_interval=parse_timeframe_interval(match.group(1)),
            )
        )
    return breaks
