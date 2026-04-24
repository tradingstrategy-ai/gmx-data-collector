"""Gap detection for GMX OHLCV data.

Detects contiguous ranges where GMX data is suspected missing or stale based
on price pct-change, zero-volume runs, and missing rows against the expected
timeframe grid.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import polars as pl


@dataclass(frozen=True, slots=True)
class DetectorConfig:
    """Tunable thresholds; defaults match the design doc."""

    gap_pct_threshold: float = 0.20
    merge_gap_bars: int = 2
    min_range_bars: int = 1


def price_jump_mask(df: pl.DataFrame, threshold: float) -> pl.Series:
    """Return a boolean Series marking bars where ``|pct_change(close)| > threshold``.

    The first bar is always ``False`` because pct-change is undefined.

    :param df: OHLCV DataFrame with a ``close`` column.
    :param threshold: Fractional threshold, e.g. ``0.20`` for 20%.
    :returns: Boolean Series same length as ``df``.
    """
    pct = df["close"].pct_change().abs()
    return (pct > threshold).fill_null(False)


_TF_TO_MINUTES: dict[str, int] = {
    "1min": 1, "5min": 5, "15min": 15, "1h": 60, "4h": 240, "1d": 1440,
}


def zero_volume_mask(df: pl.DataFrame) -> pl.Series:
    """Boolean Series marking bars with zero volume.

    :param df: OHLCV DataFrame with a ``volume`` column.
    :returns: Boolean Series.
    """
    return df["volume"] == 0


def reindex_and_mark_missing(df: pl.DataFrame, tf_minutes: int) -> tuple[pl.DataFrame, pl.Series]:
    """Reindex ``df`` onto a regular ``tf_minutes``-spaced grid.

    :param df: OHLCV DataFrame sorted or unsorted by timestamp.
    :param tf_minutes: Grid interval in minutes.
    :returns: ``(reindexed_df, missing_mask)`` where ``missing_mask[i]`` is
        ``True`` iff the timestamp was absent from the original frame.
    """
    if df.is_empty():
        return df, pl.Series("missing", [], dtype=pl.Boolean)

    sorted_df = df.sort("timestamp")
    start = sorted_df["timestamp"][0]
    end = sorted_df["timestamp"][-1]
    grid = pl.datetime_range(
        start, end, interval=f"{tf_minutes}m", time_zone="UTC", eager=True
    ).alias("timestamp")
    grid_df = pl.DataFrame({"timestamp": grid})
    merged = grid_df.join(sorted_df, on="timestamp", how="left")
    missing = merged["close"].is_null()
    return merged, missing


def minutes_for_timeframe(tf: str) -> int:
    """Return the grid interval in minutes for a timeframe string.

    :param tf: Timeframe string, e.g. ``"1h"``, ``"5min"``.
    :raises ValueError: For unknown timeframe strings.
    """
    if tf not in _TF_TO_MINUTES:
        raise ValueError(f"unknown timeframe: {tf!r}")
    return _TF_TO_MINUTES[tf]


class RangeKind(Enum):
    """Categorisation of a detected gap range."""

    FULL = "full"
    VOLUME_ONLY = "volume_only"


@dataclass(frozen=True, slots=True)
class GapRange:
    """Inclusive [start_idx, end_idx] range into the reindexed DataFrame."""

    start_idx: int
    end_idx: int
    kind: RangeKind

    def __len__(self) -> int:
        return self.end_idx - self.start_idx + 1


@dataclass(frozen=True, slots=True)
class DetectionResult:
    """Output of :func:`detect_gaps`."""

    reindexed_df: pl.DataFrame
    full_ranges: list[GapRange]
    volume_ranges: list[GapRange]


def cluster_ranges(
    mask: pl.Series, merge_gap_bars: int, min_range_bars: int
) -> list[tuple[int, int]]:
    """Cluster a boolean Series into inclusive ``[start, end]`` index pairs.

    Runs of ``True`` separated by at most ``merge_gap_bars`` ``False`` entries
    are merged. Resulting ranges shorter than ``min_range_bars`` are dropped.

    :param mask: Boolean Series.
    :param merge_gap_bars: Max ``False`` gap to bridge when merging runs.
    :param min_range_bars: Minimum length for a range to be kept.
    :returns: List of ``(start_idx, end_idx)`` inclusive pairs.
    """
    result: list[tuple[int, int]] = []
    vals = mask.to_list()
    n = len(vals)
    i = 0
    while i < n:
        if not vals[i]:
            i += 1
            continue
        start = i
        end = i
        while end + 1 < n:
            look = end + 1
            skipped = 0
            while look < n and not vals[look] and skipped < merge_gap_bars:
                look += 1
                skipped += 1
            if look < n and vals[look]:
                end = look
            else:
                break
        if (end - start + 1) >= min_range_bars:
            result.append((start, end))
        i = end + 1
    return result


def detect_gaps(
    df: pl.DataFrame, tf: str, config: DetectorConfig
) -> DetectionResult:
    """Run the full detection pipeline on a single ``(symbol, timeframe)`` frame.

    :param df: GMX OHLCV DataFrame.
    :param tf: Timeframe string, e.g. ``"1h"``.
    :param config: Detection thresholds.
    :returns: :class:`DetectionResult` with reindexed frame and gap ranges.
    """
    tf_minutes = minutes_for_timeframe(tf)
    reindexed, missing = reindex_and_mark_missing(df, tf_minutes=tf_minutes)

    price_jump = price_jump_mask(reindexed, threshold=config.gap_pct_threshold)
    price_bad = price_jump | missing

    vol_null_or_zero = reindexed["volume"].is_null() | (reindexed["volume"] == 0)
    vol_bad = vol_null_or_zero
    vol_only = vol_bad & ~price_bad

    full_ranges = [
        GapRange(s, e, RangeKind.FULL)
        for s, e in cluster_ranges(price_bad, config.merge_gap_bars, config.min_range_bars)
    ]
    volume_ranges = [
        GapRange(s, e, RangeKind.VOLUME_ONLY)
        for s, e in cluster_ranges(vol_only, config.merge_gap_bars, config.min_range_bars)
    ]
    return DetectionResult(
        reindexed_df=reindexed,
        full_ranges=full_ranges,
        volume_ranges=volume_ranges,
    )
