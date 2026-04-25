"""Merge CEX OHLCV into GMX parquet, honouring detector ranges and symbol scaling."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

from .detector import DetectionResult, GapRange, RangeKind
from .symbols import PRICE_DIVISORS, normalize_k_prefix, _bare_symbol

log = logging.getLogger(__name__)


class HistoryTruncationError(ValueError):
    """Raised when the reconciled frame begins later than the original."""


@dataclass
class ReconcileStats:
    """Counts of bar replacements made during reconciliation."""

    full_replaced: int = 0
    volume_replaced: int = 0
    kept: int = 0
    cex_missing: int = 0


def apply_price_scale(cex_df: pl.DataFrame, gmx_symbol: str) -> pl.DataFrame:
    """Scale CEX OHLC and volume so they align with the GMX symbol's price regime.

    For 1000x tokens (e.g. BONK): OHLC / 1000, volume * 1000. For others: noop.

    :param cex_df: CEX OHLCV DataFrame.
    :param gmx_symbol: GMX-side symbol, e.g. ``"BONK"``.
    :returns: DataFrame with adjusted OHLCV columns (or the original if no scaling needed).
    """
    bare = _bare_symbol(normalize_k_prefix(gmx_symbol))
    divisor = PRICE_DIVISORS.get(bare)
    if divisor is None:
        return cex_df
    return cex_df.with_columns(
        (pl.col("open") / divisor).alias("open"),
        (pl.col("high") / divisor).alias("high"),
        (pl.col("low") / divisor).alias("low"),
        (pl.col("close") / divisor).alias("close"),
        (pl.col("volume") * divisor).alias("volume"),
    )


def _pct_change_across(df: pl.DataFrame, start_idx: int, end_idx: int) -> float | None:
    """Close-to-close fractional change from bar before ``start_idx`` to bar at ``end_idx``."""
    if start_idx == 0 or end_idx >= df.height:
        return None
    prev = df["close"][start_idx - 1]
    last = df["close"][end_idx]
    if prev is None or last is None or prev == 0:
        return None
    return (last - prev) / prev


def _relative_close_delta(left: float | None, right: float | None) -> float | None:
    """Return the absolute fractional delta between two close prices."""
    if left is None or right is None or left == 0:
        return None
    return abs(right - left) / abs(left)


def reconcile(
    gmx_df: pl.DataFrame,
    cex_df: pl.DataFrame,
    detection: DetectionResult,
    gmx_symbol: str,
    config,
) -> tuple[pl.DataFrame, ReconcileStats]:
    """Apply CEX OHLCV replacement ranges into the reindexed GMX frame.

    :param gmx_df: Original GMX OHLCV DataFrame (pre-reindex).
    :param cex_df: CEX OHLCV DataFrame (freqtrade feather, may be empty).
    :param detection: Output of :func:`~detector.detect_gaps`.
    :param gmx_symbol: GMX-side symbol for price scaling.
    :param config: :class:`~detector.DetectorConfig` for threshold access.
    :returns: ``(corrected_df, stats)`` — corrected frame has same schema as
        ``detection.reindexed_df``.
    """
    stats = ReconcileStats()
    scaled_cex = apply_price_scale(cex_df, gmx_symbol) if not cex_df.is_empty() else cex_df
    cex_by_ts: dict = (
        {row["timestamp"]: row for row in scaled_cex.iter_rows(named=True)}
        if not scaled_cex.is_empty()
        else {}
    )

    df = detection.reindexed_df
    new_rows = df.to_dicts()

    # Full-replace pass
    for gap in detection.full_ranges:
        ts_range = [new_rows[i]["timestamp"] for i in range(gap.start_idx, gap.end_idx + 1)]
        missing = [ts for ts in ts_range if ts not in cex_by_ts]
        if missing:
            log.info("cex missing for %s range [%s..%s]", gmx_symbol, ts_range[0], ts_range[-1])
            stats.cex_missing += 1
            continue

        gmx_pct = _pct_change_across(df, gap.start_idx, gap.end_idx)
        cex_slice = scaled_cex.filter(pl.col("timestamp").is_in(ts_range))
        prev_ts = new_rows[gap.start_idx - 1]["timestamp"] if gap.start_idx > 0 else None
        cex_prev = cex_by_ts.get(prev_ts, {}).get("close") if prev_ts is not None else None
        cex_last = cex_slice["close"][-1] if cex_slice.height > 0 else None
        cex_pct = (
            (cex_last - cex_prev) / cex_prev
            if (cex_prev and cex_prev != 0 and cex_last is not None)
            else None
        )

        if (
            gmx_pct is not None
            and cex_pct is not None
            and abs(cex_pct - gmx_pct) < config.gap_pct_threshold / 2
        ):
            stats.kept += 1
            continue

        for i in range(gap.start_idx, gap.end_idx + 1):
            src = cex_by_ts[new_rows[i]["timestamp"]]
            for col in ("open", "high", "low", "close", "volume"):
                new_rows[i][col] = src[col]
        stats.full_replaced += 1

    # Volume-only pass
    for gap in detection.volume_ranges:
        for i in range(gap.start_idx, gap.end_idx + 1):
            ts = new_rows[i]["timestamp"]
            if ts in cex_by_ts:
                delta = _relative_close_delta(new_rows[i]["close"], cex_by_ts[ts]["close"])
                if delta is not None and delta < config.gap_pct_threshold / 2:
                    new_rows[i]["volume"] = cex_by_ts[ts]["volume"]
                    stats.volume_replaced += 1

    return pl.DataFrame(new_rows, schema=df.schema), stats


def assert_history_preserved(original: pl.DataFrame, corrected: pl.DataFrame) -> None:
    """Fatal guard: earliest timestamp of corrected must equal or precede original.

    :param original: Pre-reconciliation GMX frame.
    :param corrected: Post-reconciliation frame.
    :raises HistoryTruncationError: If the corrected frame starts later than the original.
    """
    if original.is_empty() and corrected.is_empty():
        return
    if original.is_empty() or corrected.is_empty():
        return
    orig_first = original.sort("timestamp")["timestamp"][0]
    corr_first = corrected.sort("timestamp")["timestamp"][0]
    if corr_first > orig_first:
        raise HistoryTruncationError(
            f"corrected frame starts at {corr_first} but original starts at {orig_first}"
        )


def warn_seam_discontinuities(df: pl.DataFrame, threshold: float, gmx_symbol: str) -> int:
    """Log a warning for residual pct_change > threshold after reconciliation. Never raises.

    :param df: Post-reconciliation OHLCV DataFrame.
    :param threshold: Fractional threshold.
    :param gmx_symbol: Used only for the log message.
    :returns: Count of offending bars.
    """
    pct = df["close"].pct_change().abs()
    n = int((pct > threshold).fill_null(False).sum())
    if n > 0:
        log.warning(
            "seam discontinuities after reconcile for %s: %d bars > %.2f",
            gmx_symbol,
            n,
            threshold,
        )
    return n
