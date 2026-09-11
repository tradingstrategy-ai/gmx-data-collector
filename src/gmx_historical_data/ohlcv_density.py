"""Helpers for detecting flat/placeholder OHLCV candles.

A "flat" candle (``high == low``) with no genuine intrabar price movement can
arise from two different situations that must not be conflated:

1. A source that genuinely only prints once per bar (e.g. Chainlink's classic
   on-chain feed during a quiet period, or any Chainlink window that
   predates GMX's own ``/prices/candles`` API existing) forward-filled by
   the resampler. This can be a faithful reflection of that source's real
   update cadence and is not, by itself, a defect.
2. A denser source (GMX's own API, which prints genuine per-minute OHLC) was
   available for that same window but a flat placeholder from a coarser
   source ended up on disk anyway — a data-quality regression.

These helpers quantify "how flat" a slice of OHLCV data is. They are used
both by the merge layer (:mod:`gmx_historical_data.storage`, preferring
denser rows on timestamp collision) and by collection gap detection
(:mod:`gmx_historical_data.daemon.gap_detector`, so a store that is
timestamp-continuous but density-starved for a window a denser source could
have covered is not mistaken for "up to date" and skipped forever).

Root cause this module exists to guard against: ``candles/arbitrum/BTC/
1m.parquet`` was found 92% flat (``high == low``) across its *entire*
2021-07-13 -> 2026-09-06 history, including the most recent ~6 months, even
though GMX's own API demonstrably provides dense (0% flat) 1-minute data for
that same recent window (see ``user_data/data/gmx/futures/
BTC_USDC_USDC-1m-futures.feather``). The gap detectors only ever compared
*timestamps* ("do we already have a row for every recent minute?"), which
Chainlink's forward-filled resampling always satisfies, so a GMX-API refetch
of the recent window was never triggered once the initial Chainlink walk
reached "now".
"""

from __future__ import annotations

import pandas as pd
import polars as pl

#: Fraction of flat (``high == low``) rows in a recent window above which the
#: window is considered "stale density" — i.e. dominated by a coarser
#: source's placeholders even though a denser source may be available.
#: Chosen well below the ~0.92 flat fraction actually observed for the BTC
#: regression so genuine low-volatility periods (which do produce some flat
#: real candles) don't false-positive.
DEFAULT_STALE_DENSITY_THRESHOLD = 0.5


def flat_fraction_pandas(df: pd.DataFrame) -> float:
    """Return the fraction of rows where ``high == low`` in a pandas frame.

    :param df: OHLCV frame with ``high`` and ``low`` columns.
    :return: Fraction in ``[0.0, 1.0]``; ``0.0`` for an empty frame.
    """
    if df.empty:
        return 0.0
    return float((df["high"] == df["low"]).mean())


def flat_fraction_polars(df: pl.DataFrame) -> float:
    """Return the fraction of rows where ``high == low`` in a Polars frame.

    :param df: OHLCV frame with ``high`` and ``low`` columns.
    :return: Fraction in ``[0.0, 1.0]``; ``0.0`` for an empty frame.
    """
    if df.is_empty():
        return 0.0
    return float((df["high"] == df["low"]).mean())


def merge_ohlcv_preferring_dense(
    existing: pl.DataFrame,
    incoming: pl.DataFrame,
    ts_col: str = "timestamp",
) -> pl.DataFrame:
    """Merge two OHLCV frames on a timestamp column, preferring the denser row.

    Plain ``keep="last"`` dedup after ``pl.concat([existing, incoming])``
    always keeps whichever frame was listed second, regardless of quality --
    so a flat (``high == low``) placeholder from a coarser source could
    silently overwrite a genuine, denser candle purely based on call order
    (or vice versa). This is the shared merge used by both the source
    candle store (:meth:`gmx_historical_data.storage.ParquetStorage.
    save_candles`) and the Freqtrade export
    (:meth:`gmx_historical_data.freqtrade_exporter.FreqtradeExporter.
    _merge_export_frames`), so a store fixed by one path can't be
    regressed back to flat by the other.

    A row with ``high != low`` (real intrabar movement) always wins over a
    flat row for the same timestamp. When both rows are equally dense (or
    equally flat), the *incoming* row wins -- preserving "newer write wins"
    semantics for genuine same-density updates.

    :param existing: On-disk OHLCV frame.
    :param incoming: New OHLCV frame to merge in.
    :param ts_col: Name of the timestamp column to dedup on (``"timestamp"``
        for the candle store, ``"date"`` for the Freqtrade export).
    :return: Merged, timestamp-sorted frame with the dense/incoming tiebreak
        columns dropped.
    """
    existing_marked = existing.with_columns(
        [
            (pl.col("high") != pl.col("low")).alias("__dense"),
            pl.lit(0, dtype=pl.Int8).alias("__seq"),
        ]
    )
    incoming_marked = incoming.with_columns(
        [
            (pl.col("high") != pl.col("low")).alias("__dense"),
            pl.lit(1, dtype=pl.Int8).alias("__seq"),
        ]
    )
    # Sort so that, within each timestamp, a dense row always sorts after a
    # flat one, and (among equal density) incoming always sorts after
    # existing -- so unique(keep="last") below picks dense-over-flat, then
    # incoming-over-existing on a true tie.
    combined = pl.concat([existing_marked, incoming_marked]).sort([ts_col, "__dense", "__seq"])
    return (
        combined.unique(subset=[ts_col], keep="last", maintain_order=True)
        .drop(["__dense", "__seq"])
        .sort(ts_col)
    )


def is_stale_density_pandas(
    df: pd.DataFrame,
    threshold: float = DEFAULT_STALE_DENSITY_THRESHOLD,
) -> bool:
    """Check whether a pandas OHLCV window is dominated by flat placeholders.

    :param df: OHLCV frame with ``high`` and ``low`` columns, already sliced
        to the window of interest (e.g. the range a denser source could
        cover).
    :param threshold: Flat-fraction above which the window is considered
        stale. Defaults to :data:`DEFAULT_STALE_DENSITY_THRESHOLD`.
    :return: ``True`` if the flat fraction exceeds ``threshold``.
    """
    if df.empty:
        return False
    return flat_fraction_pandas(df) > threshold
