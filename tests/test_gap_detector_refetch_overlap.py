"""Regression tests for AdaptiveGapDetector's NORMAL_GAP refetch overlap.

Root cause: a 4h candle collected by ``collect --update`` while it was still
forming (e.g. 30 minutes into its 4-hour window) got written to
``candles/arbitrum/<SYM>/4h.parquet`` with a narrow, in-progress OHLC
snapshot. ``_calculate_incremental_boundaries`` / ``detect_gap_adaptive``'s
``NORMAL_GAP`` case then set the next run's fetch start to
``our_latest + interval``, i.e. strictly *after* that bar -- so no later run
ever asked the API for that timestamp again, and the narrow snapshot stayed
wrong in storage forever, even once the real candle had long since closed.
This bar is not caught by ``STALE_DENSITY`` either: that check only flags
fully flat (``high == low``) rows, and a bar collected 30 minutes into its
window still has some genuine intrabar movement (``high > low``), just not
the full range the closed bar will eventually have.

Confirmed live on vitalik8: ``gmx-data-collector/user_data/data/gmx/candles/
arbitrum/BTC/4h.parquet`` had dozens of 4h bars going back to 2023 sitting
with ``open == low`` and ``high == close`` (a candle that only ever ticked up
from its open, then froze) -- clustered at the hour the nightly cron
happened to run, 40+ days after the fact.

The fix: ``NORMAL_GAP`` always re-fetches the most recently stored bar
(``refetch_overlap_bars``, default 1) instead of starting strictly after it.
``merge_ohlcv_preferring_dense`` already prefers the incoming row whenever
density is tied (both ``high > low``), so once the API actually has the
closed bar's full range, the refetch safely overwrites the stale one.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from gmx_historical_data.daemon.gap_detector import AdaptiveGapDetector, GapStatus
from gmx_historical_data.storage import ParquetStorage


class _FakeGMXFetcher:
    """Stub GMXDataFetcher returning a fixed (earliest, latest) API range."""

    def __init__(self, earliest: datetime | None, latest: datetime | None) -> None:
        self._earliest = earliest
        self._latest = latest

    def get_latest_data_range(self, symbol: str, period: str):
        return self._earliest, self._latest


def _store_one_bar(
    storage: ParquetStorage,
    symbol: str,
    timeframe: str,
    ts: datetime,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
) -> None:
    """Save a single OHLCV bar, as one incremental collection run would.

    :param storage: Target ParquetStorage.
    :param symbol: Token symbol.
    :param timeframe: Timeframe string (e.g. '4h').
    :param ts: Bar open timestamp.
    :param open_: Open price.
    :param high: High price.
    :param low: Low price.
    :param close: Close price.
    """
    df = pd.DataFrame(
        {
            "timestamp": [ts],
            "open": [open_],
            "high": [high],
            "low": [low],
            "close": [close],
            "symbol": [symbol],
        }
    )
    storage.save_candles(df, timeframe, symbol)


def test_normal_gap_refetches_latest_stored_bar_by_default(tmp_path):
    """NORMAL_GAP's fetch_start is our_latest itself, not our_latest + interval.

    :ensures: The most recently stored bar is always included in the next
        incremental fetch window, so a bar collected before it closed has a
        chance to be corrected on a later run.
    """
    storage = ParquetStorage(tmp_path)
    our_latest = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
    _store_one_bar(
        storage,
        "BTC",
        "4h",
        our_latest,
        open_=79864.77,
        high=79974.65,
        low=79864.77,
        close=79974.65,
    )

    now = our_latest + timedelta(days=40)
    fetcher = _FakeGMXFetcher(earliest=our_latest - timedelta(days=200), latest=now)
    detector = AdaptiveGapDetector(storage, fetcher)

    result = detector.detect_gap_adaptive("BTC", "4h")

    assert result.status == GapStatus.NORMAL_GAP
    assert result.needs_fetch is True
    assert result.fetch_start == our_latest, (
        "default refetch_overlap_bars=1 must start the fetch AT our_latest, not strictly after it"
    )


def test_refetch_overlap_bars_widens_the_window(tmp_path):
    """A larger refetch_overlap_bars re-fetches more of the recent history.

    :ensures: refetch_overlap_bars is actually honoured, not a dead
        constructor argument.
    """
    storage = ParquetStorage(tmp_path)
    our_latest = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
    _store_one_bar(
        storage,
        "ETH",
        "4h",
        our_latest,
        open_=100.0,
        high=101.0,
        low=100.0,
        close=101.0,
    )

    now = our_latest + timedelta(days=40)
    fetcher = _FakeGMXFetcher(earliest=our_latest - timedelta(days=200), latest=now)
    detector = AdaptiveGapDetector(storage, fetcher, refetch_overlap_bars=3)

    result = detector.detect_gap_adaptive("ETH", "4h")

    assert result.status == GapStatus.NORMAL_GAP
    assert result.fetch_start == our_latest - timedelta(hours=8), (
        "refetch_overlap_bars=3 on a 4h timeframe must step back 2 extra bars (8h) from our_latest"
    )


def test_refetch_overlap_bars_rejects_values_below_one():
    """refetch_overlap_bars must always re-fetch at least the latest bar."""
    with pytest.raises(ValueError, match="refetch_overlap_bars"):
        AdaptiveGapDetector(None, None, refetch_overlap_bars=0)


def test_frozen_partial_bar_is_corrected_once_the_overlap_refetch_lands(tmp_path):
    """End-to-end: a narrow bar collected mid-formation gets healed by the
    next incremental run's overlap refetch, once the API has the closed value.

    :ensures: the exact failure this fix targets -- a 4h bar captured 30
        minutes into its window, then never revisited -- is actually
        corrected by a later run, not just reported as needing a fetch.
    """
    storage = ParquetStorage(tmp_path)
    bar_open = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)

    # Run 1: collected 30 minutes into the bar -- narrow, in-progress range.
    _store_one_bar(
        storage,
        "BTC",
        "4h",
        bar_open,
        open_=79864.77,
        high=79974.65,
        low=79864.77,
        close=79974.65,
    )

    now = bar_open + timedelta(days=1)
    fetcher = _FakeGMXFetcher(earliest=bar_open - timedelta(days=200), latest=now)
    detector = AdaptiveGapDetector(storage, fetcher)

    gap = detector.detect_gap_adaptive("BTC", "4h")
    assert gap.status == GapStatus.NORMAL_GAP
    assert gap.fetch_start == bar_open

    # Run 2 (simulated): a day later, the bar has long since closed, so the
    # API now returns its true, full range for that same timestamp. The
    # production fetch always returns the exchange's whole available window
    # and then filters locally to >= fetch_start (see cli.py
    # _fetch_gmx_candles_for_timeframes) -- simulate that filtered result
    # directly here.
    refetched = pd.DataFrame(
        {
            "timestamp": [bar_open],
            "open": [79864.77],
            "high": [80250.10],  # the real, full 4h range
            "low": [79612.30],
            "close": [79980.40],
            "symbol": ["BTC"],
        }
    )
    storage.save_candles(refetched, "4h", "BTC")

    healed = storage.read_candles("4h", "BTC")
    row = healed[healed["timestamp"] == bar_open].iloc[0]
    assert row["high"] == 80250.10
    assert row["low"] == 79612.30
    assert row["close"] == 79980.40
    assert len(healed) == 1, "the overlap refetch must correct the row in place, never duplicate it"
