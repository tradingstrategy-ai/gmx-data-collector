"""Regression tests for AdaptiveGapDetector's STALE_DENSITY status.

Root cause: ``candles/arbitrum/BTC/1m.parquet`` was found 92% flat
(``high == low``) across its entire history, INCLUDING the most recent
~6 months, even though GMX's own API demonstrably provides dense (0% flat)
1-minute data for that same window.

``AdaptiveGapDetector.detect_gap_adaptive`` only ever compared *timestamps*:
"do we already have a row for every minute up to now?" Chainlink's
forward-filled resampling always satisfies that (it writes a row -- flat if
no real update happened -- for every bucket), so the detector reported
``NO_GAP`` forever once the initial Chainlink walk reached "now", and
``gmx_historical_data.cli collect --update`` (and the periodic daemon) never
queried GMX's API again for that symbol/timeframe. This is the load-bearing
guard against that regressing: a timestamp-continuous but mostly-flat
recent window must be reported as needing a fetch (``STALE_DENSITY``), not
``NO_GAP``, whenever GMX's API can plausibly cover it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from gmx_historical_data.daemon.gap_detector import AdaptiveGapDetector, GapStatus
from gmx_historical_data.ohlcv_density import is_stale_density_pandas
from gmx_historical_data.storage import ParquetStorage


class _FakeGMXFetcher:
    """Stub GMXDataFetcher returning a fixed (earliest, latest) API range."""

    def __init__(self, earliest: datetime | None, latest: datetime | None) -> None:
        self._earliest = earliest
        self._latest = latest

    def get_latest_data_range(self, symbol: str, period: str):
        return self._earliest, self._latest


def _near_now() -> datetime:
    """Return a minute-aligned timestamp just ahead of the wall clock.

    ``AdaptiveGapDetector.detect_gap_adaptive`` computes its own
    ``datetime.now(UTC)`` internally rather than accepting an injected
    clock, so "timestamp-continuous through now" has to mean the *real*
    now at test time, not an arbitrary fixed date. Minute-aligning also
    avoids sub-second precision the storage schema (second precision)
    would otherwise reject, and the one-minute lead keeps
    ``our_latest + interval_delta >= now`` true even with normal test
    overhead between store setup and the detector call.
    """
    return datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)


def _forward_filled_flat_store(storage: ParquetStorage, symbol: str, now: datetime) -> None:
    """Simulate a Chainlink-walk store: continuous flat 1-minute candles to ``now``.

    :param storage: Target ParquetStorage.
    :param symbol: Token symbol.
    :param now: Latest timestamp to forward-fill through.
    """
    start = now - timedelta(hours=8)
    timestamps = pd.date_range(start=start, end=now, freq="1min", tz="UTC")
    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * len(timestamps),
            "high": [100.0] * len(timestamps),  # flat: high == low everywhere
            "low": [100.0] * len(timestamps),
            "close": [100.0] * len(timestamps),
            "symbol": [symbol] * len(timestamps),
        }
    )
    storage.save_candles(df, "1min", symbol)


def _dense_store(storage: ParquetStorage, symbol: str, now: datetime) -> None:
    """Simulate a genuinely dense store: continuous non-flat candles to ``now``.

    :param storage: Target ParquetStorage.
    :param symbol: Token symbol.
    :param now: Latest timestamp to cover through.
    """
    start = now - timedelta(hours=8)
    timestamps = pd.date_range(start=start, end=now, freq="1min", tz="UTC")
    n = len(timestamps)
    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * n,
            "high": [101.0 + i * 0.001 for i in range(n)],
            "low": [99.0 - i * 0.001 for i in range(n)],
            "close": [100.0] * n,
            "symbol": [symbol] * n,
        }
    )
    storage.save_candles(df, "1min", symbol)


def test_timestamp_continuous_but_flat_store_is_not_no_gap(tmp_path):
    """A store that is timestamp-current but mostly flat must not latch NO_GAP.

    :ensures: The exact BTC 1m regression -- forward-filled flat coverage
        reaching "now" -- is reported as STALE_DENSITY (needs_fetch=True),
        not NO_GAP, so a GMX-API refetch is actually attempted.
    """
    storage = ParquetStorage(tmp_path)
    now = _near_now()
    _forward_filled_flat_store(storage, "BTC", now)

    # GMX API can cover roughly the same recent window (its sliding window
    # is small, but it's available and dense).
    fetcher = _FakeGMXFetcher(earliest=now - timedelta(hours=5), latest=now)
    detector = AdaptiveGapDetector(storage, fetcher)

    result = detector.detect_gap_adaptive("BTC", "1min")

    assert result.status == GapStatus.STALE_DENSITY
    assert result.needs_fetch is True
    assert result.has_data_loss is False, "Stale density is not the same as permanent data loss."
    assert result.fetch_start == fetcher._earliest


def test_timestamp_continuous_and_dense_store_is_no_gap(tmp_path):
    """A genuinely dense, timestamp-current store must still short-circuit to NO_GAP.

    :ensures: The STALE_DENSITY check does not false-positive on legitimate
        "already up to date" data and cause needless refetching every cycle.
    """
    storage = ParquetStorage(tmp_path)
    now = _near_now()
    _dense_store(storage, "ETH", now)

    fetcher = _FakeGMXFetcher(earliest=now - timedelta(hours=5), latest=now)
    detector = AdaptiveGapDetector(storage, fetcher)

    result = detector.detect_gap_adaptive("ETH", "1min")

    assert result.status == GapStatus.NO_GAP
    assert result.needs_fetch is False


def test_stale_density_threshold_is_configurable(tmp_path):
    """A stricter threshold can be set for callers who want less sensitivity.

    :ensures: ``stale_density_threshold`` is actually honoured, not a dead
        constructor argument.
    """
    storage = ParquetStorage(tmp_path)
    now = _near_now()
    _forward_filled_flat_store(storage, "BTC", now)

    fetcher = _FakeGMXFetcher(earliest=now - timedelta(hours=5), latest=now)
    # Threshold of 1.01 can never be exceeded by a fraction in [0, 1] -> the
    # store is always treated as fine regardless of flatness.
    detector = AdaptiveGapDetector(storage, fetcher, stale_density_threshold=1.01)

    result = detector.detect_gap_adaptive("BTC", "1min")

    assert result.status == GapStatus.NO_GAP


def test_api_unavailable_path_is_unaffected(tmp_path):
    """When the GMX API can't be queried, behaviour is unchanged (no density check possible).

    :ensures: The STALE_DENSITY addition does not change the pre-existing
        API_UNAVAILABLE fallback path (density can't be assessed without an
        API range to check against).
    """
    storage = ParquetStorage(tmp_path)
    now = _near_now()
    _forward_filled_flat_store(storage, "BTC", now)

    fetcher = _FakeGMXFetcher(earliest=None, latest=None)
    detector = AdaptiveGapDetector(storage, fetcher)

    result = detector.detect_gap_adaptive("BTC", "1min")

    assert result.status == GapStatus.NO_GAP


def test_tiny_all_flat_window_is_not_stale_density():
    """Small listings/quiet windows must not latch the stale-density gate."""
    frame = pd.DataFrame({"high": [100.0, 100.0], "low": [100.0, 100.0]})

    assert is_stale_density_pandas(frame) is False
