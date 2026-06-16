"""Tests for ``--force`` overwrite + genesis behaviour in the collect path.

Behaviours pinned here:

1. ``DataCollector._merge_and_save_candles`` writes with ``overwrite=True`` when
   ``force`` is set (re-write path), and merges (``overwrite`` defaulting False)
   otherwise.
2. ``FetchBoundaryCalculator.calculate_boundaries`` short-circuits to FULL
   (genesis, ``chainlink_start_timestamp is None``) when ``force`` is set, even
   for a symbol that already has stored candles.

Small fakes stub storage / the adaptive detector so no real RPC or disk access
is required. The collector is built with ``object.__new__`` to bypass the heavy
``__init__`` (RPC providers, Web3, HyperSync).
"""

from datetime import UTC, datetime

import pandas as pd

from gmx_historical_data.cli import DataCollector
from gmx_historical_data.config import FetchMode
from gmx_historical_data.daemon.gap_detector import GapDetectionResult, GapStatus
from gmx_historical_data.fetch_boundary_calculator import FetchBoundaryCalculator
from gmx_historical_data.gap_analyzer import DataGapAnalyzer


class _RecordingStorage:
    """Storage stub that records ``save_candles`` calls.

    :param existing_df: DataFrame returned by :meth:`read_candles`.
    """

    def __init__(self, existing_df: pd.DataFrame) -> None:
        self._existing_df = existing_df
        self.save_candles_calls: list[dict] = []

    def read_candles(self, timeframe: str, symbol: str) -> pd.DataFrame:
        """Return the canned existing candles DataFrame.

        :param timeframe: Ignored.
        :param symbol: Ignored.
        :return: The DataFrame provided at construction.
        """
        return self._existing_df

    def save_candles(
        self,
        df: pd.DataFrame,
        timeframe: str,
        symbol: str,
        overwrite: bool = False,
    ) -> None:
        """Record the call (including the ``overwrite`` kwarg).

        :param df: Candles DataFrame.
        :param timeframe: Timeframe string.
        :param symbol: Token symbol.
        :param overwrite: Whether the file should be overwritten.
        """
        self.save_candles_calls.append(
            {
                "df": df,
                "timeframe": timeframe,
                "symbol": symbol,
                "overwrite": overwrite,
            }
        )


def _candles_df(start: str, periods: int, freq: str = "1h") -> pd.DataFrame:
    """Build a minimal tz-aware OHLCV DataFrame with a ``symbol`` column.

    :param start: Start timestamp.
    :param periods: Number of rows.
    :param freq: Pandas frequency string.
    :return: DataFrame ready for :meth:`save_candles`.
    """
    idx = pd.date_range(start, periods=periods, freq=freq, tz=UTC)
    return pd.DataFrame(
        {
            "timestamp": idx,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 0.0,
            "symbol": "ETH",
        }
    )


def _bare_collector(storage: _RecordingStorage) -> DataCollector:
    """Create a ``DataCollector`` without running its heavy ``__init__``.

    :param storage: Recording storage stub to attach.
    :return: A collector instance with only ``storage`` populated.
    """
    collector = object.__new__(DataCollector)
    collector.storage = storage
    return collector


def test_force_passes_overwrite_true():
    """``force=True`` writes with ``overwrite=True`` and skips the merge load."""
    existing = _candles_df("2025-01-01", 10)
    storage = _RecordingStorage(existing)
    collector = _bare_collector(storage)

    new_df = _candles_df("2026-01-01", 5)
    collector._merge_and_save_candles("ETH", "1h", new_df, merge_with_existing=False, force=True)

    assert len(storage.save_candles_calls) == 1
    assert storage.save_candles_calls[0]["overwrite"] is True


def test_default_passes_overwrite_false():
    """Default (no force) writes with ``overwrite`` False (merge-by-default)."""
    existing = _candles_df("2025-01-01", 10)
    storage = _RecordingStorage(existing)
    collector = _bare_collector(storage)

    new_df = _candles_df("2026-01-01", 5)
    collector._merge_and_save_candles("ETH", "1h", new_df, merge_with_existing=True, force=False)

    assert len(storage.save_candles_calls) == 1
    assert storage.save_candles_calls[0]["overwrite"] is False


class _FakeStorage:
    """Storage stub returning a fixed candles DataFrame (boundary tests).

    :param existing_df: DataFrame returned by :meth:`read_candles`.
    """

    def __init__(self, existing_df: pd.DataFrame) -> None:
        self._existing_df = existing_df

    def read_candles(self, timeframe: str, symbol: str) -> pd.DataFrame:
        """Return the canned existing candles DataFrame.

        :param timeframe: Ignored.
        :param symbol: Ignored.
        :return: The DataFrame provided at construction.
        """
        return self._existing_df


class _FakeGapDetector:
    """Adaptive gap detector stub returning a fixed :class:`GapDetectionResult`.

    :param result: Result returned by :meth:`detect_gap_adaptive`.
    """

    def __init__(self, result: GapDetectionResult) -> None:
        self._result = result

    def detect_gap_adaptive(self, symbol: str, timeframe: str) -> GapDetectionResult:
        """Return the canned gap-detection result.

        :param symbol: Ignored.
        :param timeframe: Ignored.
        :return: The fixed result provided at construction.
        """
        return self._result


def test_force_uses_full_boundaries_even_with_existing_data():
    """``force=True`` -> FULL (genesis) boundaries despite stored candles.

    A symbol with stored data and a NORMAL_GAP would normally be INCREMENTAL.
    Under ``force`` the calculator must short-circuit to full collection:
    FULL mode with ``chainlink_start_timestamp is None`` (genesis re-walk).
    """
    existing = _candles_df("2026-01-01", 100)
    gmx_earliest = datetime(2025, 6, 1, tzinfo=UTC)

    result = GapDetectionResult(
        status=GapStatus.NORMAL_GAP,
        fetch_start=datetime(2026, 1, 5, tzinfo=UTC),
        fetch_end=datetime(2026, 1, 6, tzinfo=UTC),
    )
    calc = FetchBoundaryCalculator(
        storage=_FakeStorage(existing),
        adaptive_gap_detector=_FakeGapDetector(result),
        gap_analyzer=DataGapAnalyzer(),
    )

    b = calc.calculate_boundaries(
        symbol="ETH",
        timeframe="1h",
        mode=FetchMode.INCREMENTAL,
        chainlink_available=True,
        gmx_earliest=gmx_earliest,
        force=True,
    )

    assert b.mode == FetchMode.FULL
    assert b.chainlink_start_timestamp is None
