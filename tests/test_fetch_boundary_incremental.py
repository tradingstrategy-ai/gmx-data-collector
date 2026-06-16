"""Incremental boundary tests for ``FetchBoundaryCalculator``.

These tests pin two behaviours of ``_calculate_incremental_boundaries``:

1. On ``NORMAL_GAP``/``DATA_LOSS_GAP`` the Chainlink backfill start is *bounded*
   (from the gap analyzer's feed floor), never ``None`` (genesis).
2. A symbol that genuinely has stored candles is never routed into the
   full-collection fallback even when the adaptive detector reports
   ``NO_EXISTING_DATA``/``API_UNAVAILABLE``.

Small fakes stub the adaptive gap detector and storage so no real RPC / disk
access is required.
"""

from datetime import UTC, datetime

import pandas as pd

from gmx_historical_data.config import FetchMode
from gmx_historical_data.daemon.gap_detector import GapDetectionResult, GapStatus
from gmx_historical_data.fetch_boundary_calculator import FetchBoundaryCalculator
from gmx_historical_data.gap_analyzer import DataGapAnalyzer


class _FakeStorage:
    """Storage stub returning a fixed candles DataFrame.

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


def _df(start: str, periods: int, freq: str = "1h") -> pd.DataFrame:
    """Build a minimal tz-aware OHLCV DataFrame.

    :param start: Start timestamp.
    :param periods: Number of rows.
    :param freq: Pandas frequency string.
    :return: DataFrame with a tz-aware ``timestamp`` column.
    """
    idx = pd.date_range(start, periods=periods, freq=freq, tz=UTC)
    return pd.DataFrame({"timestamp": idx, "close": 1.0})


def test_incremental_normal_gap_uses_bounded_chainlink_start():
    """NORMAL_GAP with stored data and older GMX history -> bounded start.

    The symbol has stored candles starting 2026-01-01 but GMX has history back
    to 2025-06-01, so an older slice is missing. The boundary must be
    INCREMENTAL, request a Chainlink backfill, and carry a non-None (bounded)
    ``chainlink_start_timestamp`` instead of genesis.
    """
    existing = _df("2026-01-01", 100)
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
    )

    assert b.mode == FetchMode.INCREMENTAL
    assert b.chainlink_needed is True
    assert b.chainlink_start_timestamp is not None  # bounded, not genesis
    assert b.chainlink_start_timestamp < b.chainlink_end_timestamp


def test_symbol_with_data_not_routed_to_full_on_no_existing_data():
    """NO_EXISTING_DATA misclassification with stored rows -> stay incremental.

    The adaptive detector can mis-report NO_EXISTING_DATA even though storage
    holds candles. Such a symbol must NOT fall back to full (genesis) collection;
    it must be treated as incremental.
    """
    existing = _df("2026-01-01", 100)
    gmx_earliest = datetime(2026, 1, 1, tzinfo=UTC)

    result = GapDetectionResult(
        status=GapStatus.NO_EXISTING_DATA,
        fetch_start=None,
        fetch_end=None,
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
    )

    assert b.mode == FetchMode.INCREMENTAL  # not FULL
