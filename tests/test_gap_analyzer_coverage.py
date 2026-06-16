"""Coverage-aware incremental backfill tests for ``DataGapAnalyzer``.

These tests pin the behaviour of ``_calculate_incremental_gap``: the incremental
path must never imply "walk from genesis" (``start=None``) when we already hold a
contiguous block of stored data. Instead it must either skip the backfill (data
covered) or return a *bounded* start (feed floor) for the missing older slice.
"""

from datetime import UTC

import pandas as pd

from gmx_historical_data.gap_analyzer import DataGapAnalyzer


def _df(start: str, periods: int, freq: str = "1h") -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame for tests.

    :param start: Start timestamp (parseable by :func:`pandas.date_range`).
    :param periods: Number of rows to generate.
    :param freq: Pandas frequency string for the index spacing.
    :return: DataFrame with a tz-aware ``timestamp`` column and OHLCV columns.
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
        }
    )


def test_incremental_gap_no_backfill_when_history_covered():
    """If our stored data starts at/before GMX earliest, no Chainlink backfill.

    This documents the current correct behaviour for the covered case.
    """
    analyzer = DataGapAnalyzer()
    gmx = _df("2026-01-01", 24)  # GMX earliest = 2026-01-01
    existing = _df("2021-07-13", 100)  # we already have data back to 2021
    start, end = analyzer._calculate_incremental_gap(gmx, existing)
    assert (start, end) == (None, None)  # nothing to fetch


def test_incremental_gap_bounds_start_to_feed_floor_not_genesis():
    """When older data IS missing, start must be bounded (feed floor), not None.

    ``None`` means "walk from genesis" to the RPC collector. When we already
    hold a contiguous block, the backfill must target only the missing older
    slice, bounded below by the feed floor.
    """
    analyzer = DataGapAnalyzer()
    # GMX has older history (2025-06-01) than we store (2026-01-01): our earliest
    # > gmx earliest, so the older slice [feed_floor, our_earliest-1] is missing.
    gmx = _df("2025-06-01", 24)
    existing = _df("2026-01-01", 100)
    start, end = analyzer._calculate_incremental_gap(gmx, existing)
    # end is just before our earliest; start must be a concrete floor, not None
    assert end == int(pd.Timestamp("2026-01-01", tz=UTC).timestamp()) - 1
    assert start is not None and start < end
