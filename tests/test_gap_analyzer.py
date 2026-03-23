"""Tests for data gap analysis."""

from datetime import UTC

import pandas as pd

from gmx_historical_data.gap_analyzer import DataGapAnalyzer


def test_calculate_gap_with_chainlink_available():
    """Test gap calculation when Chainlink feed exists."""
    # Create mock GMX data (starts 2024-07-01)
    dates = pd.date_range("2024-07-01", "2024-12-31", freq="1h", tz=UTC)
    gmx_df = pd.DataFrame(
        {
            "timestamp": dates,
            "close": [2000.0] * len(dates),  # Dummy prices
        }
    )

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(gmx_df=gmx_df, chainlink_available=True)

    # Should backfill from beginning to just before GMX start
    # backfill_start is None to indicate "fetch all available"
    assert backfill_start is None
    assert backfill_end is not None

    # backfill_end should be ~1 second before GMX earliest
    gmx_earliest = gmx_df["timestamp"].min().timestamp()
    assert abs(backfill_end - gmx_earliest) < 2  # Within 2 seconds


def test_calculate_gap_no_chainlink():
    """Test gap when Chainlink feed doesn't exist."""
    dates = pd.date_range("2024-07-01", "2024-12-31", freq="1h", tz=UTC)
    gmx_df = pd.DataFrame(
        {
            "timestamp": dates,
            "close": [2000.0] * len(dates),
        }
    )

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(gmx_df=gmx_df, chainlink_available=False)

    # No backfill needed
    assert backfill_start is None
    assert backfill_end is None


def test_calculate_gap_empty_gmx_data():
    """Test gap when GMX data is empty."""
    gmx_df = pd.DataFrame()

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(gmx_df=gmx_df, chainlink_available=True)

    # With empty GMX data, no backfill needed (nothing to backfill before)
    assert backfill_start is None
    assert backfill_end is None
