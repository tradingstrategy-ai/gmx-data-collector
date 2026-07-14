"""Tests for shared OHLCV validation helpers."""

from datetime import UTC, datetime

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.ohlcv_validation import (
    ordering_tolerance_for_timeframe,
    validate_ohlcv,
)
from gmx_historical_data.storage import ParquetStorage


def _frame(*, open_: float | None, high: float | None, low: float | None, close: float | None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": pl.Series(
                "date", [datetime(2025, 9, 27, 13, tzinfo=UTC)], dtype=pl.Datetime("us", "UTC")
            ),
            "open": pl.Series("open", [open_], dtype=pl.Float64),
            "high": pl.Series("high", [high], dtype=pl.Float64),
            "low": pl.Series("low", [low], dtype=pl.Float64),
            "close": pl.Series("close", [close], dtype=pl.Float64),
            "volume": pl.Series("volume", [0.0], dtype=pl.Float64),
        }
    )


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("close", None),
        ("close", 0.0),
        ("close", float("inf")),
    ],
)
def test_validate_ohlcv_rejects_invalid_close_values(column, value):
    frame = _frame(open_=1.0, high=1.0, low=1.0, close=1.0).with_columns(
        pl.lit(value, dtype=pl.Float64).alias(column)
    )

    with pytest.raises(ValueError, match="invalid OHLCV"):
        validate_ohlcv(frame, timestamp_column="date", location="SATS/1h")


def test_validate_ohlcv_rejects_inverted_high_low():
    frame = _frame(open_=4957.67, high=1.0, low=4942.54, close=4952.28)

    with pytest.raises(ValueError, match="OHLC ordering"):
        validate_ohlcv(frame, timestamp_column="date", location="XAUT/1h")


def test_validate_ohlcv_allows_negative_funding_open():
    """Funding rates live in ``open`` and legitimately go negative."""
    frame = _frame(open_=-0.0001, high=0.0, low=0.0, close=0.0)

    result = validate_ohlcv(
        frame,
        timestamp_column="date",
        location="AAVE/funding",
        allow_nonpositive_prices=True,
    )

    assert result.height == 1


def test_validate_ohlcv_tolerates_small_ordering_overshoot_within_band():
    """A benign 0.5% low-above-body overshoot passes at 4h but not at 1h."""
    frame = _frame(open_=100.0, high=101.0, low=100.5, close=100.2)

    with pytest.raises(ValueError, match="OHLC ordering"):
        validate_ohlcv(frame, timestamp_column="date", location="X/1h")

    # Within the 0.75% band -> accepted.
    validate_ohlcv(
        frame, timestamp_column="date", location="X/4h", ordering_tolerance=0.0075
    )


def test_validate_ohlcv_rejects_ordering_overshoot_beyond_tolerance():
    """A 1% low-above-body overshoot breaches even the tolerant bound."""
    frame = _frame(open_=100.0, high=101.0, low=101.0, close=100.2)

    with pytest.raises(ValueError, match="OHLC ordering"):
        validate_ohlcv(
            frame, timestamp_column="date", location="X/4h", ordering_tolerance=0.0075
        )


def test_ordering_tolerance_for_timeframe_matrix():
    assert ordering_tolerance_for_timeframe("4h") == 0.0075
    assert ordering_tolerance_for_timeframe("1d") == 0.0075
    assert ordering_tolerance_for_timeframe("1h") == 0.0
    assert ordering_tolerance_for_timeframe("5m") == 0.0
    assert ordering_tolerance_for_timeframe(None) == 0.0


def test_save_candles_rejects_all_null_bar(tmp_path):
    storage = ParquetStorage(tmp_path)
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-09-27 13:00:00+00:00"], utc=True),
            "open": [float("nan")],
            "high": [float("nan")],
            "low": [float("nan")],
            "close": [float("nan")],
            "symbol": ["SATS"],
        }
    )

    with pytest.raises(ValueError, match="save_candles\\(SATS/1h\\).*invalid OHLCV"):
        storage.save_candles(frame, timeframe="1h", symbol="SATS")


def test_save_candles_accepts_unsorted_incoming(tmp_path):
    """Collectors are not guaranteed to hand candles pre-sorted; save_candles
    must sort before validating so a spurious 'non-monotonic timestamps'
    ValueError is not raised for otherwise-valid, out-of-order input."""
    storage = ParquetStorage(tmp_path)
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2025-09-27 15:00:00+00:00",
                    "2025-09-27 13:00:00+00:00",
                    "2025-09-27 14:00:00+00:00",
                ],
                utc=True,
            ),
            "open": [102.0, 100.0, 101.0],
            "high": [103.0, 101.0, 102.0],
            "low": [101.5, 99.5, 100.5],
            "close": [102.5, 100.5, 101.5],
            "symbol": ["ETH", "ETH", "ETH"],
        }
    )

    output_path = storage.save_candles(frame, timeframe="1h", symbol="ETH")

    persisted = pd.read_parquet(output_path)
    assert persisted["timestamp"].is_monotonic_increasing
