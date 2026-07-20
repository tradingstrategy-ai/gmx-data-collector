"""Tests for shared OHLCV validation helpers."""

from datetime import UTC, datetime

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.ohlcv_validation import (
    count_open_outside_envelope,
    validate_ohlcv,
)
from gmx_historical_data.storage import ParquetStorage


def _frame(
    *, open_: float | None, high: float | None, low: float | None, close: float | None
) -> pl.DataFrame:
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


def test_validate_ohlcv_accepts_carried_forward_open_outside_envelope():
    """``open`` is carried from the previous close, so it may sit outside the
    candle's own high/low whenever price gaps between bars.  That is an export
    convention, not corruption, and must not fail validation at any timeframe.

    Mirrors NEAR 2024-03-06 16:00, where open=4.79805 sat 0.84% below
    low=4.83824 and tripped the old 0.75% tolerance band.
    """
    frame = _frame(open_=4.79805, high=5.55460, low=4.83824, close=5.39190)

    for timeframe in ("1m", "1h", "4h", "1d"):
        validate_ohlcv(frame, timestamp_column="date", location=f"NEAR/{timeframe}")


def test_validate_ohlcv_rejects_close_outside_envelope():
    """``close`` is a genuine in-window print, so it must lie within high/low."""
    frame = _frame(open_=100.0, high=101.0, low=99.0, close=101.5)

    with pytest.raises(ValueError, match="OHLC ordering"):
        validate_ohlcv(frame, timestamp_column="date", location="X/4h")


def test_validate_ohlcv_rejects_decimal_shift_confined_to_open():
    """Excluding ``open`` from the envelope must not let a unit error in
    ``open`` alone pass: here only ``open`` is 10x too large while the rest of
    the row is self-consistent.  Regression guard for the gap found reviewing
    the carried-open change.
    """
    frame = _frame(open_=0.6685, high=0.0669, low=0.0668, close=0.06685)

    with pytest.raises(ValueError, match="open scale"):
        validate_ohlcv(frame, timestamp_column="date", location="OM/1m")


def test_validate_ohlcv_open_scale_bound_admits_real_gaps():
    """The scale bound must not fire on ordinary carried-open gaps, including
    the widest one observed in production (wstETH 4h, 2.34%)."""
    near = _frame(open_=4.79805, high=5.55460, low=4.83824, close=5.39190)
    wsteth = _frame(open_=2607.36, high=2668.44, low=2668.44, close=2668.44)

    validate_ohlcv(near, timestamp_column="date", location="NEAR/4h")
    validate_ohlcv(wsteth, timestamp_column="date", location="wstETH/4h")


def test_count_open_outside_envelope_reports_without_failing():
    """The carried-open artifact is counted for visibility, not enforcement."""
    outside = _frame(open_=4.79805, high=5.55460, low=4.83824, close=5.39190)
    inside = _frame(open_=100.0, high=101.0, low=99.0, close=100.5)

    assert count_open_outside_envelope(outside) == 1
    assert count_open_outside_envelope(inside) == 0


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
