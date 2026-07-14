"""Synthetic regressions for candle-integrity audit coverage."""

from datetime import UTC, datetime, timedelta

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.ohlcv_validation import assert_export_parity, validate_ohlcv
from scripts.validate_price_continuity import validate_frame


def _pandas_frame(values: list[float], *, start: datetime, timeframe: str = "1h") -> pd.DataFrame:
    delta = {
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }[timeframe]
    timestamps = [start + i * delta for i in range(len(values))]
    return pd.DataFrame(
        {
            "date": pd.to_datetime(timestamps, utc=True),
            "open": values,
            "high": [v * 1.01 for v in values],
            "low": [v * 0.99 for v in values],
            "close": values,
            "volume": [0.0] * len(values),
        }
    )


def test_validate_ohlcv_rejects_sats_null_bar() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": pl.Series(
                [datetime(2025, 9, 27, 13, tzinfo=UTC)], dtype=pl.Datetime("ns", "UTC")
            ),
            "open": [None],
            "high": [None],
            "low": [None],
            "close": [None],
        }
    )

    with pytest.raises(ValueError, match="SATS/1h.*invalid OHLCV"):
        validate_ohlcv(frame, timestamp_column="timestamp", location="SATS/1h")


def test_validate_frame_rejects_om_style_regime_flip() -> None:
    frame = _pandas_frame(
        [0.066850, 0.007513, 0.066850],
        start=datetime(2026, 5, 26, 7, tzinfo=UTC),
    )

    report = validate_frame(frame, timeframe="1h", path="OM_USDC_USDC-1h-futures.parquet")
    assert report.decade_jumps > 0
    assert report.ok is False


def test_validate_frame_rejects_xaut_style_scale_jump() -> None:
    frame = _pandas_frame(
        [4957.84, 4.952280e15, 4942.904],
        start=datetime(2026, 1, 23, 6, tzinfo=UTC),
    )

    report = validate_frame(frame, timeframe="1h", path="XAUT_USDC_USDC-1h-futures.parquet")
    assert report.decade_jumps > 0
    assert report.ok is False


def test_validate_frame_accepts_clean_bonk_style_series() -> None:
    frame = _pandas_frame(
        [0.000032, 0.000033, 0.000034],
        start=datetime(2026, 1, 1, tzinfo=UTC),
    )

    report = validate_frame(
        frame,
        timeframe="1h",
        path="BONK_USDC_USDC-1h-futures.feather",
        allow_gaps=True,
    )
    assert report.ok is True
    assert report.decade_jumps == 0
    assert report.zero_or_nan == 0
    assert report.duplicate_ts == 0
    assert report.malformed_ohlcv == 0


def test_validate_frame_rejects_non_monotonic_file_order() -> None:
    frame = (
        _pandas_frame(
            [0.000032, 0.000033],
            start=datetime(2026, 1, 1, tzinfo=UTC),
        )
        .iloc[::-1]
        .reset_index(drop=True)
    )

    report = validate_frame(frame, timeframe="1h", path="BONK_USDC_USDC-1h-futures.feather")

    assert report.malformed_ohlcv == 1
    assert report.ok is False


def test_assert_export_parity_accepts_identical_frames() -> None:
    left = pl.DataFrame(
        {
            "date": pl.datetime_range(
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 1, 2, tzinfo=UTC),
                interval="1h",
                time_zone="UTC",
                eager=True,
                closed="left",
            ).cast(pl.Datetime("ns", "UTC")),
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "volume": [0.0, 0.0],
        }
    )
    right = left.clone()

    assert_export_parity(left, right, location="BONK_USDC_USDC-1h-futures")


def _overshoot_frame(timeframe: str) -> pd.DataFrame:
    delta = {"1h": timedelta(hours=1), "4h": timedelta(hours=4)}[timeframe]
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return pd.DataFrame(
        {
            "date": pd.to_datetime([start + i * delta for i in range(3)], utc=True),
            "open": [100.0, 100.0, 100.0],
            "high": [101.0, 101.0, 101.0],
            # Row 0: low 0.5% above the body -> benign aggregation overshoot.
            "low": [100.5, 99.0, 99.0],
            "close": [100.2, 100.2, 100.2],
        }
    )


def test_validate_frame_reports_tolerated_4h_overshoot_without_failing() -> None:
    report = validate_frame(
        _overshoot_frame("4h"),
        timeframe="4h",
        path="BTC_USDC_USDC-4h-futures.feather",
        allow_gaps=True,
    )
    assert report.malformed_ohlcv == 0
    assert report.tolerated_ordering == 1
    assert report.ok is True


def test_validate_frame_rejects_same_overshoot_at_1h() -> None:
    report = validate_frame(
        _overshoot_frame("1h"),
        timeframe="1h",
        path="BTC_USDC_USDC-1h-futures.feather",
        allow_gaps=True,
    )
    assert report.malformed_ohlcv == 1
    assert report.ok is False
