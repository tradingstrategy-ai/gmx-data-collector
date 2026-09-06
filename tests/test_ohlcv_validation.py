"""Tests for shared OHLCV validation helpers."""

from datetime import UTC, datetime, timedelta

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.ohlcv_validation import (
    CadenceBreak,
    assert_export_parity,
    count_open_outside_envelope,
    find_cadence_breaks,
    parse_timeframe_interval,
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


def test_validate_ohlcv_raises_export_validation_error_with_reason():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = pl.DataFrame({"date": [1], "open": [1.0], "high": [1.0], "low": [1.0]})
    with pytest.raises(ExportValidationError) as excinfo:
        validate_ohlcv(frame, timestamp_column="date", location="X/1h")
    assert excinfo.value.reason == "missing_columns"
    assert excinfo.value.location == "X/1h"
    assert isinstance(excinfo.value, ValueError)


def test_validate_ohlcv_empty_frame_reason_slug():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = pl.DataFrame(
        {"date": [], "open": [], "high": [], "low": [], "close": []},
        schema={
            "date": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
        },
    )
    with pytest.raises(ExportValidationError) as excinfo:
        validate_ohlcv(frame, timestamp_column="date", location="X/1h")
    assert excinfo.value.reason == "empty_frame"


def test_assert_export_parity_raises_export_validation_error_with_reason():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    left = pl.DataFrame(
        {"date": [1], "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [0.0]}
    )
    right = pl.DataFrame(
        {"date": [1], "open": [2.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [0.0]}
    )
    with pytest.raises(ExportValidationError) as excinfo:
        assert_export_parity(left, right, location="X/1h")
    assert excinfo.value.reason == "parity_mismatch"


def test_export_validation_error_pickle_round_trip():
    """ExportValidationError's 3-arg __init__ is incompatible with
    ValueError's default args-based __reduce__ (pickle.dumps would call
    cls(str(exc)), missing the required reason/message args), so pickling
    or copying an instance raised TypeError. __reduce__ fixes this --
    latent today (ThreadPoolExecutor, not ProcessPoolExecutor, is used
    throughout this repo) but a cheap inoculation against a future
    ProcessPoolExecutor or cache that pickles exceptions."""
    import pickle

    from gmx_historical_data.ohlcv_validation import ExportValidationError

    original = ExportValidationError("X/1h", "test_reason", "test message")
    restored = pickle.loads(pickle.dumps(original))

    assert restored.location == "X/1h"
    assert restored.reason == "test_reason"
    assert str(restored) == "test message"


def _dated_frame(hours: list[int]) -> pl.DataFrame:
    """Build a frame whose `date` column holds the given hour offsets from a fixed base.

    :param hours: Hour offsets from 2025-06-02 00:00 UTC, in order.
    :returns: A minimal valid OHLCV frame at those timestamps.
    """
    base = datetime(2025, 6, 2, tzinfo=UTC)
    dates = [base + timedelta(hours=h) for h in hours]
    n = len(dates)
    return pl.DataFrame(
        {
            "date": pl.Series("date", dates, dtype=pl.Datetime("us", "UTC")),
            "open": pl.Series("open", [1.0] * n, dtype=pl.Float64),
            "high": pl.Series("high", [1.0] * n, dtype=pl.Float64),
            "low": pl.Series("low", [1.0] * n, dtype=pl.Float64),
            "close": pl.Series("close", [1.0] * n, dtype=pl.Float64),
            "volume": pl.Series("volume", [0.0] * n, dtype=pl.Float64),
        }
    )


@pytest.mark.parametrize(
    "token,expected",
    [
        ("1m", timedelta(minutes=1)),
        ("5m", timedelta(minutes=5)),
        ("15m", timedelta(minutes=15)),
        ("1h", timedelta(hours=1)),
        ("4h", timedelta(hours=4)),
        ("1d", timedelta(days=1)),
        ("1min", timedelta(minutes=1)),
        ("5min", timedelta(minutes=5)),
        ("15min", timedelta(minutes=15)),
        ("8h", timedelta(hours=8)),
        ("12h", timedelta(hours=12)),
    ],
)
def test_parse_timeframe_interval_accepts_both_key_formats(token, expected):
    assert parse_timeframe_interval(token) == expected


@pytest.mark.parametrize("token", ["", "h", "1w", "abc", "0h", "1x", "-1h"])
def test_parse_timeframe_interval_rejects_unknown_tokens(token):
    with pytest.raises(ValueError):
        parse_timeframe_interval(token)


def test_find_cadence_breaks_returns_empty_for_contiguous_series():
    frame = _dated_frame([0, 4, 8, 12])
    assert (
        find_cadence_breaks(frame, timestamp_column="date", expected_interval=timedelta(hours=4))
        == []
    )


def test_find_cadence_breaks_detects_single_missing_bar():
    # The production BTC 4h defect: 16:00 present, 20:00 missing, 00:00 present.
    frame = _dated_frame([12, 16, 24, 28])
    breaks = find_cadence_breaks(
        frame, timestamp_column="date", expected_interval=timedelta(hours=4)
    )
    assert len(breaks) == 1
    assert breaks[0].before == datetime(2025, 6, 2, 16, tzinfo=UTC)
    assert breaks[0].after == datetime(2025, 6, 3, 0, tzinfo=UTC)
    assert breaks[0].actual == timedelta(hours=8)
    assert breaks[0].missing_bars == 1
    assert isinstance(breaks[0], CadenceBreak)


def test_find_cadence_breaks_reports_multi_bar_hole_and_multiple_breaks():
    frame = _dated_frame([0, 4, 20, 24, 40])
    breaks = find_cadence_breaks(
        frame, timestamp_column="date", expected_interval=timedelta(hours=4)
    )
    assert [b.missing_bars for b in breaks] == [3, 3]


def test_find_cadence_breaks_handles_short_frames():
    assert (
        find_cadence_breaks(
            _dated_frame([0]), timestamp_column="date", expected_interval=timedelta(hours=4)
        )
        == []
    )


def test_find_cadence_breaks_is_order_independent():
    """The exporter always sorts before writing, but the primitive must not
    depend on the caller having done so."""
    frame = _dated_frame([24, 12, 16, 28])
    breaks = find_cadence_breaks(
        frame, timestamp_column="date", expected_interval=timedelta(hours=4)
    )
    assert len(breaks) == 1
    assert breaks[0].missing_bars == 1


def test_validate_ohlcv_ignores_cadence_by_default():
    """The default must be a complete no-op -- seven existing call sites and
    10 existing tests depend on it."""
    frame = _dated_frame([12, 16, 24])
    assert validate_ohlcv(frame, timestamp_column="date", location="X/4h") is frame


def test_validate_ohlcv_ignores_cadence_when_policy_is_ignore():
    frame = _dated_frame([12, 16, 24])
    assert (
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="ignore",
        )
        is frame
    )


def test_validate_ohlcv_raises_cadence_break_when_policy_is_raise():
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = _dated_frame([12, 16, 24])
    with pytest.raises(ExportValidationError) as excinfo:
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="raise",
        )
    assert excinfo.value.reason == "cadence_break"
    assert excinfo.value.location == "X/4h"
    assert isinstance(excinfo.value, ValueError)
    assert "2025-06-02 16:00:00+00:00" in str(excinfo.value)


def test_validate_ohlcv_cadence_raise_passes_contiguous_series():
    frame = _dated_frame([0, 4, 8, 12])
    assert (
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="raise",
        )
        is frame
    )


def test_validate_ohlcv_cadence_error_is_a_data_defect_error():
    """The whole point of reusing the #28 taxonomy: the exporter's existing
    guard must already catch this without any new plumbing."""
    from gmx_historical_data.atomic_parquet import DATA_DEFECT_ERRORS
    from gmx_historical_data.ohlcv_validation import ExportValidationError

    frame = _dated_frame([12, 16, 24])
    try:
        validate_ohlcv(
            frame,
            timestamp_column="date",
            location="X/4h",
            expected_interval=timedelta(hours=4),
            cadence_policy="raise",
        )
    except DATA_DEFECT_ERRORS as exc:
        assert isinstance(exc, ExportValidationError)
    else:
        pytest.fail("cadence break was not caught by DATA_DEFECT_ERRORS")
