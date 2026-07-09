"""Tests for scripts/validate_price_continuity.py."""

import importlib.util
from pathlib import Path

import pandas as pd

_SPEC = importlib.util.spec_from_file_location(
    "validate_price_continuity",
    Path(__file__).parent.parent / "scripts" / "validate_price_continuity.py",
)
vpc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(vpc)


def _frame(closes, start="2024-01-01", freq="1h", ts_col="date"):
    idx = pd.date_range(start, periods=len(closes), freq=freq, tz="UTC")
    return pd.DataFrame(
        {ts_col: idx, "open": closes, "high": closes, "low": closes,
         "close": closes, "volume": 0.0}
    )


def test_clean_series_passes():
    report = vpc.validate_frame(_frame([1.0e-5, 1.1e-5, 1.05e-5, 0.95e-5]), timeframe="1h")
    assert report.decade_jumps == 0
    assert report.missing_bars == 0
    assert report.ok


def test_timestamp_column_accepted():
    df = _frame([1.0e-5, 1.1e-5, 1.05e-5], ts_col="timestamp")
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.ok


def test_decade_fold_detected():
    df = _frame([2.1e-5, 2.2e-6, 2.15e-5, 2.3e-6])  # PEPE-style 10x flips
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.decade_jumps >= 2
    assert not report.ok


def test_unit_cliff_detected():
    df = _frame([1.9e5, 1.9e5, 1.9e-5, 2.0e-5])  # SHIB-style 1e10 cliff
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.decade_jumps >= 1
    assert not report.ok


def test_grid_gap_detected():
    df = _frame([1.0e-5, 1.0e-5, 1.0e-5, 1.0e-5])
    df = df.drop(index=[1, 2]).reset_index(drop=True)  # BONK-style holes
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.missing_bars == 2
    assert not report.ok


def test_zero_and_nan_flagged():
    df = _frame([1.0e-5, 0.0, float("nan"), 1.0e-5])
    report = vpc.validate_frame(df, timeframe="1h")
    assert report.zero_or_nan == 2
    assert not report.ok


def test_timeframe_from_name():
    assert vpc._timeframe_from_name(Path("PEPE_USDC_USDC-1h-futures.feather")) == "1h"
    assert vpc._timeframe_from_name(Path("1h.parquet")) == "1h"
    assert vpc._timeframe_from_name(Path("1m.parquet")) == "1m"
