"""Test that FreqTrade exporter produces millisecond timestamps."""

import pandas as pd
import pytest

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter


def test_transform_dataframe_timestamp_unit(tmp_path):
    """_transform_dataframe should produce ms-precision timestamps."""
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(["2026-01-01 00:00:00", "2026-01-01 01:00:00"], utc=True),
        "open": [100.0, 101.0],
        "high": [102.0, 103.0],
        "low": [99.0, 100.0],
        "close": [101.0, 102.0],
    })
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    result = exporter._transform_dataframe(df)

    # FreqTrade reads timestamps with unit="ms"
    assert "datetime64[ms" in str(result["date"].dtype), (
        f"Expected ms precision, got {result['date'].dtype}"
    )


def test_transform_funding_rate_timestamp_unit(tmp_path):
    """_transform_funding_rate should produce ms-precision timestamps."""
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(["2026-01-01 00:00:00", "2026-01-01 01:00:00"], utc=True),
        "funding_rate_hourly": [0.000001, 0.000002],
        "funding_rate": [2.8e-10, 5.6e-10],
    })
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    result = exporter._transform_funding_rate(df)

    assert "datetime64[ms" in str(result["date"].dtype), (
        f"Expected ms precision, got {result['date'].dtype}"
    )


def test_transform_mark_price_timestamp_unit(tmp_path):
    """_transform_mark_price should produce ms-precision timestamps."""
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(["2026-01-01 00:00:00", "2026-01-01 01:00:00"], utc=True),
        "open": [100.0, 101.0],
        "high": [102.0, 103.0],
        "low": [99.0, 100.0],
        "close": [101.0, 102.0],
    })
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    result = exporter._transform_mark_price(df)

    assert "datetime64[ms" in str(result["date"].dtype), (
        f"Expected ms precision, got {result['date'].dtype}"
    )
