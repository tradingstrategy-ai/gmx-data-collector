"""Test that FreqTrade exporter produces nanosecond timestamps (Polars)."""

import pandas as pd
import polars as pl

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


def test_transform_dataframe_timestamp_unit(tmp_path):
    """_transform_dataframe should produce ns-precision UTC timestamps."""
    df = pl.DataFrame(
        {
            "timestamp": pl.Series(["2026-01-01 00:00:00", "2026-01-01 01:00:00"])
            .str.to_datetime(time_unit="us")
            .dt.replace_time_zone("UTC"),
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
        }
    )
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    result = exporter._transform_dataframe(df)

    assert result.schema["date"] == pl.Datetime("ns", "UTC"), (
        f"Expected Datetime(ns, UTC), got {result.schema['date']}"
    )


def test_transform_funding_rate_timestamp_unit(tmp_path):
    """_transform_funding_rate should produce ns-precision UTC timestamps."""
    df = pl.DataFrame(
        {
            "timestamp": pl.Series(["2026-01-01 00:00:00", "2026-01-01 01:00:00"])
            .str.to_datetime(time_unit="us")
            .dt.replace_time_zone("UTC"),
            "funding_rate_hourly": [0.000001, 0.000002],
            "funding_rate": [2.8e-10, 5.6e-10],
        }
    )
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    result = exporter._transform_funding_rate(df)

    assert result.schema["date"] == pl.Datetime("ns", "UTC"), (
        f"Expected Datetime(ns, UTC), got {result.schema['date']}"
    )


def test_transform_mark_price_timestamp_unit(tmp_path):
    """_transform_mark_price should produce ns-precision UTC timestamps."""
    df = pl.DataFrame(
        {
            "timestamp": pl.Series(["2026-01-01 00:00:00", "2026-01-01 01:00:00"])
            .str.to_datetime(time_unit="us")
            .dt.replace_time_zone("UTC"),
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
        }
    )
    exporter = FreqtradeExporter(data_dir=tmp_path, output_dir=tmp_path)
    result = exporter._transform_mark_price(df)

    assert result.schema["date"] == pl.Datetime("ns", "UTC"), (
        f"Expected Datetime(ns, UTC), got {result.schema['date']}"
    )


def _save(storage: ParquetStorage, timestamps: list[str], value: float) -> None:
    """Write a minimal OHLCV frame for ETH/1h into the raw candle store."""
    storage.save_candles(
        pd.DataFrame(
            {
                "timestamp": pd.to_datetime(timestamps, utc=True),
                "open": [value] * len(timestamps),
                "high": [value] * len(timestamps),
                "low": [value] * len(timestamps),
                "close": [value] * len(timestamps),
                "symbol": ["ETH"] * len(timestamps),
            }
        ),
        "1h",
        "ETH",
    )


def test_export_merges_destination_with_mismatched_time_unit(tmp_path):
    """A destination feather at a different time unit must not wedge the export.

    ``_transform_*`` always emits ``date`` at ns, but a file already on disk may
    carry us precision (older exporter, foreign tool, partially-completed run).
    ``pl.concat`` refuses to vstack mismatched temporal dtypes, so without
    normalisation a single such file aborts the whole export run.
    """
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorage(storage_dir)
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    _save(storage, ["2024-01-01T00:00:00", "2024-01-01T01:00:00"], 1.0)
    exporter = FreqtradeExporter(storage_dir, output_dir)
    exporter.export(symbols=["ETH"], timeframes=["1h"])

    feather_path = next(output_dir.rglob("ETH_USDC_USDC-1h-futures.feather"))

    # Rewrite the destination at microsecond precision, as a foreign writer would.
    pl.read_ipc(feather_path, memory_map=False).with_columns(
        pl.col("date").cast(pl.Datetime("us", "UTC"))
    ).write_ipc(feather_path, compression="zstd")
    assert pl.read_ipc(feather_path, memory_map=False).schema["date"] == pl.Datetime("us", "UTC")

    # Re-export with newer candles: must merge rather than raise on vstack.
    _save(storage, ["2024-01-01T02:00:00"], 2.0)
    exporter.export(symbols=["ETH"], timeframes=["1h"])

    merged = pl.read_ipc(feather_path, memory_map=False)
    assert merged.schema["date"] == pl.Datetime("ns", "UTC")
    assert merged.height == 3, f"history not preserved: {merged.height} rows"
    assert merged["date"].is_sorted()
