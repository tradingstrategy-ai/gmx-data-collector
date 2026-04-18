"""Tests for Freqtrade exporter parquet cleanup behaviour."""

from pathlib import Path

import pandas as pd

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from gmx_historical_data.storage import ParquetStorage


def _make_candles(symbols, dates):
    """Build a minimal candle DataFrame for testing.

    :param symbols: List of symbol strings.
    :param dates: List of date strings.
    :returns: pandas DataFrame with OHLCV columns.
    """
    rows = []
    for sym in symbols:
        for d in dates:
            rows.append(
                {
                    "timestamp": pd.Timestamp(d, tz="UTC"),
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "symbol": sym,
                }
            )
    return pd.DataFrame(rows)


def test_export_cleanup_removes_parquet_by_default(tmp_path):
    """Test that keep_parquet=False (default) removes source parquet after export."""
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorage(storage_dir)
    storage.save_candles(_make_candles(["ETH"], ["2024-01-01"]), "1h", "ETH")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    exporter = FreqtradeExporter(storage_dir, output_dir)
    exporter.export(symbols=["ETH"], timeframes=["1h"], keep_parquet=False)

    # Verify feather was created (futures + mark + index = 3 files for one symbol/timeframe)
    feather_files = list(output_dir.rglob("*.feather"))
    assert len(feather_files) >= 1, "feather export must succeed before cleanup is valid"

    # After export with keep_parquet=False, the source parquet should be gone
    candle_path = storage_dir / "candles" / "arbitrum" / "ETH" / "1h.parquet"
    assert not candle_path.exists()


def test_export_cleanup_kept_when_requested(tmp_path):
    """Test that keep_parquet=True leaves the source parquet file in place."""
    storage_dir = tmp_path / "data"
    storage_dir.mkdir()
    storage = ParquetStorage(storage_dir)
    storage.save_candles(_make_candles(["ETH"], ["2024-01-01"]), "1h", "ETH")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    exporter = FreqtradeExporter(storage_dir, output_dir)
    exporter.export(symbols=["ETH"], timeframes=["1h"], keep_parquet=True)

    # Source parquet should still exist
    candle_path = storage_dir / "candles" / "arbitrum" / "ETH" / "1h.parquet"
    assert candle_path.exists()


def test_makefile_export_supports_keep_flag():
    """Test that the Makefile declares KEEP variable and passes it to export-freqtrade.

    :raises AssertionError: If KEEP variable or --keep flag is missing from Makefile.
    """
    makefile = Path(__file__).parent.parent / "Makefile"
    content = makefile.read_text()
    assert "KEEP ?=" in content, "Makefile must declare KEEP variable with default"
    assert "$(KEEP)" in content, (
        "Makefile must wire $(KEEP) into the export-freqtrade CLI invocation"
    )
