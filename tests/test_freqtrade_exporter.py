"""Tests for Freqtrade exporter."""

import tempfile
from pathlib import Path

import pandas as pd
import pyarrow.feather as feather
import pytest

from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.freqtrade_exporter import FreqtradeExporter


@pytest.fixture
def sample_storage():
    """Create storage with sample data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        # Create test candles for ETH
        df_eth = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2024-01-01 00:00:00",
                        "2024-01-01 01:00:00",
                        "2024-01-01 02:00:00",
                    ],
                    utc=True,
                ),
                "open": [2000.0, 2010.0, 2005.0],
                "high": [2050.0, 2060.0, 2055.0],
                "low": [1990.0, 2000.0, 1995.0],
                "close": [2010.0, 2005.0, 2020.0],
                "symbol": ["ETH", "ETH", "ETH"],
            }
        )
        storage.save_candles(df_eth, "1h", "ETH")
        storage.save_candles(df_eth, "4h", "ETH")

        # Create test candles for BTC
        df_btc = df_eth.assign(
            symbol="BTC",
            open=[40000.0, 40100.0, 40050.0],
            high=[40500.0, 40600.0, 40550.0],
            low=[39900.0, 40000.0, 39950.0],
            close=[40100.0, 40050.0, 40200.0],
        )
        storage.save_candles(df_btc, "1h", "BTC")

        yield Path(tmpdir)


def test_export_creates_freqtrade_format(sample_storage):
    """Test export creates files with correct freqtrade futures format."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        result = exporter.export()

        # Check files created in gmx/futures subdirectory
        futures_dir = Path(output_dir) / "gmx" / "futures"
        assert futures_dir.exists()

        # Futures format: BASE_QUOTE_SETTLE-timeframe-futures.feather
        eth_1h = futures_dir / "ETH_USDC_USDC-1h-futures.feather"
        assert eth_1h.exists()

        # Read and verify format
        df = pd.read_feather(eth_1h)
        assert list(df.columns) == ["date", "open", "high", "low", "close", "volume"]
        assert len(df) == 3
        assert df["volume"].iloc[0] == 0.0


def test_export_specific_symbols(sample_storage):
    """Test export filters by symbol."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        result = exporter.export(symbols=["ETH"])

        futures_dir = Path(output_dir) / "gmx" / "futures"
        assert (futures_dir / "ETH_USDC_USDC-1h-futures.feather").exists()
        assert not (futures_dir / "BTC_USDC_USDC-1h-futures.feather").exists()


def test_export_specific_timeframes(sample_storage):
    """Test export filters by timeframe."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        result = exporter.export(timeframes=["1h"])

        futures_dir = Path(output_dir) / "gmx" / "futures"
        assert (futures_dir / "ETH_USDC_USDC-1h-futures.feather").exists()
        assert not (futures_dir / "ETH_USDC_USDC-4h-futures.feather").exists()


def test_export_returns_stats(sample_storage):
    """Test export returns statistics."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        result = exporter.export()

        assert "ETH" in result
        assert "BTC" in result
        # ETH: 1h + 4h = 2 ohlcv + 2 mark + 2 index = 6
        assert result["ETH"]["ohlcv_files"] == 2
        assert result["ETH"]["mark_files"] == 2
        assert result["ETH"]["index_files"] == 2
        assert result["ETH"]["files"] == 6
        # BTC: 1h = 1 ohlcv + 1 mark + 1 index = 3
        assert result["BTC"]["ohlcv_files"] == 1
        assert result["BTC"]["mark_files"] == 1
        assert result["BTC"]["index_files"] == 1
        assert result["BTC"]["files"] == 3


def test_export_creates_index_files(sample_storage):
    """Test export creates index price files matching mark price data."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export()

        futures_dir = Path(output_dir) / "gmx" / "futures"
        index_file = futures_dir / "ETH_USDC_USDC-1h-index.feather"
        mark_file = futures_dir / "ETH_USDC_USDC-1h-mark.feather"

        assert index_file.exists()

        df_index = pd.read_feather(index_file)
        df_mark = pd.read_feather(mark_file)
        pd.testing.assert_frame_equal(df_index, df_mark)


def test_date_column_is_datetime(sample_storage):
    """Test date column is proper datetime type."""
    with tempfile.TemporaryDirectory() as output_dir:
        exporter = FreqtradeExporter(sample_storage, Path(output_dir))
        exporter.export()

        df = pd.read_feather(
            Path(output_dir) / "gmx" / "futures" / "ETH_USDC_USDC-1h-futures.feather"
        )
        assert pd.api.types.is_datetime64_any_dtype(df["date"])
