"""Tests for storage list methods."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from gmx_historical_data.storage import ParquetStorage


def test_list_symbols_empty():
    """Test list_symbols returns empty list for empty directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))
        assert storage.list_symbols() == []


def test_list_symbols_with_data():
    """Test list_symbols returns symbols with candle data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        # Create test candles
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [99.0, 100.0],
                "close": [104.0, 105.0],
                "symbol": ["ETH", "ETH"],
            }
        )
        storage.save_candles(df, "1h", "ETH")
        storage.save_candles(df.assign(symbol="BTC"), "1h", "BTC")

        symbols = storage.list_symbols()
        assert sorted(symbols) == ["BTC", "ETH"]


def test_list_timeframes_for_symbol():
    """Test list_timeframes returns available timeframes for a symbol."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01"], utc=True),
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [104.0],
                "symbol": ["ETH"],
            }
        )
        storage.save_candles(df, "1h", "ETH")
        storage.save_candles(df, "4h", "ETH")
        storage.save_candles(df, "1d", "ETH")

        timeframes = storage.list_timeframes("ETH")
        assert sorted(timeframes) == ["1d", "1h", "4h"]


def test_list_timeframes_unknown_symbol():
    """Test list_timeframes returns empty for unknown symbol."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))
        assert storage.list_timeframes("UNKNOWN") == []
