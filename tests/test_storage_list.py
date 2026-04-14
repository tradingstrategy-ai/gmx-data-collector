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


def test_save_candles_merges_existing_by_default():
    """save_candles must preserve history already on disk (merge-by-default).

    :ensures: Historical rows are not overwritten when a newer partial dataset is written.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        historic = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2022-01-01", "2022-01-02", "2022-01-03"], utc=True
                ),
                "open": [100.0, 101.0, 102.0],
                "high": [105.0, 106.0, 107.0],
                "low": [99.0, 100.0, 101.0],
                "close": [104.0, 105.0, 106.0],
                "symbol": ["BTC", "BTC", "BTC"],
            }
        )
        storage.save_candles(historic, "1d", "BTC")

        recent = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-03", "2022-01-04"], utc=True),
                "open": [102.0, 103.0],
                "high": [107.0, 108.0],
                "low": [101.0, 102.0],
                "close": [106.0, 107.0],
                "symbol": ["BTC", "BTC"],
            }
        )
        storage.save_candles(recent, "1d", "BTC")

        result = storage.read_candles("1d", "BTC")
        assert len(result) == 4, (
            f"Expected 4 rows (full history merged), got {len(result)}. "
            "save_candles is overwriting instead of merging."
        )
        assert result["timestamp"].min() == pd.Timestamp("2022-01-01", tz="UTC")
        assert result["timestamp"].max() == pd.Timestamp("2022-01-04", tz="UTC")


def test_save_candles_overwrite_flag_replaces_data():
    """save_candles(overwrite=True) must discard existing data.

    :ensures: Explicit overwrite=True replaces file contents entirely.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        historic = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2022-01-01", "2022-01-02", "2022-01-03"], utc=True
                ),
                "open": [100.0, 101.0, 102.0],
                "high": [105.0, 106.0, 107.0],
                "low": [99.0, 100.0, 101.0],
                "close": [104.0, 105.0, 106.0],
                "symbol": ["BTC", "BTC", "BTC"],
            }
        )
        storage.save_candles(historic, "1d", "BTC")

        recent = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-03", "2022-01-04"], utc=True),
                "open": [102.0, 103.0],
                "high": [107.0, 108.0],
                "low": [101.0, 102.0],
                "close": [106.0, 107.0],
                "symbol": ["BTC", "BTC"],
            }
        )
        storage.save_candles(recent, "1d", "BTC", overwrite=True)

        result = storage.read_candles("1d", "BTC")
        assert len(result) == 2, "overwrite=True should replace, not merge."
        assert result["timestamp"].min() == pd.Timestamp("2022-01-03", tz="UTC")


def test_save_candles_deduplicates_overlapping_timestamps():
    """Overlapping timestamps keep the latest-written value (keep='last').

    :ensures: Duplicate timestamps from newer write win over older stored value.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        first = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-01"], utc=True),
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [104.0],
                "symbol": ["ETH"],
            }
        )
        storage.save_candles(first, "1h", "ETH")

        updated = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-01"], utc=True),
                "open": [200.0],
                "high": [210.0],
                "low": [190.0],
                "close": [205.0],
                "symbol": ["ETH"],
            }
        )
        storage.save_candles(updated, "1h", "ETH")

        result = storage.read_candles("1h", "ETH")
        assert len(result) == 1
        assert result.iloc[0]["open"] == 200.0, "Newer write should win on duplicate timestamp."


def test_save_candles_raises_on_tz_naive_timestamp():
    """save_candles must raise ValueError for tz-naive timestamp columns.

    :ensures: Callers receive a clear error instead of a cryptic Polars panic.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-01", "2022-01-02"]),  # no tz
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [99.0, 100.0],
                "close": [104.0, 105.0],
                "symbol": ["BTC", "BTC"],
            }
        )
        with pytest.raises(ValueError, match="timezone-naive"):
            storage.save_candles(df, "1h", "BTC")
