"""Tests for data coverage analyzer."""

import tempfile
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.data_coverage_analyzer import (
    DataCoverageAnalyzer,
    SymbolCoverage,
    TimeframeCoverage,
)


@pytest.fixture
def storage_dir():
    """Create temporary storage directory.

    :return: Temporary directory path
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def storage(storage_dir):
    """Create ParquetStorage instance.

    :param storage_dir: Storage directory fixture
    :return: ParquetStorage instance
    """
    return ParquetStorage(storage_dir)


@pytest.fixture
def mock_cache():
    """Create mock BlockTimestampCache.

    :return: Mock cache instance
    """
    cache = Mock()
    # Mock simple timestamp->block conversion: block = timestamp / 4 + 100_000_000
    # Inverse: timestamp = (block - 100_000_000) * 4
    cache.get_block_for_timestamp = lambda ts: int(ts / 4) + 100_000_000
    cache.get_timestamp_for_block = lambda block: int((block - 100_000_000) * 4)
    return cache


def test_no_existing_data(storage_dir):
    """Test analyzer when no data exists for symbol.

    :param storage_dir: Storage directory fixture
    """
    analyzer = DataCoverageAnalyzer(storage_dir)
    coverage = analyzer.analyze_symbol_coverage("NONEXISTENT")

    assert coverage.symbol == "NONEXISTENT"
    assert coverage.has_data is False
    assert coverage.earliest_timestamp is None
    assert coverage.latest_timestamp is None
    assert len(coverage.timeframe_coverage) == 0


def test_single_timeframe_coverage(storage_dir, storage):
    """Test coverage analysis with single timeframe.

    :param storage_dir: Storage directory fixture
    :param storage: ParquetStorage fixture
    """
    # Create test data for single timeframe (1h)
    timestamps = pd.to_datetime(
        ["2024-01-01 00:00:00", "2024-01-01 01:00:00", "2024-01-01 02:00:00"],
        utc=True
    )
    df = pd.DataFrame({
        "timestamp": timestamps,
        "open": [100.0, 101.0, 102.0],
        "high": [105.0, 106.0, 107.0],
        "low": [99.0, 100.0, 101.0],
        "close": [104.0, 105.0, 106.0],
        "symbol": ["ETH", "ETH", "ETH"],
    })
    storage.save_candles(df, "1h", "ETH")

    # Analyze coverage
    analyzer = DataCoverageAnalyzer(storage_dir)
    coverage = analyzer.analyze_symbol_coverage("ETH")

    assert coverage.symbol == "ETH"
    assert coverage.has_data is True
    assert coverage.earliest_timestamp == int(timestamps[0].timestamp())
    assert coverage.latest_timestamp == int(timestamps[-1].timestamp())
    assert len(coverage.timeframe_coverage) == 1

    # Check 1h timeframe coverage
    tf_coverage = coverage.timeframe_coverage["1h"]
    assert tf_coverage.timeframe == "1h"
    assert tf_coverage.earliest == int(timestamps[0].timestamp())
    assert tf_coverage.latest == int(timestamps[-1].timestamp())
    assert tf_coverage.candle_count == 3


def test_multiple_timeframe_coverage(storage_dir, storage):
    """Test coverage analysis with multiple timeframes.

    :param storage_dir: Storage directory fixture
    :param storage: ParquetStorage fixture
    """
    # Create data for 1h timeframe
    timestamps_1h = pd.to_datetime(
        ["2024-01-01 00:00:00", "2024-01-01 01:00:00"],
        utc=True
    )
    df_1h = pd.DataFrame({
        "timestamp": timestamps_1h,
        "open": [100.0, 101.0],
        "high": [105.0, 106.0],
        "low": [99.0, 100.0],
        "close": [104.0, 105.0],
        "symbol": ["BTC", "BTC"],
    })
    storage.save_candles(df_1h, "1h", "BTC")

    # Create data for 4h timeframe (starts earlier)
    timestamps_4h = pd.to_datetime(
        ["2023-12-31 20:00:00", "2024-01-01 00:00:00"],
        utc=True
    )
    df_4h = pd.DataFrame({
        "timestamp": timestamps_4h,
        "open": [99.0, 100.0],
        "high": [103.0, 105.0],
        "low": [98.0, 99.0],
        "close": [102.0, 104.0],
        "symbol": ["BTC", "BTC"],
    })
    storage.save_candles(df_4h, "4h", "BTC")

    # Create data for 1d timeframe (ends later)
    timestamps_1d = pd.to_datetime(
        ["2024-01-01 00:00:00", "2024-01-02 00:00:00"],
        utc=True
    )
    df_1d = pd.DataFrame({
        "timestamp": timestamps_1d,
        "open": [100.0, 110.0],
        "high": [120.0, 125.0],
        "low": [95.0, 105.0],
        "close": [115.0, 120.0],
        "symbol": ["BTC", "BTC"],
    })
    storage.save_candles(df_1d, "1d", "BTC")

    # Analyze coverage
    analyzer = DataCoverageAnalyzer(storage_dir)
    coverage = analyzer.analyze_symbol_coverage("BTC")

    assert coverage.symbol == "BTC"
    assert coverage.has_data is True
    # Earliest should be from 4h timeframe
    assert coverage.earliest_timestamp == int(timestamps_4h[0].timestamp())
    # Latest should be from 1d timeframe
    assert coverage.latest_timestamp == int(timestamps_1d[-1].timestamp())
    assert len(coverage.timeframe_coverage) == 3

    # Verify each timeframe
    assert "1h" in coverage.timeframe_coverage
    assert "4h" in coverage.timeframe_coverage
    assert "1d" in coverage.timeframe_coverage


def test_earliest_gap_across_timeframes(storage_dir, storage):
    """Test that analyzer finds earliest gap across all timeframes.

    :param storage_dir: Storage directory fixture
    :param storage: ParquetStorage fixture
    """
    # Create data starting at different times for different timeframes
    # 1m: starts at 2024-01-02
    timestamps_1m = pd.to_datetime(
        ["2024-01-02 00:00:00", "2024-01-02 00:01:00"],
        utc=True
    )
    df_1m = pd.DataFrame({
        "timestamp": timestamps_1m,
        "open": [100.0, 101.0],
        "high": [105.0, 106.0],
        "low": [99.0, 100.0],
        "close": [104.0, 105.0],
        "symbol": ["ETH", "ETH"],
    })
    storage.save_candles(df_1m, "1min", "ETH")

    # 1h: starts at 2024-01-01 (earlier - this is the gap we want to find)
    timestamps_1h = pd.to_datetime(
        ["2024-01-01 00:00:00", "2024-01-01 01:00:00"],
        utc=True
    )
    df_1h = pd.DataFrame({
        "timestamp": timestamps_1h,
        "open": [100.0, 101.0],
        "high": [105.0, 106.0],
        "low": [99.0, 100.0],
        "close": [104.0, 105.0],
        "symbol": ["ETH", "ETH"],
    })
    storage.save_candles(df_1h, "1h", "ETH")

    # Analyze coverage
    analyzer = DataCoverageAnalyzer(storage_dir)
    coverage = analyzer.analyze_symbol_coverage("ETH")

    # Earliest should be from 1h (even though 1m has more recent data)
    assert coverage.earliest_timestamp == int(timestamps_1h[0].timestamp())


def test_get_missing_block_range(storage_dir, storage, mock_cache):
    """Test calculating missing block range.

    :param storage_dir: Storage directory fixture
    :param storage: ParquetStorage fixture
    :param mock_cache: Mock cache fixture
    """
    # Create data starting at 2024-01-10
    timestamps = pd.to_datetime(
        ["2024-01-10 00:00:00", "2024-01-10 01:00:00"],
        utc=True
    )
    df = pd.DataFrame({
        "timestamp": timestamps,
        "open": [100.0, 101.0],
        "high": [105.0, 106.0],
        "low": [99.0, 100.0],
        "close": [104.0, 105.0],
        "symbol": ["LINK", "LINK"],
    })
    storage.save_candles(df, "1h", "LINK")

    analyzer = DataCoverageAnalyzer(storage_dir)
    coverage = analyzer.analyze_symbol_coverage("LINK")

    # Calculate missing range with genesis at block 120_000_000
    genesis_block = 120_000_000
    safety_margin = 1000

    start_block, end_block = analyzer.get_missing_block_range(
        coverage, mock_cache, genesis_block, safety_margin
    )

    # Should return range from genesis to earliest_data_block + safety_margin
    earliest_ts = coverage.earliest_timestamp
    earliest_block = mock_cache.get_block_for_timestamp(earliest_ts)

    assert start_block == genesis_block
    assert end_block == earliest_block + safety_margin


def test_no_missing_range_when_genesis_covered(storage_dir, storage, mock_cache):
    """Test no missing range when data exists before genesis.

    :param storage_dir: Storage directory fixture
    :param storage: ParquetStorage fixture
    :param mock_cache: Mock cache fixture
    """
    # Create data starting very early (before typical genesis)
    timestamps = pd.to_datetime(
        ["2023-01-01 00:00:00", "2023-01-01 01:00:00"],
        utc=True
    )
    df = pd.DataFrame({
        "timestamp": timestamps,
        "open": [100.0, 101.0],
        "high": [105.0, 106.0],
        "low": [99.0, 100.0],
        "close": [104.0, 105.0],
        "symbol": ["BTC", "BTC"],
    })
    storage.save_candles(df, "1h", "BTC")

    analyzer = DataCoverageAnalyzer(storage_dir)
    coverage = analyzer.analyze_symbol_coverage("BTC")

    # Use a genesis block that's after the data
    # Data is from 2023-01-01 = 1672531200 seconds
    # For genesis_ts > 1672531200: (genesis_block - 100M) * 4 > 1672531200
    # genesis_block > 518M, so use 520M
    genesis_block = 520_000_000  # Corresponds to timestamp ~2053 (far in the future)

    start_block, end_block = analyzer.get_missing_block_range(
        coverage, mock_cache, genesis_block
    )

    # Should return (None, None) when data exists before genesis
    assert start_block is None
    assert end_block is None


def test_get_missing_range_no_data(storage_dir, mock_cache):
    """Test missing range when no data exists.

    :param storage_dir: Storage directory fixture
    :param mock_cache: Mock cache fixture
    """
    analyzer = DataCoverageAnalyzer(storage_dir)
    coverage = analyzer.analyze_symbol_coverage("NODATA")

    genesis_block = 120_000_000

    start_block, end_block = analyzer.get_missing_block_range(
        coverage, mock_cache, genesis_block
    )

    # Should return (genesis_block, None) when no data exists
    assert start_block == genesis_block
    assert end_block is None
