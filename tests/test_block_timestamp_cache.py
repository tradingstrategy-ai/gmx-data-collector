"""Tests for block-timestamp cache system.

This module tests the BlockTimestampCache which provides efficient
timestamp↔block conversions using sampled blocks and linear interpolation.
"""

from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest
from web3 import Web3

from gmx_historical_data.block_timestamp_cache import BlockTimestampCache
from gmx_historical_data.config import (
    BLOCK_SAMPLE_INTERVAL,
    GMX_V2_GENESIS_BLOCK,
)


@pytest.fixture
def mock_web3():
    """Create a mock Web3 instance with predictable block timestamps.

    Returns blocks with timestamps that increase linearly:
    - Block 120_000_000: timestamp 1691366400
    - Block 120_001_000: timestamp 1691366650 (+250 seconds, 0.25s/block)
    - Block 120_002_000: timestamp 1691366900 (+250 seconds, 0.25s/block)
    - etc.
    """
    web3 = Mock(spec=Web3)
    eth = Mock()

    # Define block time calculation
    def get_block(block_number):
        """Mock get_block that returns predictable timestamps."""
        base_block = GMX_V2_GENESIS_BLOCK
        base_timestamp = 1691366400  # Aug 7, 2023 00:00:00 UTC

        # 0.25 seconds per block
        blocks_elapsed = block_number - base_block
        timestamp = base_timestamp + int(blocks_elapsed * 0.25)

        block_obj = MagicMock()
        block_obj.timestamp = timestamp
        block_obj.number = block_number
        return block_obj

    # Mock eth.block_number property
    eth.get_block = get_block
    type(eth).block_number = property(lambda self: GMX_V2_GENESIS_BLOCK + 50000)

    web3.eth = eth
    return web3


@pytest.fixture
def cache_file(tmp_path):
    """Create a temporary cache file path.

    :param tmp_path: pytest tmp_path fixture
    :return: Path to cache file in temporary .cache directory
    """
    cache_dir = tmp_path / ".cache"
    cache_dir.mkdir(exist_ok=True)
    return cache_dir / "block_timestamps.parquet"


def test_build_cache_from_scratch(mock_web3, cache_file):
    """Test building cache from scratch.

    Should:
    - Sample blocks at BLOCK_SAMPLE_INTERVAL intervals
    - Store in parquet format with block and timestamp columns
    - Create cache file on disk
    """
    cache = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )

    # Build cache from genesis to +10000 blocks
    end_block = GMX_V2_GENESIS_BLOCK + 10000
    cache.build_cache(end_block=end_block)

    # Verify cache file exists
    assert cache_file.exists()

    # Verify cache was loaded
    assert cache.cache_df is not None

    # Verify cache contents
    df = pd.read_parquet(cache_file)
    assert len(df) == 11  # 0, 1000, 2000, ..., 10000 = 11 samples
    assert "block" in df.columns
    assert "timestamp" in df.columns
    assert df["block"].dtype == "uint64"
    assert df["timestamp"].dtype == "uint64"

    # Verify sample intervals
    assert df["block"].iloc[0] == GMX_V2_GENESIS_BLOCK
    assert df["block"].iloc[-1] == end_block
    assert all(df["block"].diff().dropna() == BLOCK_SAMPLE_INTERVAL)


def test_get_block_for_timestamp_interpolation(mock_web3, cache_file):
    """Test converting timestamp to block using interpolation.

    Should use linear interpolation between sampled blocks.
    """
    cache = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )

    # Build cache
    end_block = GMX_V2_GENESIS_BLOCK + 10000
    cache.build_cache(end_block=end_block)

    # Test exact sample point
    base_timestamp = 1691366400
    block = cache.get_block_for_timestamp(base_timestamp)
    assert block == GMX_V2_GENESIS_BLOCK

    # Test interpolation between samples
    # At block 120_000_500, timestamp should be base + (500 * 0.25) = base + 125
    target_timestamp = base_timestamp + 125
    block = cache.get_block_for_timestamp(target_timestamp)
    assert abs(block - (GMX_V2_GENESIS_BLOCK + 500)) < 10  # Allow small error

    # Test timestamp beyond cache (should extrapolate)
    far_timestamp = base_timestamp + 5000
    block = cache.get_block_for_timestamp(far_timestamp)
    assert block > GMX_V2_GENESIS_BLOCK


def test_get_timestamp_for_block_interpolation(mock_web3, cache_file):
    """Test converting block to timestamp using interpolation.

    Should use linear interpolation between sampled blocks.
    """
    cache = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )

    # Build cache
    end_block = GMX_V2_GENESIS_BLOCK + 10000
    cache.build_cache(end_block=end_block)

    # Test exact sample point
    base_timestamp = 1691366400
    timestamp = cache.get_timestamp_for_block(GMX_V2_GENESIS_BLOCK)
    assert timestamp == base_timestamp

    # Test interpolation between samples
    # At block 120_000_500, timestamp should be base + (500 * 0.25) = base + 125
    target_block = GMX_V2_GENESIS_BLOCK + 500
    timestamp = cache.get_timestamp_for_block(target_block)
    expected_timestamp = base_timestamp + 125
    assert abs(timestamp - expected_timestamp) < 5  # Allow small error


def test_incremental_update(mock_web3, cache_file):
    """Test updating cache with new blocks.

    Should:
    - Load existing cache
    - Append new samples from last block to end_block
    - Preserve existing samples
    """
    cache = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )

    # Build initial cache
    initial_end = GMX_V2_GENESIS_BLOCK + 5000
    cache.build_cache(end_block=initial_end)
    initial_len = len(cache.cache_df)

    # Update cache with more blocks
    new_end = GMX_V2_GENESIS_BLOCK + 10000
    cache.update_cache(end_block=new_end)

    # Verify cache was extended
    assert len(cache.cache_df) > initial_len
    assert cache.cache_df["block"].iloc[-1] == new_end

    # Verify existing samples are preserved
    df = pd.read_parquet(cache_file)
    assert df["block"].iloc[0] == GMX_V2_GENESIS_BLOCK
    assert df["block"].iloc[-1] == new_end


def test_cache_auto_loads_on_timestamp_conversion(mock_web3, cache_file):
    """Test cache automatically loads from disk when needed.

    Should:
    - Start with cache_df = None
    - Auto-load cache when get_block_for_timestamp is called
    - Use loaded cache for conversion
    """
    # Build cache first
    cache1 = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )
    cache1.build_cache(end_block=GMX_V2_GENESIS_BLOCK + 10000)

    # Create new cache instance (cache_df should be None initially)
    cache2 = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )

    assert cache2.cache_df is None

    # Call conversion method - should auto-load
    base_timestamp = 1691366400
    block = cache2.get_block_for_timestamp(base_timestamp)

    # Verify cache was loaded
    assert cache2.cache_df is not None
    assert block == GMX_V2_GENESIS_BLOCK


def test_stale_cache_auto_updates(mock_web3, cache_file):
    """Test stale cache automatically updates when accessed.

    Should:
    - Detect cache is stale (> CACHE_STALE_THRESHOLD behind current block)
    - Auto-update cache to current block
    - Use updated cache for conversion
    """
    # Build cache that will be stale
    stale_end = GMX_V2_GENESIS_BLOCK + 5000
    cache1 = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )
    cache1.build_cache(end_block=stale_end)

    # Mock current block to be far ahead (making cache stale)
    # Current block is genesis + 50000 (from mock_web3 fixture)
    # Cache ends at genesis + 5000
    # Difference = 45000 > CACHE_STALE_THRESHOLD (10000)

    # Create new cache instance
    cache2 = BlockTimestampCache(
        cache_path=cache_file,
        web3=mock_web3,
        genesis_block=GMX_V2_GENESIS_BLOCK,
    )

    # Call conversion - should detect staleness and update
    base_timestamp = 1691366400
    cache2.get_block_for_timestamp(base_timestamp)

    # Verify cache was updated to near current block
    assert cache2.cache_df is not None
    assert cache2.cache_df["block"].iloc[-1] > stale_end
