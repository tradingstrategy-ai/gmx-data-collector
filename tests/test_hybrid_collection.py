"""Integration test for hybrid data collection."""

import os
from pathlib import Path

import pytest

from gmx_historical_data.cli import DataCollector
from gmx_historical_data.config import CollectionConfig


@pytest.mark.skipif(
    not os.getenv("JSON_RPC_ARBITRUM") or not os.getenv("HYPERSYNC_API_TOKEN"),
    reason="Requires JSON_RPC_ARBITRUM and HYPERSYNC_API_TOKEN env vars",
)
@pytest.mark.asyncio
async def test_hybrid_collection_eth():
    """Test hybrid collection for ETH (has Chainlink feed)."""
    config = CollectionConfig(
        output_dir=Path("./test_data"),
        rpc_url=os.getenv("JSON_RPC_ARBITRUM"),
        hypersync_api_token=os.getenv("HYPERSYNC_API_TOKEN"),
    )

    collector = DataCollector(config, use_gmx_api=True)

    # Collect ETH (has Chainlink feed)
    await collector.collect_symbol("ETH", full=True)

    # Verify results
    storage = collector.storage
    df_1h = storage.read_candles("1h", "ETH")

    assert not df_1h.empty, "No 1h candles for ETH"

    # Check coverage
    earliest = df_1h["timestamp"].min()
    latest = df_1h["timestamp"].max()
    time_range_days = (latest - earliest).days

    # With Chainlink backfill, earliest data can go back to 2021 (Chainlink feed launch)
    # Without backfill, it starts from GMX V2 launch (Aug 2023)
    assert earliest.year <= 2023, f"Expected 2023 or earlier, got {earliest.year}"

    # Should extend to recent data — at least 180 days of coverage
    assert time_range_days > 180, f"Expected >180 days, got {time_range_days}"

    print("\n✓ ETH hybrid collection successful")
    print(f"  Coverage: {earliest} → {latest} ({time_range_days} days)")
    print(f"  1h candles: {len(df_1h):,}")


@pytest.mark.skipif(not os.getenv("JSON_RPC_ARBITRUM"), reason="Requires JSON_RPC_ARBITRUM env var")
def test_gmx_v2_genesis_constant():
    """Test GMX V2 genesis constant is defined."""
    from gmx_historical_data.config import (
        GMX_V2_GENESIS_BLOCK,
        GMX_V2_GENESIS_TIMESTAMP,
    )

    # Verify constants are set
    assert GMX_V2_GENESIS_BLOCK == 120_000_000
    assert GMX_V2_GENESIS_TIMESTAMP == 1691366400

    # Verify timestamp corresponds to Aug 2023
    import datetime

    dt = datetime.datetime.fromtimestamp(GMX_V2_GENESIS_TIMESTAMP, tz=datetime.UTC)
    assert dt.year == 2023
    assert dt.month == 8
