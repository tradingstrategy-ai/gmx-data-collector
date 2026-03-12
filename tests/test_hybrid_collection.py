"""Integration test for hybrid data collection."""

import pytest
import os
from pathlib import Path
from gmx_historical_data.cli import DataCollector
from gmx_historical_data.config import CollectionConfig


@pytest.mark.skipif(not os.getenv("JSON_RPC_ARBITRUM"), reason="Requires JSON_RPC_ARBITRUM env var")
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

    # Should have historical data from GMX V2 launch
    # GMX V2 launched Aug 2023, GMX API has ~6 months
    # So earliest should be around Aug 2023
    assert earliest.year == 2023, f"Expected 2023, got {earliest.year}"
    assert earliest.month >= 8, f"Expected Aug or later, got month {earliest.month}"

    # Should extend to recent data
    assert time_range_days > 180, f"Expected >180 days, got {time_range_days}"

    print(f"\n✓ ETH hybrid collection successful")
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

    dt = datetime.datetime.fromtimestamp(GMX_V2_GENESIS_TIMESTAMP, tz=datetime.timezone.utc)
    assert dt.year == 2023
    assert dt.month == 8
