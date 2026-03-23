"""Integration test for event-based data collection."""

import os
from pathlib import Path

import pytest
from web3 import Web3

from gmx_historical_data.event_aggregator import aggregate_events_to_ohlcv
from gmx_historical_data.gmx_event_collector import GMXEventCollector
from gmx_historical_data.gmx_market_mapper import GMXMarketMapper
from gmx_historical_data.storage import ParquetStorage


@pytest.mark.asyncio
async def test_event_based_collection_flow(tmp_path: Path):
    """Test complete event-based collection flow for ETH.

    Note: This test may fail if HyperSync API requires authentication
    or is rate-limited. In production, use API tokens for reliable access.

    Note: Block number (180_000_000) may become stale - update if test fails
    due to block being too far in the past or future.
    """
    rpc_url = os.environ.get("JSON_RPC_ARBITRUM")
    if not rpc_url:
        pytest.skip("JSON_RPC_ARBITRUM not set")

    # Setup
    web3 = Web3(Web3.HTTPProvider(rpc_url))
    hypersync_endpoint = "https://arbitrum.hypersync.xyz"

    # Small block range for quick test (10,000 blocks)
    end_block = 180_000_000
    start_block = end_block - 10_000

    try:
        # Step 1: Collect events
        collector = GMXEventCollector(hypersync_endpoint, rpc_url)
        all_events = await collector.collect_position_events(start_block, end_block)

        assert isinstance(all_events, list)
        if all_events:  # Only validate structure if we have events
            # Verify events have required attributes
            sample_event = all_events[0]
            assert hasattr(sample_event, "execution_price")
            assert hasattr(sample_event, "market")
            assert hasattr(sample_event, "block_timestamp")
        print(f"Collected {len(all_events)} total events")

        # Step 2: Map markets to symbols
        mapper = GMXMarketMapper(web3)
        market_mapping = mapper.get_market_symbol_mapping()

        assert len(market_mapping) > 0

        # Step 3: Filter for ETH market
        eth_market = None
        for addr, symbol in market_mapping.items():
            if symbol == "ETH":
                eth_market = addr
                break

        if not eth_market:
            pytest.skip("ETH market not found")

        eth_events = [e for e in all_events if e.market.lower() == eth_market.lower()]

        print(f"Found {len(eth_events)} ETH events")

        if len(eth_events) == 0:
            pytest.skip("No ETH events in this range")

        # Step 4: Aggregate to OHLCV
        ohlcv = aggregate_events_to_ohlcv(eth_events, timeframe="1h", symbol="ETH")

        assert len(ohlcv) > 0
        assert "open" in ohlcv.columns
        assert "high" in ohlcv.columns
        assert "low" in ohlcv.columns
        assert "close" in ohlcv.columns
        assert "volume" in ohlcv.columns
        assert "symbol" in ohlcv.columns

        print(f"Generated {len(ohlcv)} 1h candles")

        # Step 5: Save to parquet
        storage = ParquetStorage(tmp_path)
        events_path = storage.save_position_events(eth_events, "ETH", partition_id=0)
        candles_path = storage.save_candles(ohlcv, "1h", "ETH")

        assert events_path.exists()
        assert candles_path.exists()

        print(f"Saved events to {events_path}")
        print(f"Saved candles to {candles_path}")
    except RuntimeError as e:
        if "403 Forbidden" in str(e):
            pytest.skip("HyperSync API returned 403 - authentication may be required")
        raise
