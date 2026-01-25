"""Tests for GMX event collector."""

import os
import pytest
from web3 import Web3
from gmx_historical_data.gmx_event_collector import (
    GMXEventCollector,
    get_position_event_hashes,
)
from gmx_historical_data.config import EVENT_EMITTER_ADDRESS


def test_get_position_event_hashes():
    """Test generating event name hashes for filtering."""
    hashes = get_position_event_hashes()

    assert len(hashes) == 2  # PositionIncrease, PositionDecrease
    assert all(isinstance(h, str) for h in hashes)
    assert all(len(h) == 66 for h in hashes)  # 0x prefix (2 chars) + 32 bytes (64 hex chars) = 66 chars total
    assert all(h.startswith("0x") for h in hashes)


@pytest.mark.asyncio
async def test_collect_events_small_range():
    """Test collecting events from a small block range.

    Note: This test may fail if HyperSync API requires authentication
    or is rate-limited. In production, use API tokens for reliable access.

    Note: Block number (180_000_000) may become stale - update if test fails
    due to block being too far in the past or future.
    """
    hypersync_endpoint = "https://arbitrum.hypersync.xyz"

    collector = GMXEventCollector(
        hypersync_endpoint=hypersync_endpoint,
        rpc_url=os.environ.get("JSON_RPC_ARBITRUM"),
    )

    # Collect from a small recent range (last 1000 blocks)
    # This should complete quickly
    end_block = 180_000_000  # Recent block on Arbitrum (as of Jan 2025 - may need updating)
    start_block = end_block - 1000

    try:
        events = await collector.collect_position_events(
            start_block=start_block,
            end_block=end_block,
        )

        # Should get some events (GMX is active)
        # But if range is too small, might be 0
        assert isinstance(events, list)
        assert all(hasattr(e, "execution_price") for e in events)
        assert all(hasattr(e, "market") for e in events)
    except RuntimeError as e:
        if "403 Forbidden" in str(e):
            pytest.skip("HyperSync API returned 403 - authentication may be required")
        raise


def test_event_collector_initialization():
    """Test event collector initialization."""
    rpc_url = os.environ.get("JSON_RPC_ARBITRUM")
    if not rpc_url:
        pytest.skip("JSON_RPC_ARBITRUM not set")

    collector = GMXEventCollector(
        hypersync_endpoint="https://arbitrum.hypersync.xyz",
        rpc_url=rpc_url,
    )

    assert collector.hypersync_endpoint == "https://arbitrum.hypersync.xyz"
    assert collector.web3 is not None
