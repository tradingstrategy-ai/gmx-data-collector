"""Tests for GMX event collector."""

import os

import pytest

from gmx_historical_data.gmx_event_collector import (
    GMXEventCollector,
    get_position_event_hashes,
)


class _FakeLog:
    def __init__(self, block_number):
        self.block_number = block_number
        self.block_hash = "0x0"
        self.transaction_hash = f"0x{block_number:x}"
        self.transaction_index = 0
        self.log_index = 0
        self.address = "0xee"
        self.topics = ["0xsig", "0xname", None, None]
        self.data = "0x"


class _FakeBlock:
    def __init__(self, number):
        self.number = number
        self.timestamp = 1788000000 + number


class _FakeResponse:
    def __init__(self, logs, next_block):
        self.data = type(
            "D", (), {"logs": logs, "blocks": [_FakeBlock(log.block_number) for log in logs]}
        )
        self.next_block = next_block


class _PagingClient:
    """HyperSync caps a response by size, not by the range you asked for."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.requested = []

    async def get(self, query):
        self.requested.append((query.from_block, query.to_block))
        return self.pages.pop(0)


def _collector_with(client) -> GMXEventCollector:
    collector = GMXEventCollector(
        hypersync_endpoint="https://arbitrum.hypersync.xyz",
        rpc_url="http://localhost:0",
    )
    collector.client = client
    return collector


class TestCollectPositionEventsPaging:
    """A single ``get()`` truncates a large range by payload size, not by
    the range requested -- the same class of bug fixed in
    ``trade_tick_collector.collect_ticks``. Silently ignoring ``next_block``
    would drop the rest of the range.
    """

    @pytest.mark.asyncio
    async def test_follows_next_block_until_the_range_is_covered(self):
        client = _PagingClient(
            [
                _FakeResponse([_FakeLog(100)], next_block=150),
                _FakeResponse([_FakeLog(150)], next_block=200),
                _FakeResponse([_FakeLog(199)], next_block=201),
            ]
        )
        collector = _collector_with(client)

        await collector.collect_position_events(start_block=100, end_block=200)

        assert client.requested == [(100, 201), (150, 201), (200, 201)]

    @pytest.mark.asyncio
    async def test_pages_an_unbounded_range_until_progress_stops(self):
        """``end_block=None`` (the CLI's ``--end-block`` default) means
        'to the tip', not 'one page'. The loop must keep following
        ``next_block`` with no upper bound on the cursor.
        """
        client = _PagingClient(
            [
                _FakeResponse([_FakeLog(100)], next_block=150),
                _FakeResponse([_FakeLog(150)], next_block=200),
                _FakeResponse([_FakeLog(200)], next_block=200),
            ]
        )
        collector = _collector_with(client)

        await collector.collect_position_events(start_block=100, end_block=None)

        assert client.requested == [(100, None), (150, None), (200, None)]

    @pytest.mark.asyncio
    async def test_stops_when_the_cursor_stops_advancing(self):
        """A ``next_block`` that does not move would loop forever."""
        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=100)])
        collector = _collector_with(client)

        await collector.collect_position_events(start_block=100, end_block=200)

        assert len(client.requested) == 1

    @pytest.mark.asyncio
    async def test_stops_when_next_block_is_absent(self):
        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=None)])
        collector = _collector_with(client)

        await collector.collect_position_events(start_block=100, end_block=200)

        assert len(client.requested) == 1

    @pytest.mark.asyncio
    async def test_empty_range_makes_no_request(self):
        client = _PagingClient([])
        collector = _collector_with(client)

        events = await collector.collect_position_events(start_block=200, end_block=100)

        assert events == []
        assert client.requested == []

    @pytest.mark.asyncio
    async def test_aggregates_block_timestamps_across_pages(self):
        """Block timestamps from an earlier page must still be available
        when parsing logs returned by a later page."""
        client = _PagingClient(
            [
                _FakeResponse([_FakeLog(100)], next_block=150),
                _FakeResponse([_FakeLog(150)], next_block=151),
            ]
        )
        collector = _collector_with(client)

        # Neither fake log decodes to a real position event, but the
        # collector must not raise while trying both pages' logs against
        # the combined timestamp map.
        events = await collector.collect_position_events(start_block=100, end_block=150)

        assert events == []
        assert len(client.requested) == 2


def test_get_position_event_hashes():
    """Test generating event name hashes for filtering."""
    hashes = get_position_event_hashes()

    assert len(hashes) == 2  # PositionIncrease, PositionDecrease
    assert all(isinstance(h, str) for h in hashes)
    assert all(
        len(h) == 66 for h in hashes
    )  # 0x prefix (2 chars) + 32 bytes (64 hex chars) = 66 chars total
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
        if "403 Forbidden" in str(e) or "401 Unauthorized" in str(e):
            pytest.skip("HyperSync API requires authentication — set HYPERSYNC_API_TOKEN")
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
