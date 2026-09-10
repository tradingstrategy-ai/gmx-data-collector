"""Tests for GMX event collector."""

import logging
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

    def __init__(self, pages, height=None):
        self.pages = list(pages)
        self.requested = []
        self.height = height
        self.height_calls = 0

    async def get(self, query):
        self.requested.append((query.from_block, query.to_block))
        return self.pages.pop(0)

    async def get_height(self):
        self.height_calls += 1
        return self.height


class _FakeWeb3Eth:
    """Stands in for ``Web3().eth`` -- just enough for the one property
    ``collect_position_events`` touches before it starts scanning."""

    def __init__(self, chain_id_error: Exception | None = None):
        self._chain_id_error = chain_id_error
        self.chain_id_calls = 0

    @property
    def chain_id(self) -> int:
        self.chain_id_calls += 1
        if self._chain_id_error is not None:
            raise self._chain_id_error
        return 42161


class _FakeWeb3:
    def __init__(self, chain_id_error: Exception | None = None):
        self.eth = _FakeWeb3Eth(chain_id_error)


def _collector_with(client, web3: object | None = "default") -> GMXEventCollector:
    collector = GMXEventCollector(
        hypersync_endpoint="https://arbitrum.hypersync.xyz",
        rpc_url="http://localhost:0",
    )
    collector.client = client
    collector.web3 = _FakeWeb3() if web3 == "default" else web3
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
    async def test_resolves_none_end_block_to_the_current_height_once(self):
        """``end_block=None`` (the CLI's ``--end-block`` default) used to mean
        'chase next_block with no upper bound' -- at a live head, the archive
        keeps growing while the scan runs, so that loop is not guaranteed to
        ever see 'no forward progress' (confirmed live: 8 consecutive pages,
        cursor never caught up). Resolving the height once, up front, turns
        it into the same bounded, already-proven loop as an explicit
        end_block.
        """
        client = _PagingClient(
            [
                _FakeResponse([_FakeLog(100)], next_block=150),
                _FakeResponse([_FakeLog(150)], next_block=200),
                _FakeResponse([_FakeLog(199)], next_block=201),
            ],
            height=200,
        )
        collector = _collector_with(client)

        await collector.collect_position_events(start_block=100, end_block=None)

        assert client.height_calls == 1
        assert client.requested == [(100, 201), (150, 201), (200, 201)]

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


class TestCollectPositionEventsRpcHealthCheck:
    """``parse_position_event`` resolves the EventEmitter contract via a
    one-time ``web3.eth.chain_id`` RPC call; a per-log ``except Exception``
    would otherwise swallow an unreachable RPC exactly like a malformed log,
    making an infrastructure outage indistinguishable from "no events here".
    """

    @pytest.mark.asyncio
    async def test_raises_before_scanning_when_rpc_is_unreachable(self):
        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=150)])
        web3 = _FakeWeb3(chain_id_error=ConnectionError("no route to host"))
        collector = _collector_with(client, web3=web3)

        with pytest.raises(ConnectionError):
            await collector.collect_position_events(start_block=100, end_block=200)

        # Failed before making a single HyperSync request -- a caller
        # retrying this must not burn a page against a dead RPC.
        assert client.requested == []

    @pytest.mark.asyncio
    async def test_skips_the_check_when_web3_is_none(self):
        """Some callers construct the collector without a decoder web3
        (e.g. paging-only tests); the health check must not turn that into
        an AttributeError."""
        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=101)])
        collector = _collector_with(client, web3=None)

        events = await collector.collect_position_events(start_block=100, end_block=100)

        assert events == []

    @pytest.mark.asyncio
    async def test_does_not_check_rpc_health_for_an_empty_range(self):
        client = _PagingClient([])
        web3 = _FakeWeb3()
        collector = _collector_with(client, web3=web3)

        await collector.collect_position_events(start_block=200, end_block=100)

        assert web3.eth.chain_id_calls == 0


class TestCollectPositionEventsStallWarning:
    @pytest.mark.asyncio
    async def test_warns_when_the_scan_stalls_short_of_the_requested_end(self, caplog):
        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=100)])
        collector = _collector_with(client)

        with caplog.at_level(logging.WARNING):
            await collector.collect_position_events(start_block=100, end_block=200)

        assert any("stalled" in record.getMessage() for record in caplog.records)

    @pytest.mark.asyncio
    async def test_no_warning_when_the_full_range_is_covered(self, caplog):
        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=101)])
        collector = _collector_with(client)

        with caplog.at_level(logging.WARNING):
            await collector.collect_position_events(start_block=100, end_block=100)

        assert not any("stalled" in record.getMessage() for record in caplog.records)


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
    rpc_url = os.environ.get("JSON_RPC_ARBITRUM")
    if not rpc_url:
        # Decoding now probes RPC health eagerly (fails loudly rather than
        # silently swallowing every log) -- without a real endpoint this
        # would just fail against the Web3 default of localhost:8545,
        # which is a missing-fixture problem, not a real test failure.
        pytest.skip("JSON_RPC_ARBITRUM not set")

    hypersync_endpoint = "https://arbitrum.hypersync.xyz"

    collector = GMXEventCollector(
        hypersync_endpoint=hypersync_endpoint,
        rpc_url=rpc_url,
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
