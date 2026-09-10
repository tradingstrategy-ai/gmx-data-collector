"""Tests for building the address->metadata maps the tick decoder needs.

Every scaling decision downstream keys off these maps. A market that
resolves to the wrong decimals does not fail loudly -- it publishes volume
off by a power of ten -- so unresolvable entries must be dropped here.
"""

import pytest

from gmx_historical_data.gmx_trade_ticks import TokenMeta
from gmx_historical_data.trade_tick_collector import (
    build_market_map,
    build_token_map,
    chunk_ranges,
    resolve_scan_range,
)

RAW_TOKENS = [
    {"symbol": "BTC", "address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "decimals": 8},
    {"symbol": "ETH", "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "decimals": 18},
    {
        "symbol": "APT",
        "address": "0x3f8f0dCE4dCE4d0D1d0871941e79CDA82cA50d0B",
        "decimals": 8,
        "synthetic": True,
    },
    {"symbol": "USDC", "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "decimals": 6},
]

BTC_TOKEN = RAW_TOKENS[0]["address"]
ETH_TOKEN = RAW_TOKENS[1]["address"]


class TestBuildTokenMap:
    def test_maps_address_to_symbol_and_decimals(self):
        tokens = build_token_map(RAW_TOKENS)

        assert tokens[BTC_TOKEN] == TokenMeta(symbol="BTC", decimals=8)
        assert tokens[ETH_TOKEN].decimals == 18

    def test_synthetic_tokens_are_included(self):
        """Synthetic markets (APT, DOGE) have no ERC-20 but still trade."""
        tokens = build_token_map(RAW_TOKENS)

        assert tokens["0x3f8f0dCE4dCE4d0D1d0871941e79CDA82cA50d0B"].symbol == "APT"

    def test_entries_without_decimals_are_dropped(self):
        tokens = build_token_map([{"symbol": "BAD", "address": "0xdead"}])

        assert tokens == {}


class TestBuildMarketMap:
    def _market(self, name, index_token, market_token="0xMARKET"):
        return {"name": name, "indexToken": index_token, "marketToken": market_token}

    def test_market_token_resolves_to_index_symbol_and_decimals(self):
        tokens = build_token_map(RAW_TOKENS)
        markets = build_market_map(
            [self._market("BTC/USD [WBTC.b-USDC]", BTC_TOKEN, "0xAAA")], tokens
        )

        assert markets["0xAAA"] == TokenMeta(symbol="BTC", decimals=8)

    def test_swap_only_pools_are_excluded(self):
        """`SWAP-ONLY [USDC-USDT]` has no index token and no candle file."""
        tokens = build_token_map(RAW_TOKENS)
        markets = build_market_map([self._market("SWAP-ONLY [USDC-USDT]", "", "0xBBB")], tokens)

        assert markets == {}

    def test_market_with_unknown_index_token_is_dropped(self):
        tokens = build_token_map(RAW_TOKENS)
        markets = build_market_map(
            [self._market("NEW/USD [NEW-USDC]", "0xUNLISTED", "0xCCC")], tokens
        )

        assert markets == {}

    def test_multiple_markets_share_one_symbol(self):
        """BTC has several collateral variants; all count toward BTC volume."""
        tokens = build_token_map(RAW_TOKENS)
        markets = build_market_map(
            [
                self._market("BTC/USD [WBTC.b-USDC]", BTC_TOKEN, "0xAAA"),
                self._market("BTC/USD [BTC-USDC]", BTC_TOKEN, "0xDDD"),
            ],
            tokens,
        )

        assert {m.symbol for m in markets.values()} == {"BTC"}
        assert len(markets) == 2


class TestResolveScanRange:
    def test_uses_checkpoint_when_present(self):
        start, end = resolve_scan_range(tip=1_000_000, last_scanned=999_000, max_blocks=500_000)

        assert (start, end) == (999_001, 1_000_000)

    def test_falls_back_to_a_bounded_window_on_first_run(self):
        start, end = resolve_scan_range(tip=1_000_000, last_scanned=None, max_blocks=350_000)

        assert (start, end) == (650_001, 1_000_000)
        assert end - start + 1 == 350_000

    def test_long_gap_is_capped_so_one_run_cannot_scan_forever(self):
        """After an outage the checkpoint may be weeks behind. The daily job
        has a fixed time budget, so it catches up in bounded chunks."""
        start, end = resolve_scan_range(tip=10_000_000, last_scanned=1_000, max_blocks=500_000)

        assert start == 1_001
        assert end == 501_000
        assert end - start < 500_001

    def test_already_current_yields_an_empty_range(self):
        start, end = resolve_scan_range(tip=500, last_scanned=500, max_blocks=1000)

        assert start > end


class TestChunkRanges:
    def test_splits_into_inclusive_chunks(self):
        assert chunk_ranges(0, 9, 5) == [(0, 4), (5, 9)]

    def test_final_chunk_is_short_not_overshooting(self):
        """Overshooting the end would query blocks past the target and, on a
        backfill, pull fills that belong to a later day's file."""
        chunks = chunk_ranges(0, 11, 5)

        assert chunks == [(0, 4), (5, 9), (10, 11)]
        assert chunks[-1][1] == 11

    def test_single_chunk_when_range_fits(self):
        assert chunk_ranges(100, 200, 1000) == [(100, 200)]

    def test_empty_when_start_exceeds_end(self):
        assert chunk_ranges(10, 5, 100) == []

    def test_chunks_are_contiguous_and_cover_everything(self):
        chunks = chunk_ranges(1000, 5000, 700)

        assert chunks[0][0] == 1000
        assert chunks[-1][1] == 5000
        for earlier, later in zip(chunks, chunks[1:]):
            assert later[0] == earlier[1] + 1


class TestCachedChainIdProvider:
    def test_chain_id_is_fetched_once_not_per_event(self, monkeypatch):
        """eth_defi's decoder asks for the chain id on every single event.
        Over a genesis backfill that is one RPC round-trip per fill and
        dominates the runtime, so the answer is memoised."""
        from gmx_historical_data.trade_tick_collector import CachedChainIdProvider

        calls = []

        def fake_make_request(self, method, params):
            calls.append(method)
            return {"jsonrpc": "2.0", "id": 1, "result": "0xa4b1"}

        monkeypatch.setattr(
            "web3.providers.rpc.HTTPProvider.make_request", fake_make_request, raising=False
        )

        provider = CachedChainIdProvider("https://example.invalid")
        first = provider.make_request("eth_chainId", [])
        second = provider.make_request("eth_chainId", [])

        assert first == second
        assert calls == ["eth_chainId"]

    def test_other_methods_are_not_cached(self, monkeypatch):
        from gmx_historical_data.trade_tick_collector import CachedChainIdProvider

        calls = []

        def fake_make_request(self, method, params):
            calls.append(method)
            return {"jsonrpc": "2.0", "id": 1, "result": "0x1"}

        monkeypatch.setattr(
            "web3.providers.rpc.HTTPProvider.make_request", fake_make_request, raising=False
        )

        provider = CachedChainIdProvider("https://example.invalid")
        provider.make_request("eth_blockNumber", [])
        provider.make_request("eth_blockNumber", [])

        assert calls == ["eth_blockNumber", "eth_blockNumber"]


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
            "D", (), {"logs": logs, "blocks": [_FakeBlock(x.block_number) for x in logs]}
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


class TestCollectTicksPaging:
    async def _run(self, client, start, end):
        from gmx_historical_data.trade_tick_collector import collect_ticks

        return await collect_ticks(client, start, end, web3=None, markets={}, tokens={})

    def test_follows_next_block_until_the_range_is_covered(self):
        """A single get() returned only ~35% of a 300k-block range in
        production. Ignoring next_block silently drops the rest, and the
        backfill checkpoint then advances past blocks never scanned.

        Every page ends one past the requested block because HyperSync's
        ``to_block`` is exclusive; the old expectation of ``(200, 200)`` for
        the last page pinned the off-by-one that dropped block 200.
        """
        import asyncio

        client = _PagingClient(
            [
                _FakeResponse([_FakeLog(100)], next_block=150),
                _FakeResponse([_FakeLog(150)], next_block=200),
                _FakeResponse([_FakeLog(199)], next_block=201),
            ]
        )

        _, reached_block = asyncio.run(self._run(client, 100, 200))

        assert client.requested == [(100, 201), (150, 201), (200, 201)]
        assert reached_block == 200

    def test_stops_when_the_cursor_stops_advancing(self):
        """A next_block that does not move would loop forever.

        The caller must checkpoint against what was actually reached, not
        the requested end -- a stall short of it is a real, unfinished scan,
        not a completed one.
        """
        import asyncio

        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=100)])

        _, reached_block = asyncio.run(self._run(client, 100, 200))

        assert len(client.requested) == 1
        assert reached_block == 99

    def test_stops_when_next_block_is_absent(self):
        import asyncio

        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=None)])

        _, reached_block = asyncio.run(self._run(client, 100, 200))

        assert len(client.requested) == 1
        assert reached_block == 99

    def test_empty_range_makes_no_request(self):
        import asyncio

        client = _PagingClient([])

        assert asyncio.run(self._run(client, 200, 100)) == ([], 199)
        assert client.requested == []

    def test_reached_block_equals_end_when_the_full_range_is_covered(self):
        import asyncio

        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=101)])

        _, reached_block = asyncio.run(self._run(client, 100, 100))

        assert reached_block == 100


class _FakeWeb3Eth:
    """Stands in for ``Web3().eth`` -- just enough for the one property
    ``collect_ticks`` touches before it starts scanning."""

    def __init__(self, chain_id_error: Exception | None = None):
        self._chain_id_error = chain_id_error

    @property
    def chain_id(self) -> int:
        if self._chain_id_error is not None:
            raise self._chain_id_error
        return 42161


class _FakeWeb3:
    def __init__(self, chain_id_error: Exception | None = None):
        self.eth = _FakeWeb3Eth(chain_id_error)


class TestCollectTicksRpcHealthCheck:
    """``decode_gmx_event`` resolves the EventEmitter contract via a
    one-time ``web3.eth.chain_id`` RPC call; ``decode_ticks``'s per-log
    ``except Exception`` would otherwise swallow an unreachable RPC exactly
    like a malformed log, so a daily run would checkpoint an empty result
    as if it were a genuinely quiet window.
    """

    def test_raises_before_scanning_when_rpc_is_unreachable(self):
        import asyncio

        from gmx_historical_data.trade_tick_collector import collect_ticks

        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=150)])
        web3 = _FakeWeb3(chain_id_error=ConnectionError("no route to host"))

        with pytest.raises(ConnectionError):
            asyncio.run(collect_ticks(client, 100, 200, web3, markets={}, tokens={}))

        # Failed before making a single HyperSync request -- a retry must
        # not burn a page against a dead RPC.
        assert client.requested == []

    def test_skips_the_check_when_web3_is_none(self):
        import asyncio

        from gmx_historical_data.trade_tick_collector import collect_ticks

        client = _PagingClient([_FakeResponse([_FakeLog(100)], next_block=101)])

        ticks, reached_block = asyncio.run(
            collect_ticks(client, 100, 100, None, markets={}, tokens={})
        )

        assert reached_block == 100

    def test_does_not_check_rpc_health_for_an_empty_range(self):
        import asyncio

        from gmx_historical_data.trade_tick_collector import collect_ticks

        client = _PagingClient([])
        web3 = _FakeWeb3(chain_id_error=ConnectionError("no route to host"))

        # Would raise if the health check ran for an empty range.
        ticks, reached_block = asyncio.run(
            collect_ticks(client, 200, 100, web3, markets={}, tokens={})
        )

        assert ticks == []


class _FlakyPool:
    """Stands in for RotatingHypersyncClient: a pool with a rotating key."""

    def __init__(self, failures, rotates=True):
        self.failures = failures
        self.rotates = rotates
        self.attempts = 0
        self.rotations = 0
        self._page = _FakeResponse([_FakeLog(10)], next_block=None)

    @property
    def client(self):
        pool = self

        class _C:
            async def get(self, query):
                pool.attempts += 1
                if pool.attempts <= pool.failures:
                    raise RuntimeError("http response status code 429 Too Many Requests")
                return pool._page

        return _C()

    def rotate_on_error(self, exc):
        if not self.rotates:
            return False
        self.rotations += 1
        return True


class TestCollectTicksWithRetry:
    def _run(self, pool, **kw):
        import asyncio

        from gmx_historical_data.trade_tick_collector import collect_ticks_with_retry

        return asyncio.run(
            collect_ticks_with_retry(pool, 1, 100, web3=None, markets={}, tokens={}, **kw)
        )

    def test_rate_limit_rotates_the_key_and_retries(self):
        """A 429 on a shared key must not cost a day's volume when other
        keys in the pool are still good."""
        pool = _FlakyPool(failures=1)

        self._run(pool)

        assert pool.rotations == 1
        assert pool.attempts == 2

    def test_gives_up_after_the_retry_budget(self):
        import pytest

        pool = _FlakyPool(failures=99, rotates=False)

        with pytest.raises(RuntimeError):
            self._run(pool, max_retries=2, base_delay=0)

        assert pool.attempts == 3

    def test_succeeds_without_retrying_when_healthy(self):
        pool = _FlakyPool(failures=0)

        self._run(pool)

        assert pool.attempts == 1
        assert pool.rotations == 0
