"""Collect GMX v2 trade fills from HyperSync and turn them into ticks.

All GMX v2 events land on one ``EventEmitter`` contract as generic
``EventLog``/``EventLog1``/``EventLog2`` logs with ``keccak256(event_name)``
in ``topic1``, so a single address plus a topic1 filter selects exactly the
fills we want server-side -- ``PositionIncrease``, ``PositionDecrease`` and
``SwapInfo``.

Decoding gotcha worth keeping: HyperSync returns unselected topic slots as
``None``, and ``eth_defi``'s decoder raises on those and returns ``None`` for
the whole event. Topics must be compacted before decoding, which
:func:`log_to_dict` does.

Scanning is checkpointed by block number rather than by date so a run that
is interrupted, or a cron that misses a day, resumes exactly where it
stopped instead of re-deriving a block range from wall-clock time.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any

import requests
from eth_defi.gmx.events import decode_gmx_event
from eth_utils import keccak
from hypersync import BlockField, FieldSelection, LogField, LogSelection, Query
from web3 import Web3
from web3.providers.rpc import HTTPProvider

from gmx_historical_data.config import EVENT_EMITTER_ADDRESS
from gmx_historical_data.gmx_trade_ticks import (
    PERP_EVENTS,
    SWAP_EVENT,
    TokenMeta,
    TradeTick,
    ticks_from_event_data,
)
from gmx_historical_data.hypersync_range import exclusive_end

logger = logging.getLogger(__name__)

#: Oracle metadata endpoint carrying token decimals, per chain.
TOKENS_URL: dict[str, str] = {
    "arbitrum": "https://arbitrum-api.gmxinfra.io/tokens",
    "avalanche": "https://avalanche-api.gmxinfra.io/tokens",
}

#: Events this collector decodes into ticks.
TRADE_EVENTS: tuple[str, ...] = (*PERP_EVENTS, SWAP_EVENT)

#: Arbitrum produces roughly 4 blocks a second; one day is about 345k.
BLOCKS_PER_DAY = 345_600

#: Default ceiling on a single run's scan, so catching up after an outage
#: cannot blow the daily job's time budget in one go.
DEFAULT_MAX_BLOCKS = 500_000


def event_topic(name: str) -> str:
    """Return the ``topic1`` filter value for a GMX event name.

    :param name: Event name, e.g. ``SwapInfo``.
    :returns: ``0x``-prefixed keccak256 hash.
    """
    digest = keccak(text=name).hex()
    return digest if digest.startswith("0x") else f"0x{digest}"


def build_token_map(raw_tokens: Sequence[dict]) -> dict[str, TokenMeta]:
    """Index the oracle token list by address.

    :param raw_tokens: Entries from the ``/tokens`` endpoint, each with
        ``symbol``, ``address`` and ``decimals``.
    :returns: Address -> :class:`TokenMeta`; entries missing a symbol,
        address or decimals are dropped rather than defaulted.
    """
    tokens: dict[str, TokenMeta] = {}
    for entry in raw_tokens:
        address = entry.get("address")
        symbol = entry.get("symbol")
        decimals = entry.get("decimals")
        if not address or not symbol or decimals is None:
            continue
        tokens[address] = TokenMeta(symbol=symbol, decimals=int(decimals))
    return tokens


def fetch_token_map(chain: str = "arbitrum", timeout: int = 30) -> dict[str, TokenMeta]:
    """Fetch and index token metadata from the GMX oracle API.

    :param chain: Chain name.
    :param timeout: HTTP timeout in seconds.
    :returns: Address -> :class:`TokenMeta`.
    :raises requests.HTTPError: If the endpoint is unreachable.
    """
    response = requests.get(TOKENS_URL[chain], timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    raw = payload.get("tokens", payload) if isinstance(payload, dict) else payload
    tokens = build_token_map(raw)
    logger.info("Loaded decimals for %d tokens (chain=%s)", len(tokens), chain)
    return tokens


def build_market_map(
    markets: Sequence[dict],
    tokens: dict[str, TokenMeta],
) -> dict[str, TokenMeta]:
    """Map each perpetual market token to its index token's symbol and scale.

    ``sizeDeltaInTokens`` on a position event is denominated in the *index*
    token's decimals, so the market has to carry that scale, not the
    collateral's. Swap-only pools have no index token and no candle file, so
    they are excluded; several markets legitimately share one symbol (BTC has
    multiple collateral variants) and all of them count toward it.

    :param markets: Raw market dicts from ``get_markets_info()``.
    :param tokens: Address -> :class:`TokenMeta` from :func:`build_token_map`.
    :returns: Market token address -> index :class:`TokenMeta`.
    """
    resolved: dict[str, TokenMeta] = {}
    for market in markets:
        name = market.get("name", "")
        if "/" not in name:
            continue
        market_token = market.get("marketToken")
        index_token = market.get("indexToken")
        if not market_token or not index_token:
            continue

        meta = tokens.get(index_token)
        if meta is None:
            lowered = index_token.lower()
            meta = next((v for k, v in tokens.items() if k.lower() == lowered), None)
        if meta is None:
            logger.debug("Market %s has unmapped index token %s", name, index_token)
            continue

        symbol = name.split("/")[0].strip()
        if not symbol:
            continue
        resolved[market_token] = TokenMeta(symbol=symbol, decimals=meta.decimals)
    return resolved


def resolve_scan_range(
    tip: int,
    last_scanned: int | None,
    max_blocks: int = DEFAULT_MAX_BLOCKS,
) -> tuple[int, int]:
    """Decide which block range this run should scan.

    :param tip: Current chain head as reported by HyperSync.
    :param last_scanned: Highest block a previous run completed, or ``None``
        on a first run.
    :param max_blocks: Ceiling on the size of a single run's range.
    :returns: Inclusive ``(start, end)``. ``start > end`` means there is
        nothing new to scan.
    """
    start = tip - max_blocks + 1 if last_scanned is None else last_scanned + 1
    start = max(start, 0)
    end = min(tip, start + max_blocks - 1)
    return start, end


class CachedChainIdProvider(HTTPProvider):
    """HTTP provider that answers ``eth_chainId`` from memory after the first call.

    ``eth_defi``'s GMX decoder asks the node for the chain id once per event.
    Measured against Arbitrum that is the whole cost of decoding -- 651 logs
    took 68s, 651 of which were ``eth_chainId`` round-trips -- and the answer
    cannot change within a run. Caching it turns the backfill from
    RPC-latency-bound into CPU-bound.
    """

    def __init__(self, *args, **kwargs) -> None:
        """Initialise the provider with an empty chain-id cache."""
        super().__init__(*args, **kwargs)
        self._chain_id_response: dict | None = None

    def make_request(self, method: str, params: Any) -> Any:
        """Serve ``eth_chainId`` from cache; forward everything else.

        :param method: JSON-RPC method name.
        :param params: JSON-RPC parameters.
        :returns: JSON-RPC response.
        """
        if method == "eth_chainId":
            if self._chain_id_response is None:
                self._chain_id_response = super().make_request(method, params)
            return self._chain_id_response
        return super().make_request(method, params)


def build_decoder_web3(rpc_url: str | None) -> Web3:
    """Build the ``Web3`` instance used for ABI decoding.

    :param rpc_url: Arbitrum JSON-RPC endpoint.
    :returns: ``Web3`` wired to a :class:`CachedChainIdProvider`.
    """
    return Web3(CachedChainIdProvider(rpc_url))


def chunk_ranges(start: int, end: int, size: int) -> list[tuple[int, int]]:
    """Split an inclusive block range into contiguous chunks.

    The final chunk is short rather than overshooting ``end`` -- a backfill
    that ran past its target would pull fills belonging to a later day.

    :param start: First block, inclusive.
    :param end: Last block, inclusive.
    :param size: Maximum blocks per chunk.
    :returns: Contiguous ``(from, to)`` pairs; empty when ``start > end``.
    """
    if start > end:
        return []
    chunks = []
    cursor = start
    while cursor <= end:
        chunks.append((cursor, min(cursor + size - 1, end)))
        cursor += size
    return chunks


def build_trade_query(start_block: int, end_block: int) -> Query:
    """Build the HyperSync query selecting every trade fill in a range.

    HyperSync's ``to_block`` is exclusive, so the conversion is delegated to
    :func:`~gmx_historical_data.hypersync_range.exclusive_end` -- see there for
    why passing the inclusive end dropped the last block of every range.

    :param start_block: First block, inclusive.
    :param end_block: Last block to include, inclusive.
    :returns: HyperSync :class:`Query` covering both ends.
    """
    return Query(
        from_block=start_block,
        to_block=exclusive_end(end_block),
        logs=[
            LogSelection(
                address=[EVENT_EMITTER_ADDRESS.lower()],
                topics=[[], [event_topic(name) for name in TRADE_EVENTS]],
            )
        ],
        field_selection=FieldSelection(
            log=[
                LogField.BLOCK_NUMBER,
                LogField.BLOCK_HASH,
                LogField.TRANSACTION_HASH,
                LogField.TRANSACTION_INDEX,
                LogField.LOG_INDEX,
                LogField.ADDRESS,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
            ],
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        ),
    )


def log_to_dict(log: Any) -> dict:
    """Convert a HyperSync log into the shape ``eth_defi`` expects.

    Unselected topic slots come back as ``None``; leaving them in makes the
    decoder raise and return ``None`` for the whole event, so they are
    compacted out here.

    :param log: HyperSync log object.
    :returns: Ethereum-style log dict with camelCase keys.
    """
    return {
        "blockNumber": log.block_number,
        "blockHash": log.block_hash or "",
        "transactionHash": log.transaction_hash,
        "transactionIndex": log.transaction_index or 0,
        "logIndex": log.log_index,
        "address": log.address,
        "topics": [topic for topic in (log.topics or []) if topic],
        "data": log.data,
    }


def _block_timestamps(blocks: Sequence[Any]) -> dict[int, int]:
    """Index block timestamps by block number, tolerating hex encoding.

    :param blocks: HyperSync block objects.
    :returns: Block number -> Unix seconds.
    """
    timestamps: dict[int, int] = {}
    for block in blocks:
        value = block.timestamp
        if value is None:
            continue
        timestamps[block.number] = int(value, 16) if isinstance(value, str) else int(value)
    return timestamps


def decode_ticks(
    logs: Sequence[Any],
    blocks: Sequence[Any],
    web3: Any,
    markets: dict[str, TokenMeta],
    tokens: dict[str, TokenMeta],
) -> list[TradeTick]:
    """Decode a batch of HyperSync logs into ticks.

    A log that fails to decode, or whose block timestamp is missing, is
    skipped with a debug line rather than aborting the batch -- one bad log
    must not cost a whole day's volume.

    :param logs: HyperSync log objects.
    :param blocks: HyperSync block objects covering ``logs``.
    :param web3: ``Web3`` instance used for ABI decoding.
    :param markets: Market token address -> index :class:`TokenMeta`.
    :param tokens: Token address -> :class:`TokenMeta`.
    :returns: Decoded ticks, unsorted.
    """
    timestamps = _block_timestamps(blocks)
    ticks: list[TradeTick] = []

    for log in logs:
        timestamp = timestamps.get(log.block_number)
        if timestamp is None:
            logger.debug("No timestamp for block %s; skipping log", log.block_number)
            continue
        try:
            event = decode_gmx_event(web3, log_to_dict(log))
        except Exception as exc:  # noqa: BLE001 - one bad log must not stop the batch
            logger.debug("Failed to decode log in block %s: %s", log.block_number, exc)
            continue
        if event is None:
            continue

        ticks.extend(
            ticks_from_event_data(
                event,
                timestamp=timestamp,
                block_number=log.block_number,
                tx_hash=log.transaction_hash,
                log_index=log.log_index,
                markets=markets,
                tokens=tokens,
            )
        )
    return ticks


async def collect_ticks_with_retry(
    pool: Any,
    start_block: int,
    end_block: int,
    web3: Any,
    markets: dict[str, TokenMeta],
    tokens: dict[str, TokenMeta],
    max_retries: int = 3,
    base_delay: float = 2.0,
) -> tuple[list[TradeTick], int]:
    """Collect ticks, rotating the API key on rate limits before backing off.

    HyperSync answers a hammered key with ``429``. The repo's convention --
    see ``scripts/extract_*.py`` -- is to rotate to the next key first, which
    does not count against the retry budget, and only then to back off
    exponentially. Without this a single ``429`` costs a whole day's volume.

    :param pool: :class:`~gmx_historical_data.hypersync_client_factory.RotatingHypersyncClient`.
    :param start_block: First block, inclusive.
    :param end_block: Last block, inclusive.
    :param web3: ``Web3`` instance used for ABI decoding.
    :param markets: Market token address -> index :class:`TokenMeta`.
    :param tokens: Token address -> :class:`TokenMeta`.
    :param max_retries: Retries after rotation is exhausted.
    :param base_delay: Seconds for the first backoff, doubled each retry.
    :returns: Tuple of (decoded ticks, highest block confirmed fully
        scanned) -- see :func:`collect_ticks`.
    :raises Exception: The last error, once the retry budget is spent.
    """
    attempt = 0
    while True:
        try:
            return await collect_ticks(pool.client, start_block, end_block, web3, markets, tokens)
        except Exception as e:  # noqa: BLE001 - re-raised once the budget is spent
            rotate = getattr(pool, "rotate_on_error", None)
            if callable(rotate) and rotate(e):
                logger.warning("Rate limited; rotated HyperSync key and retrying")
                continue

            attempt += 1
            if attempt > max_retries:
                raise

            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "HyperSync error (attempt %d/%d): %s — retrying in %.0fs",
                attempt,
                max_retries,
                e,
                delay,
            )
            if delay:
                await asyncio.sleep(delay)


async def collect_ticks(
    client: Any,
    start_block: int,
    end_block: int,
    web3: Any,
    markets: dict[str, TokenMeta],
    tokens: dict[str, TokenMeta],
) -> tuple[list[TradeTick], int]:
    """Fetch and decode every trade fill in a block range.

    HyperSync caps a response by payload size, not by the range requested: a
    300k-block query came back covering only ~104k blocks, with ``next_block``
    marking where to resume. A single ``get()`` therefore silently returns a
    fraction of the range, so this pages until the range is covered or the
    server stops making progress.

    Decoding resolves the EventEmitter contract via a one-time
    ``web3.eth.chain_id`` RPC call (cached after success). If that RPC is
    unreachable, every log would otherwise fail to decode and get swallowed
    by :func:`decode_ticks`'s per-log guard, making an infrastructure outage
    indistinguishable from "no fills in this range" -- and the caller would
    then checkpoint past blocks that were never actually decoded. This
    probes that RPC once, loudly, before scanning.

    :param client: HyperSync client (or rotator's active client).
    :param start_block: First block, inclusive.
    :param end_block: Last block, inclusive.
    :param web3: ``Web3`` instance used for ABI decoding.
    :param markets: Market token address -> index :class:`TokenMeta`.
    :param tokens: Token address -> :class:`TokenMeta`.
    :returns: Tuple of (decoded ticks, highest block confirmed fully
        scanned). The second value is less than ``end_block`` when the
        server stopped making forward progress before the requested end was
        reached -- callers must checkpoint against it, not ``end_block``, or
        a stalled scan is silently treated as a completed one.
    :raises Exception: If ``web3`` cannot reach the RPC endpoint.
    """
    if start_block > end_block:
        return [], start_block - 1

    if web3 is not None:
        web3.eth.chain_id

    ticks: list[TradeTick] = []
    total_logs = 0
    cursor = start_block

    while cursor <= end_block:
        response = await client.get(build_trade_query(cursor, end_block))
        logs = response.data.logs
        total_logs += len(logs)
        ticks.extend(decode_ticks(logs, response.data.blocks, web3, markets, tokens))

        next_block = getattr(response, "next_block", None)
        if not next_block or next_block <= cursor:
            # No forward progress: either the range is done or the server
            # cannot advance. Either way, looping again would never finish.
            break
        cursor = next_block

    reached_block = cursor - 1
    if reached_block < end_block:
        logger.warning(
            "Tick scan stalled at block %d, short of requested end %d (%d blocks unscanned)",
            reached_block,
            end_block,
            end_block - reached_block,
        )

    logger.info(
        "Decoded %d ticks from %d logs in blocks %d-%d",
        len(ticks),
        total_logs,
        start_block,
        reached_block,
    )
    return ticks, reached_block
