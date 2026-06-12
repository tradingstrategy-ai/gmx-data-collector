"""Collect GMX oracle price update events using HyperSync.

Queries GMX EventEmitter contract for OraclePriceUpdate events,
which contain oracle prices for tokens without public Chainlink feeds.

Uses HyperSync for fast event collection with eth_defi for decoding.
Includes retry mechanism with exponential backoff for reliability.

Architecture Notes:
    - HyperSync for fast event fetching (parallel chunks)
    - eth_defi for complex GMX event decoding (nested EventData structures)
    - Mock provider to avoid RPC calls for chain_id

Key Design Decisions:
    1. Keep eth_defi for GMX decoding: HyperSync's native decoder cannot handle
       GMX's complex nested EventData structures. eth_defi is proven correct.
    2. Mock provider for chain_id: Avoids RPC calls while keeping eth_defi working.
    3. Parallel chunk processing: HyperSync queries split into chunks for faster
       collection of large block ranges.
"""

import asyncio
import gc
import heapq
import logging
import random
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Optional

from eth_defi.gmx.events import decode_gmx_event
from eth_utils import keccak
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    HypersyncClient,
    LogField,
    LogSelection,
    Query,
    StreamConfig,
)
from rich.console import Console
from web3 import Web3
from web3.providers.base import BaseProvider
from web3.types import RPCEndpoint, RPCResponse

from gmx_historical_data.config import EVENT_EMITTER_ADDRESS

# Forward reference for type hints
if False:  # TYPE_CHECKING
    from gmx_historical_data.hypersync_key_rotator import HyperSyncKeyRotator


logger = logging.getLogger(__name__)
console = Console()


# EventLog1 signature hash from GMX EventEmitter contract
# EventLog1(address,string,string,bytes32,EventData)
# This is topic0 for most GMX events including OraclePriceUpdate
EVENTLOG1_SIGNATURE = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"


class ArbitrumMockProvider(BaseProvider):
    """Mock Web3 provider that returns Arbitrum chain_id without RPC calls.

    Used for ABI decoding with eth_defi which requires chain_id but doesn't
    actually need network connectivity for decoding operations.
    """

    ARBITRUM_CHAIN_ID = 42161

    def make_request(self, method: RPCEndpoint, params: list) -> RPCResponse:
        """Handle RPC requests by returning mock data.

        :param method: RPC method name
        :param params: RPC parameters
        :return: Mock response
        :raises ValueError: For unsupported methods
        """
        if method == "eth_chainId":
            return {"jsonrpc": "2.0", "id": 1, "result": hex(self.ARBITRUM_CHAIN_ID)}
        # For any other method, raise an error - we shouldn't need RPC
        raise ValueError(f"ArbitrumMockProvider only supports eth_chainId, got: {method}")

    def isConnected(self) -> bool:
        """Mock connection check."""
        return True


# Memory management: flush events to callback every N events to bound memory usage.
# Auto-tuned from system resources; falls back to 500K (empirically safe — OOM at ~3.5M).
try:
    from gmx_historical_data.resource_limiter import get_resource_limits as _get_limits

    FLUSH_EVERY = _get_limits()["flush_every"]
except Exception:
    FLUSH_EVERY = 500_000

# Retry configuration defaults
DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_DELAY = 1.0  # seconds
DEFAULT_MAX_DELAY = 30.0  # seconds


async def retry_with_backoff(
    coro_func,
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    operation_name: str = "operation",
    key_rotator: Optional["HyperSyncKeyRotator"] = None,
    on_key_rotated: Callable | None = None,
):
    """Execute async function with progressive backoff retry and key rotation.

    Progressive delays: 2s, 5s, 10s, 30s, 60s (with 10% jitter).
    Detects rate limit errors and rotates API keys without counting as retry.

    :param coro_func: Async function to call (will be awaited)
    :param max_retries: Maximum number of retry attempts
    :param base_delay: Initial delay between retries (seconds, legacy parameter)
    :param max_delay: Maximum delay between retries (seconds, legacy parameter)
    :param operation_name: Name for logging
    :param key_rotator: Optional HyperSyncKeyRotator for API key rotation
    :param on_key_rotated: Optional zero-arg callback invoked after each key rotation
        (used to advance the collector's active client index)
    :return: Result from successful call
    :raises: Last exception if all retries fail or all keys exhausted
    """
    # Progressive delays in seconds: 2, 5, 10, 30, 60
    progressive_delays = [2.0, 5.0, 10.0, 30.0, 60.0]
    last_exception = None
    attempt = 0

    while attempt <= max_retries:
        try:
            return await coro_func()
        except Exception as e:
            last_exception = e

            # Log full traceback
            tb_str = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            console.print(f"[red]Error in {operation_name}:[/red]\n{tb_str}")
            logger.warning(
                f"{operation_name} failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
            )

            # Check if this is a rate limit or auth error that warrants key rotation
            error_msg_lower = str(e).lower()
            is_rate_limit = any(
                keyword in error_msg_lower
                for keyword in ["rate limit", "too many requests", "429", "quota"]
            )
            is_auth_error = any(
                keyword in error_msg_lower
                for keyword in ["401", "unauthorized", "authentication failed", "invalid token"]
            )

            if (is_rate_limit or is_auth_error) and key_rotator is not None:
                label = "Rate limit" if is_rate_limit else "Auth error (401)"
                logger.warning(f"{label} detected, rotating key: {e}")
                try:
                    key_rotator.rotate()
                    logger.info(f"Rotated to API key: {key_rotator.current_key[:8]}...")
                    if on_key_rotated is not None:
                        on_key_rotated()
                    # Don't count as retry attempt, retry immediately
                    continue
                except RuntimeError as rotate_error:
                    logger.error(f"All API keys exhausted: {rotate_error}")
                    raise rotate_error

            if is_auth_error and key_rotator is None:
                # 401 with no rotator — retrying the same key won't help
                logger.error(f"Auth error with no key rotator, failing fast: {e}")
                raise

            # Not a rate limit error, or no key rotator - use normal retry logic
            if attempt < max_retries:
                # Use progressive delays
                delay_index = min(attempt, len(progressive_delays) - 1)
                delay = progressive_delays[delay_index]

                # Add 10% jitter
                jitter = random.uniform(0, delay * 0.1)
                delay += jitter

                logger.warning(
                    f"{operation_name} will retry in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{max_retries + 1})..."
                )
                await asyncio.sleep(delay)
                attempt += 1
            else:
                logger.error(f"{operation_name} failed after {max_retries + 1} attempts: {e}")
                attempt += 1

    raise last_exception


def get_oracle_price_update_hash() -> str:
    """Get event name hash for OraclePriceUpdate.

    :return: Keccak256 hash (hex string with 0x prefix for HyperSync)
    """
    return "0x" + keccak(text="OraclePriceUpdate").hex()


@dataclass
class OraclePriceEvent:
    """Parsed GMX oracle price update event.

    :param block_number: Block number
    :param block_timestamp: Block timestamp (Unix seconds)
    :param transaction_hash: Transaction hash
    :param log_index: Log index within transaction
    :param token: Token contract address
    :param provider: Price provider address
    :param min_price: Minimum price (30 decimals)
    :param max_price: Maximum price (30 decimals)
    :param timestamp: Event timestamp from oracle data
    """

    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int
    token: str
    provider: str
    min_price: int
    max_price: int
    timestamp: int


@dataclass
class CollectionStats:
    """Statistics for oracle event collection.

    :param total_events: Total events successfully parsed
    :param failed_events: Number of events that failed to parse
    :param blocks_scanned: Total blocks scanned
    :param chunks_processed: Number of chunks processed
    :param first_failures: List of first N decode failures for debugging
    """

    total_events: int = 0
    failed_events: int = 0
    blocks_scanned: int = 0
    chunks_processed: int = 0
    first_failures: list[str] = field(default_factory=list)
    max_failures_to_log: int = 5

    def record_failure(self, error_msg: str) -> None:
        """Record a decode failure.

        :param error_msg: Error message from decode failure
        """
        self.failed_events += 1
        if len(self.first_failures) < self.max_failures_to_log:
            self.first_failures.append(error_msg)

    def summary(self) -> str:
        """Generate summary string of collection stats.

        :return: Human-readable summary
        """
        success_rate = (
            (self.total_events / (self.total_events + self.failed_events) * 100)
            if (self.total_events + self.failed_events) > 0
            else 100.0
        )
        return (
            f"Parsed {self.total_events:,} events "
            f"({self.failed_events:,} failed, {success_rate:.1f}% success rate) "
            f"across {self.blocks_scanned:,} blocks in {self.chunks_processed} chunks"
        )


class OraclePriceCollector:
    """Collect GMX oracle price events via HyperSync.

    Uses HyperSync for fast event collection with cached eth_defi decoder.

    :param hypersync_endpoint: HyperSync API endpoint
    :param api_token: Optional HyperSync API token for authentication
    """

    def __init__(
        self,
        hypersync_endpoint: str,
        api_token: str | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_base_delay: float = DEFAULT_BASE_DELAY,
        retry_max_delay: float = DEFAULT_MAX_DELAY,
        key_rotator: Optional["HyperSyncKeyRotator"] = None,
    ):
        """Initialize oracle price collector.

        :param hypersync_endpoint: HyperSync API endpoint URL
        :param api_token: Optional HyperSync API token for authentication
        :param max_retries: Maximum retry attempts for failed queries
        :param retry_base_delay: Initial retry delay in seconds
        :param retry_max_delay: Maximum retry delay in seconds
        :param key_rotator: Optional HyperSyncKeyRotator for API key rotation on rate limits
        """
        self.hypersync_endpoint = hypersync_endpoint
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.key_rotator = key_rotator

        # Build client pool — one client per key so rotation actually switches endpoints.
        # With no rotator we wrap the single token in a 1-element list for uniform access.
        if key_rotator is not None:
            self.clients = key_rotator.get_clients(hypersync_endpoint)
        else:
            self.clients = [
                HypersyncClient(ClientConfig(url=hypersync_endpoint, bearer_token=api_token))
            ]
        self.client_index = 0

        # Create a Web3 instance with a mock provider that returns Arbitrum chain_id
        # This avoids RPC calls while allowing eth_defi decoder to work
        self._web3 = Web3(ArbitrumMockProvider())

        if key_rotator:
            logger.info(f"HyperSync key rotation enabled with {key_rotator.total_keys} API key(s)")

    @property
    def client(self) -> HypersyncClient:
        """Return the currently active HyperSync client.

        :return: Active :class:`HypersyncClient` for the current key index.
        """
        return self.clients[self.client_index]

    def _address_to_bytes32(self, address: str) -> str:
        """Convert address to bytes32 format (left-padded with zeros).

        :param address: Ethereum address (with or without 0x prefix)
        :return: Bytes32 hex string with 0x prefix
        """
        addr = address.lower().replace("0x", "")
        return "0x" + addr.zfill(64)

    def _bytes32_to_address(self, bytes32: str) -> str:
        """Convert bytes32 to address format (extract last 40 chars).

        :param bytes32: Bytes32 hex string
        :return: Ethereum address with 0x prefix
        """
        if not bytes32:
            return ""
        hex_str = bytes32.replace("0x", "")
        # Address is last 40 characters of bytes32
        return "0x" + hex_str[-40:]

    def build_query(
        self,
        start_block: int = 0,
        end_block: int | None = None,
        token_addresses: list[str] | None = None,
    ) -> Query:
        """Build HyperSync query for oracle price events.

        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :param token_addresses: Optional filter for specific token addresses
        :return: HyperSync query
        """
        oracle_hash = get_oracle_price_update_hash()

        # GMX EventEmitter structure for OraclePriceUpdate:
        # topic0: EventLog1 signature (0x137a...)
        # topic1: keccak256("OraclePriceUpdate") (0x41c7...)
        # topic2: token address as bytes32 (optional filter)
        #
        # Filter by EventLog1 signature (topic0) AND OraclePriceUpdate (topic1)
        # This enables server-side filtering at HyperSync for much faster queries
        topics: list[list[str]] = [
            [EVENTLOG1_SIGNATURE],  # topic0: EventLog1 only (server-side filter)
            [oracle_hash],  # topic1: OraclePriceUpdate
        ]

        if token_addresses:
            # Filter by specific tokens (convert addresses to bytes32 format)
            topic2 = [self._address_to_bytes32(addr) for addr in token_addresses]
            topics.append(topic2)

        log_selection = LogSelection(
            address=[EVENT_EMITTER_ADDRESS.lower()],
            topics=topics,
        )

        # Request only required fields to reduce payload size
        # Removed: BLOCK_HASH, TRANSACTION_INDEX, ADDRESS, TOPIC3 (unused)
        field_selection = FieldSelection(
            log=[
                LogField.BLOCK_NUMBER,
                LogField.TRANSACTION_HASH,
                LogField.LOG_INDEX,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.DATA,
            ],
            block=[
                BlockField.NUMBER,
                BlockField.TIMESTAMP,
            ],
        )

        return Query(
            from_block=start_block,
            to_block=end_block,
            logs=[log_selection],
            field_selection=field_selection,
        )

    def _decode_single_log_sync(
        self,
        log,
        block_timestamps: dict[int, int],
    ) -> OraclePriceEvent | None:
        """Decode a single HyperSync log into OraclePriceEvent (synchronous).

        :param log: HyperSync Log object
        :param block_timestamps: Mapping of block number to timestamp
        :return: Parsed oracle price event, or None if decoding failed
        :raises ValueError: If parsing fails
        """
        block_number = log.block_number

        if block_number not in block_timestamps:
            raise ValueError(
                f"Block timestamp missing for block {block_number}. "
                f"Cannot process event without valid timestamp."
            )

        # Create camelCase dict directly for eth_defi (single pass, no intermediate dict)
        log_index = log.log_index if log.log_index is not None else 0
        transaction_hash = log.transaction_hash or ""
        eth_defi_log_dict = {
            "blockNumber": block_number,
            "blockHash": "",  # Not fetched (unused by decoder)
            "transactionHash": transaction_hash,
            "transactionIndex": 0,  # Not fetched (unused by decoder)
            "logIndex": log_index,
            "address": "",  # Not fetched (unused by decoder)
            "topics": [t for t in (log.topics or []) if t is not None],
            "data": log.data or "0x",
        }

        # Use eth_defi to decode event (CPU-bound, uses cached contract)
        event_data = decode_gmx_event(self._web3, eth_defi_log_dict)

        if event_data is None:
            raise ValueError(f"Failed to decode oracle event from log at block {block_number}")

        if event_data.event_name != "OraclePriceUpdate":
            raise ValueError(f"Expected OraclePriceUpdate, got {event_data.event_name}")

        return OraclePriceEvent(
            block_number=block_number,
            block_timestamp=block_timestamps[block_number],
            transaction_hash=transaction_hash,
            log_index=log_index,
            token=event_data.get_address("token") or "",
            provider=event_data.get_address("provider") or "",
            min_price=event_data.get_uint("minPrice") or 0,
            max_price=event_data.get_uint("maxPrice") or 0,
            timestamp=event_data.get_uint("timestamp") or 0,
        )

    def _decode_batch_sync(
        self,
        logs: list,
        block_timestamps: dict[int, int],
        stats: CollectionStats | None = None,
    ) -> list[OraclePriceEvent]:
        """Decode a batch of logs synchronously (runs in thread pool).

        This method is designed to be called via asyncio.to_thread() to
        offload CPU-bound eth_defi decoding from the async event loop.

        :param logs: List of HyperSync Log objects
        :param block_timestamps: Mapping of block number to timestamp
        :param stats: Optional CollectionStats for tracking errors
        :return: List of successfully parsed oracle price events
        """
        events = []
        for log in logs:
            try:
                event = self._decode_single_log_sync(log, block_timestamps)
                if event:
                    events.append(event)
            except Exception as e:
                error_msg = f"Block {log.block_number}: {e}"
                if stats:
                    stats.record_failure(error_msg)
                    if len(stats.first_failures) <= stats.max_failures_to_log:
                        logger.warning("Failed to parse oracle event: %s", error_msg)
                else:
                    logger.warning("Failed to parse oracle event: %s", e)
        return events

    async def _decode_batch_async(
        self,
        logs: list,
        block_timestamps: dict[int, int],
        stats: CollectionStats | None = None,
    ) -> list[OraclePriceEvent]:
        """Decode a batch of logs in a thread pool (non-blocking).

        Offloads CPU-bound eth_defi ABI decoding to a thread pool worker
        to prevent blocking the async event loop.

        :param logs: List of HyperSync Log objects
        :param block_timestamps: Mapping of block number to timestamp
        :param stats: Optional CollectionStats for tracking errors
        :return: List of successfully parsed oracle price events
        """
        if not logs:
            return []
        return await asyncio.to_thread(self._decode_batch_sync, logs, block_timestamps, stats)

    async def _collect_chunk_with_retry(
        self,
        chunk_start: int,
        chunk_end: int,
        token_addresses: list[str] | None,
        chunk_id: int,
        total_chunks: int,
        progress_callback: Callable[[str], None] | None = None,
        stats: CollectionStats | None = None,
        flush_callback: Callable[[list["OraclePriceEvent"]], None] | None = None,
    ) -> tuple[list[OraclePriceEvent], dict[int, int]]:
        """Collect oracle events with retry logic for HyperSync failures.

        Wraps _collect_chunk with retry_with_backoff to handle transient
        HyperSync failures like "failed to get arrow data from server".

        :param chunk_start: Start block for this chunk
        :param chunk_end: End block for this chunk
        :param token_addresses: Optional filter for specific token addresses
        :param chunk_id: Chunk identifier for logging
        :param total_chunks: Total number of chunks for progress calculation
        :param progress_callback: Optional callback for progress updates
        :param stats: Optional CollectionStats for tracking errors
        :param flush_callback: Optional callback to flush events to disk when
            memory threshold is reached. When provided, events are flushed
            every ``FLUSH_EVERY`` events and the returned events list contains
            only the un-flushed remainder.
        :return: Tuple of (events list, block_timestamps dict)
        """

        async def collect_chunk_operation():
            return await self._collect_chunk(
                chunk_start,
                chunk_end,
                token_addresses,
                chunk_id,
                total_chunks,
                progress_callback,
                stats,
                flush_callback,
            )

        def _advance_client():
            self.client_index = (self.client_index + 1) % len(self.clients)

        return await retry_with_backoff(
            collect_chunk_operation,
            max_retries=self.max_retries,
            base_delay=self.retry_base_delay,
            max_delay=self.retry_max_delay,
            operation_name=f"HyperSync chunk collection (blocks {chunk_start:,}-{chunk_end:,})",
            key_rotator=self.key_rotator,
            on_key_rotated=_advance_client if self.key_rotator else None,
        )

    async def _collect_chunk(
        self,
        chunk_start: int,
        chunk_end: int,
        token_addresses: list[str] | None,
        chunk_id: int,
        total_chunks: int,
        progress_callback: Callable[[str], None] | None = None,
        stats: CollectionStats | None = None,
        flush_callback: Callable[[list["OraclePriceEvent"]], None] | None = None,
    ) -> tuple[list[OraclePriceEvent], dict[int, int]]:
        """Collect oracle events for a single block range chunk using streaming API.

        Uses HyperSync streaming for efficient data fetching with server-side
        batch optimization and pre-fetching.

        :param chunk_start: Start block for this chunk
        :param chunk_end: End block for this chunk
        :param token_addresses: Optional filter for specific token addresses
        :param chunk_id: Chunk identifier for logging
        :param total_chunks: Total number of chunks for progress calculation
        :param progress_callback: Optional callback for progress updates
        :param stats: Optional CollectionStats for tracking errors
        :param flush_callback: Optional callback to flush events to disk when
            memory threshold is reached. Events are flushed every
            ``FLUSH_EVERY`` events and the list is cleared to free memory.
        :return: Tuple of (events list, block_timestamps dict). When
            flush_callback is provided, the events list contains only the
            un-flushed remainder.
        """
        events = []
        block_timestamps = {}
        total_logs = 0
        total_flushed = 0
        last_log_time = time.time()
        current_block = chunk_start

        query = self.build_query(chunk_start, chunk_end, token_addresses)

        # Use streaming API for efficient batched fetching
        # Server optimizes batch sizes and pre-fetches while we process
        stream_config = StreamConfig()

        try:
            stream = await self.client.stream(query, stream_config)

            while True:
                # Receive next batch from stream
                response = await stream.recv()

                if response is None:
                    # Stream completed
                    break

                # Build block timestamp mapping from this batch
                if response.data.blocks:
                    for block in response.data.blocks:
                        timestamp = block.timestamp
                        if isinstance(timestamp, str) and timestamp.startswith("0x"):
                            timestamp = int(timestamp, 16)
                        elif isinstance(timestamp, str):
                            timestamp = int(timestamp)
                        block_timestamps[block.number] = timestamp
                        if block.number > current_block:
                            current_block = block.number

                # Decode batch in thread pool (non-blocking)
                if response.data.logs:
                    batch_events = await self._decode_batch_async(
                        response.data.logs, block_timestamps, stats
                    )
                    events.extend(batch_events)
                    total_logs += len(batch_events)

                # Flush events to callback when memory threshold reached
                if flush_callback is not None and len(events) >= FLUSH_EVERY:
                    logger.info(
                        f"[Chunk {chunk_id}] Flushing {len(events):,} events "
                        f"at block {current_block:,} to bound memory"
                    )
                    flush_callback(events)
                    total_flushed += len(events)
                    events = []
                    gc.collect()

                # Log progress every 5 seconds
                now = time.time()
                if now - last_log_time >= 5.0:
                    chunk_progress = (
                        (current_block - chunk_start) / (chunk_end - chunk_start) * 100
                        if chunk_end > chunk_start
                        else 100
                    )
                    in_memory = len(events)
                    msg = (
                        f"[Chunk {chunk_id}/{total_chunks}] "
                        f"Block {current_block:,} / {chunk_end:,} "
                        f"({chunk_progress:.1f}%) - {total_logs} events found"
                        f" (flushed: {total_flushed:,}, in-memory: {in_memory:,})"
                    )
                    logger.info(msg)
                    if progress_callback:
                        progress_callback(msg)
                    last_log_time = now

        finally:
            # Ensure stream is closed properly
            if "stream" in locals():
                stream.close()

        return events, block_timestamps

    async def collect_oracle_events(
        self,
        start_block: int = 0,
        end_block: int | None = None,
        token_addresses: list[str] | None = None,
        concurrency: int = 4,
        progress_callback: Callable[[str], None] | None = None,
        flush_callback: Callable[[list["OraclePriceEvent"]], None] | None = None,
    ) -> list[OraclePriceEvent]:
        """Collect oracle price events from HyperSync with parallel chunk processing.

        Splits the block range into chunks and processes them concurrently
        for faster collection of large historical ranges.

        When ``flush_callback`` is provided, events are flushed to the callback
        every ``FLUSH_EVERY`` events during collection **and** after each chunk
        completes, bounding peak memory to O(FLUSH_EVERY) instead of O(all_events).
        The returned list contains only un-flushed remainder events.

        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :param token_addresses: Optional filter for specific token addresses
        :param concurrency: Number of parallel chunks to process (default: 4)
        :param progress_callback: Optional callback for progress updates
        :param flush_callback: Optional callback to flush events incrementally.
            When provided, chunks are processed sequentially and events are
            flushed during and after each chunk to bound memory usage.
        :return: List of parsed oracle price events (remainder if flush_callback used)
        """
        # Get current block if end_block not specified (using HyperSync, no RPC needed)
        if end_block is None:

            def _advance_client_height():
                self.client_index = (self.client_index + 1) % len(self.clients)

            end_block = await retry_with_backoff(
                lambda: self.client.get_height(),
                max_retries=self.max_retries,
                base_delay=self.retry_base_delay,
                max_delay=self.retry_max_delay,
                operation_name="HyperSync get_height",
                key_rotator=self.key_rotator,
                on_key_rotated=_advance_client_height if self.key_rotator else None,
            )

        total_blocks = end_block - start_block
        logger.info(
            f"Starting oracle event collection: blocks {start_block:,} to {end_block:,} "
            f"({total_blocks:,} blocks total)"
        )
        if progress_callback:
            progress_callback(
                f"Scanning {total_blocks:,} blocks with {concurrency} parallel workers..."
            )

        # Initialize stats tracking
        stats = CollectionStats(blocks_scanned=total_blocks)

        # For small ranges, use single chunk
        if total_blocks < 1_000_000 or concurrency == 1:
            events, _ = await self._collect_chunk_with_retry(
                start_block,
                end_block,
                token_addresses,
                chunk_id=1,
                total_chunks=1,
                progress_callback=progress_callback,
                stats=stats,
                flush_callback=flush_callback,
            )

            # Flush any remaining events
            if flush_callback is not None and events:
                flush_callback(events)
                stats.total_events += len(events)
                events = []
                gc.collect()
            else:
                stats.total_events = len(events)

            stats.chunks_processed = 1

            # Log summary with error details if any
            logger.info(stats.summary())
            if stats.failed_events > 0 and stats.first_failures:
                logger.warning(
                    f"First {len(stats.first_failures)} decode failures: {stats.first_failures}"
                )

            return events

        # Split into chunks for parallel processing
        chunk_size = total_blocks // concurrency
        chunks = []
        for i in range(concurrency):
            cs = start_block + (i * chunk_size)
            ce = start_block + ((i + 1) * chunk_size) - 1 if i < concurrency - 1 else end_block
            chunks.append((cs, ce))

        logger.info(f"Splitting into {len(chunks)} chunks of ~{chunk_size:,} blocks each")

        # When flush_callback is provided, process chunks sequentially to bound
        # memory — each chunk flushes during collection and its remainder is
        # flushed after completion. The storage layer handles sorting per-symbol.
        if flush_callback is not None:
            chunk_stats = [CollectionStats() for _ in chunks]
            for i, (cs, ce) in enumerate(chunks):
                chunk_events, _ = await self._collect_chunk_with_retry(
                    cs,
                    ce,
                    token_addresses,
                    chunk_id=i + 1,
                    total_chunks=len(chunks),
                    progress_callback=progress_callback,
                    stats=chunk_stats[i],
                    flush_callback=flush_callback,
                )
                # Flush remainder from this chunk
                if chunk_events:
                    flush_callback(chunk_events)
                    stats.total_events += len(chunk_events)
                    del chunk_events
                gc.collect()

            # Aggregate stats
            stats.chunks_processed = len(chunks)
            for cs_stat in chunk_stats:
                stats.failed_events += cs_stat.failed_events
                for failure in cs_stat.first_failures:
                    if len(stats.first_failures) < stats.max_failures_to_log:
                        stats.first_failures.append(failure)

            logger.info(stats.summary())
            return []

        # No flush_callback — original parallel path: accumulate all in memory
        chunk_stats = [CollectionStats() for _ in chunks]
        tasks = [
            self._collect_chunk_with_retry(
                cs,
                ce,
                token_addresses,
                chunk_id=i + 1,
                total_chunks=len(chunks),
                progress_callback=progress_callback,
                stats=chunk_stats[i],
            )
            for i, (cs, ce) in enumerate(chunks)
        ]

        results = await asyncio.gather(*tasks)

        # Extract event lists from results (each chunk is already block-ordered)
        chunk_events = [events for events, _ in results]

        # Use merge sort O(n) since chunks are already sorted by block range
        # heapq.merge efficiently merges pre-sorted iterables
        all_events = list(heapq.merge(*chunk_events, key=lambda e: (e.block_number, e.log_index)))

        # Free chunk references
        del chunk_events
        del results
        gc.collect()

        # Aggregate stats from all chunks
        stats.total_events = len(all_events)
        stats.chunks_processed = len(chunks)
        for cs_stat in chunk_stats:
            stats.failed_events += cs_stat.failed_events
            # Collect first failures from each chunk
            for failure in cs_stat.first_failures:
                if len(stats.first_failures) < stats.max_failures_to_log:
                    stats.first_failures.append(failure)

        # Log summary with error details if any
        logger.info(stats.summary())
        if stats.failed_events > 0:
            if stats.first_failures:
                logger.warning(
                    f"First {len(stats.first_failures)} decode failures (of {stats.failed_events} total): "
                    f"{stats.first_failures}"
                )
            if progress_callback:
                progress_callback(
                    f"Completed with {stats.failed_events} decode errors "
                    f"({stats.total_events} events successfully parsed)"
                )

        return all_events
