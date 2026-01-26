"""Collect GMX oracle price update events using HyperSync.

Queries GMX EventEmitter contract for OraclePriceUpdate events,
which contain oracle prices for tokens without public Chainlink feeds.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Callable

from web3 import Web3
from hypersync import (
    HypersyncClient,
    ClientConfig,
    Query,
    LogSelection,
    FieldSelection,
    LogField,
    BlockField,
)
from eth_utils import keccak
from eth_defi.gmx.events import decode_gmx_event

from gmx_historical_data.config import EVENT_EMITTER_ADDRESS


logger = logging.getLogger(__name__)


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


class OraclePriceCollector:
    """Collect GMX oracle price events via HyperSync.

    :param hypersync_endpoint: HyperSync API endpoint
    :param rpc_url: Arbitrum RPC URL for Web3 operations
    :param api_token: Optional HyperSync API token for authentication
    """

    def __init__(
        self,
        hypersync_endpoint: str,
        rpc_url: str,
        api_token: str | None = None,
    ):
        self.hypersync_endpoint = hypersync_endpoint
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))

        # Initialize HyperSync client
        config = ClientConfig(url=hypersync_endpoint, bearer_token=api_token)
        self.client = HypersyncClient(config)

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

        # GMX EventEmitter structure:
        # topic0: EventLog1 signature (any variant)
        # topic1: keccak256("OraclePriceUpdate")
        # topic2: token address as bytes32 (optional filter)
        topics: list[list[str]] = [
            [],  # topic0: any EventLog variant
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

        field_selection = FieldSelection(
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

    def _parse_oracle_event(
        self,
        log_dict: dict,
        block_timestamps: dict[int, int],
    ) -> OraclePriceEvent:
        """Parse HyperSync log dict into OraclePriceEvent.

        Uses eth_defi to decode the GMX event data structure.

        :param log_dict: Log dictionary from HyperSync
        :param block_timestamps: Mapping of block number to timestamp
        :return: Parsed oracle price event
        :raises ValueError: If parsing fails
        """
        block_number = log_dict["block_number"]

        if block_number not in block_timestamps:
            raise ValueError(
                f"Block timestamp missing for block {block_number}. "
                f"Cannot process event without valid timestamp."
            )

        # Convert snake_case to camelCase for eth_defi compatibility
        eth_defi_log_dict = {
            "blockNumber": block_number,
            "blockHash": log_dict.get("block_hash", ""),
            "transactionHash": log_dict["transaction_hash"],
            "transactionIndex": log_dict.get("transaction_index", 0),
            "logIndex": log_dict["log_index"],
            "address": log_dict.get("address", ""),
            "topics": log_dict.get("topics", []),
            "data": log_dict.get("data", "0x"),
        }

        # Use eth_defi to decode event
        event_data = decode_gmx_event(self.web3, eth_defi_log_dict)

        if event_data is None:
            raise ValueError(f"Failed to decode oracle event from log: {log_dict}")

        # Validate event type
        if event_data.event_name != "OraclePriceUpdate":
            raise ValueError(
                f"Expected OraclePriceUpdate, got {event_data.event_name}"
            )

        # Extract oracle price fields from event data
        # OraclePriceUpdate contains:
        # - addressItems[0]: token
        # - addressItems[1]: provider
        # - uintItems[0]: minPrice
        # - uintItems[1]: maxPrice
        # - uintItems[2]: timestamp
        return OraclePriceEvent(
            block_number=block_number,
            block_timestamp=block_timestamps[block_number],
            transaction_hash=log_dict["transaction_hash"],
            log_index=log_dict["log_index"],
            token=event_data.get_address("token"),
            provider=event_data.get_address("provider"),
            min_price=event_data.get_uint("minPrice"),
            max_price=event_data.get_uint("maxPrice"),
            timestamp=event_data.get_uint("timestamp"),
        )

    async def _collect_chunk(
        self,
        chunk_start: int,
        chunk_end: int,
        token_addresses: list[str] | None,
        chunk_id: int,
        total_chunks: int,
        progress_callback: Callable[[str], None] | None = None,
    ) -> tuple[list[OraclePriceEvent], dict[int, int]]:
        """Collect oracle events for a single block range chunk.

        :param chunk_start: Start block for this chunk
        :param chunk_end: End block for this chunk
        :param token_addresses: Optional filter for specific token addresses
        :param chunk_id: Chunk identifier for logging
        :param total_chunks: Total number of chunks for progress calculation
        :param progress_callback: Optional callback for progress updates
        :return: Tuple of (events list, block_timestamps dict)
        """
        events = []
        block_timestamps = {}
        current_block = chunk_start
        total_logs = 0
        last_log_time = time.time()

        while True:
            query = self.build_query(current_block, chunk_end, token_addresses)
            response = await self.client.get(query)

            # Build block timestamp mapping from this batch
            if response.data.blocks:
                for block in response.data.blocks:
                    timestamp = block.timestamp
                    if isinstance(timestamp, str) and timestamp.startswith("0x"):
                        timestamp = int(timestamp, 16)
                    elif isinstance(timestamp, str):
                        timestamp = int(timestamp)
                    block_timestamps[block.number] = timestamp

            # Parse events from this batch
            batch_count = 0
            max_block_in_batch = current_block

            if response.data.logs:
                for log in response.data.logs:
                    try:
                        if log.block_number > max_block_in_batch:
                            max_block_in_batch = log.block_number

                        log_dict = {
                            "block_number": log.block_number,
                            "block_hash": log.block_hash or "",
                            "transaction_hash": log.transaction_hash or "",
                            "transaction_index": (
                                log.transaction_index
                                if log.transaction_index is not None
                                else 0
                            ),
                            "log_index": log.log_index if log.log_index is not None else 0,
                            "address": log.address or "",
                            "topics": [t for t in (log.topics or []) if t is not None],
                            "data": log.data or "0x",
                        }

                        event = self._parse_oracle_event(log_dict, block_timestamps)
                        events.append(event)
                        batch_count += 1

                    except Exception as e:
                        logger.warning("Failed to parse oracle event: %s", e)
                        continue

            total_logs += batch_count

            # Log progress every 5 seconds
            now = time.time()
            if now - last_log_time >= 5.0:
                chunk_progress = (
                    (current_block - chunk_start) / (chunk_end - chunk_start) * 100
                    if chunk_end > chunk_start
                    else 100
                )
                msg = (
                    f"[Chunk {chunk_id}/{total_chunks}] "
                    f"Block {current_block:,} / {chunk_end:,} "
                    f"({chunk_progress:.1f}%) - {total_logs} events found"
                )
                logger.info(msg)
                if progress_callback:
                    progress_callback(msg)
                last_log_time = now

            # Pagination logic
            if response.next_block is not None and response.next_block > current_block:
                current_block = response.next_block
            elif batch_count == 0 or max_block_in_batch == current_block:
                break
            else:
                current_block = max_block_in_batch + 1

            if current_block > chunk_end:
                break

        return events, block_timestamps

    async def collect_oracle_events(
        self,
        start_block: int = 0,
        end_block: int | None = None,
        token_addresses: list[str] | None = None,
        concurrency: int = 4,
        progress_callback: Callable[[str], None] | None = None,
    ) -> list[OraclePriceEvent]:
        """Collect oracle price events from HyperSync with parallel chunk processing.

        Splits the block range into chunks and processes them concurrently
        for faster collection of large historical ranges.

        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :param token_addresses: Optional filter for specific token addresses
        :param concurrency: Number of parallel chunks to process (default: 4)
        :param progress_callback: Optional callback for progress updates
        :return: List of parsed oracle price events
        """
        # Get current block if end_block not specified
        if end_block is None:
            end_block = self.web3.eth.block_number

        total_blocks = end_block - start_block
        logger.info(
            f"Starting oracle event collection: blocks {start_block:,} to {end_block:,} "
            f"({total_blocks:,} blocks total)"
        )
        if progress_callback:
            progress_callback(
                f"Scanning {total_blocks:,} blocks with {concurrency} parallel workers..."
            )

        # For small ranges, use single chunk
        if total_blocks < 1_000_000 or concurrency == 1:
            events, _ = await self._collect_chunk(
                start_block,
                end_block,
                token_addresses,
                chunk_id=1,
                total_chunks=1,
                progress_callback=progress_callback,
            )
            logger.info(
                f"Collected {len(events)} oracle price events "
                f"from blocks {start_block:,} to {end_block:,}"
            )
            return events

        # Split into chunks for parallel processing
        chunk_size = total_blocks // concurrency
        chunks = []
        for i in range(concurrency):
            chunk_start = start_block + (i * chunk_size)
            chunk_end = (
                start_block + ((i + 1) * chunk_size) - 1
                if i < concurrency - 1
                else end_block
            )
            chunks.append((chunk_start, chunk_end))

        logger.info(
            f"Splitting into {len(chunks)} parallel chunks of ~{chunk_size:,} blocks each"
        )

        # Process chunks in parallel
        tasks = [
            self._collect_chunk(
                chunk_start,
                chunk_end,
                token_addresses,
                chunk_id=i + 1,
                total_chunks=len(chunks),
                progress_callback=progress_callback,
            )
            for i, (chunk_start, chunk_end) in enumerate(chunks)
        ]

        results = await asyncio.gather(*tasks)

        # Merge results from all chunks
        all_events = []
        for events, _ in results:
            all_events.extend(events)

        # Sort by block number and log index to maintain order
        all_events.sort(key=lambda e: (e.block_number, e.log_index))

        logger.info(
            f"Collected {len(all_events)} oracle price events "
            f"from blocks {start_block:,} to {end_block:,}"
        )

        return all_events
