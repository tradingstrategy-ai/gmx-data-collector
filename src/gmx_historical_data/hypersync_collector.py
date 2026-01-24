"""Collect Chainlink AnswerUpdated events using HyperSync.

HyperSync provides 100-2000x speedup over traditional RPC calls by
querying blockchain events directly through an optimized indexing system.
"""

import asyncio
from typing import Iterator
from dataclasses import dataclass
import hypersync
from hypersync import (
    ClientConfig,
    Query,
    LogSelection,
    FieldSelection,
    LogField,
    BlockField,
)

from gmx_historical_data.config import ANSWER_UPDATED_TOPIC
from gmx_historical_data.event_decoder import AnswerUpdatedEvent, EventDecoder


@dataclass
class CollectionStats:
    """Statistics for an event collection run.

    :param total_events: Total number of events collected
    :param blocks_scanned: Number of blocks scanned
    :param start_block: Starting block number
    :param end_block: Ending block number
    :param aggregators_found: Number of unique aggregator addresses found
    """

    total_events: int = 0
    blocks_scanned: int = 0
    start_block: int = 0
    end_block: int = 0
    aggregators_found: int = 0


class HyperSyncCollector:
    """Collect Chainlink events using HyperSync.

    :param endpoint: HyperSync endpoint URL
    :param api_token: Optional API token for authenticated access
    """

    def __init__(self, endpoint: str, api_token: str | None = None):
        """Initialize HyperSync collector.

        :param endpoint: HyperSync endpoint URL (e.g., 'https://arbitrum.hypersync.xyz')
        :param api_token: Optional API token for HyperSync
        """
        config = ClientConfig(url=endpoint, bearer_token=api_token)
        self.client = hypersync.HypersyncClient(config)
        self.decoder = EventDecoder()

    def build_query(
        self,
        aggregator_addresses: list[str],
        start_block: int = 0,
        end_block: int | None = None,
    ) -> Query:
        """Build HyperSync query for AnswerUpdated events.

        :param aggregator_addresses: List of aggregator contract addresses to query
        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :return: HyperSync query object
        """
        # Normalize addresses to lowercase for HyperSync
        addresses = [addr.lower() for addr in aggregator_addresses]

        # Configure log selection
        log_selection = LogSelection(
            address=addresses,
            topics=[
                [ANSWER_UPDATED_TOPIC],  # topic0: event signature
            ],
        )

        # Configure field selection
        field_selection = FieldSelection(
            log=[
                LogField.BLOCK_NUMBER,
                LogField.TRANSACTION_HASH,
                LogField.LOG_INDEX,
                LogField.ADDRESS,
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

        # Build query
        query = Query(
            from_block=start_block,
            to_block=end_block,
            logs=[log_selection],
            field_selection=field_selection,
        )

        return query

    async def _execute_query_with_retry(
        self,
        query: Query,
        max_retries: int = 5,
        initial_backoff: float = 1.0,
    ):
        """Execute HyperSync query with exponential backoff retry logic.

        :param query: HyperSync query to execute
        :param max_retries: Maximum number of retry attempts
        :param initial_backoff: Initial backoff time in seconds
        :return: HyperSync response
        :raises: Last exception if all retries fail
        """
        last_exception = None
        backoff = initial_backoff

        for attempt in range(max_retries + 1):
            try:
                response = await self.client.get(query)
                return response
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    print(f"  [Retry {attempt + 1}/{max_retries}] HyperSync query failed: {e}")
                    print(f"  Retrying in {backoff:.1f}s...")
                    await asyncio.sleep(backoff)
                    backoff *= 2  # Exponential backoff
                else:
                    print(f"  All {max_retries} retry attempts failed")

        raise last_exception

    async def collect_events(
        self,
        aggregator_addresses: list[str],
        start_block: int = 0,
        end_block: int | None = None,
        batch_size: int = 10000,
        max_retries: int = 5,
    ) -> list[list[AnswerUpdatedEvent]]:
        """Collect AnswerUpdated events from HyperSync.

        Returns batches of events.

        :param aggregator_addresses: List of aggregator contract addresses
        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :param batch_size: Number of events to yield per batch
        :param max_retries: Maximum number of retry attempts for failed requests
        :return: List of batches of decoded AnswerUpdatedEvents
        """
        query = self.build_query(aggregator_addresses, start_block, end_block)

        # Execute query with retry logic
        response = await self._execute_query_with_retry(query, max_retries=max_retries)

        # Process logs
        all_batches = []
        batch = []

        # Create a mapping of block_number -> timestamp for efficient lookup
        block_timestamps = (
            {block.number: block.timestamp for block in response.data.blocks}
            if response.data.blocks
            else {}
        )

        for log in response.data.logs:
            # Convert log to dict format expected by decoder
            log_data = {
                "block_number": log.block_number,
                "block_timestamp": block_timestamps.get(log.block_number, 0),
                "transaction_hash": log.transaction_hash
                if log.transaction_hash
                else "",
                "log_index": log.log_index if log.log_index is not None else 0,
                "address": log.address if log.address else "",
                "topics": [topic for topic in (log.topics or []) if topic is not None],
                "data": log.data if log.data else "0x",
            }

            try:
                event = self.decoder.decode_event(log_data)
                batch.append(event)

                if len(batch) >= batch_size:
                    all_batches.append(batch)
                    batch = []
            except (ValueError, KeyError, IndexError) as e:
                print(f"Warning: Failed to decode event: {e}")
                continue

        # Add remaining events
        if batch:
            all_batches.append(batch)

        return all_batches

    async def find_first_event_block(
        self,
        aggregator_addresses: list[str],
        search_start: int = 0,
        chunk_size: int = 1_000_000,
        max_retries: int = 3,
    ) -> int | None:
        """Find the block number of the first AnswerUpdated event.

        Searches in chunks to efficiently find when the aggregator started.

        :param aggregator_addresses: List of aggregator contract addresses
        :param search_start: Starting block for search
        :param chunk_size: Size of block chunks to search
        :param max_retries: Maximum number of retry attempts for failed requests
        :return: Block number of first event, or None if no events found
        """
        current_block = search_start

        # Search in chunks until we find events
        for _ in range(20):  # Limit to 20 chunks (~20M blocks)
            query = self.build_query(
                aggregator_addresses,
                current_block,
                current_block + chunk_size
            )

            try:
                response = await self._execute_query_with_retry(query, max_retries=max_retries)

                if response.data.logs:
                    # Found events! Return the first one
                    first_block = min(log.block_number for log in response.data.logs)
                    return first_block

                # No events in this chunk, try next
                current_block += chunk_size

            except Exception as e:
                print(f"  Warning: Error searching blocks {current_block}-{current_block + chunk_size}: {e}")
                break

        return None

    async def collect_all_events(
        self,
        aggregator_addresses: list[str],
        start_block: int = 0,
        end_block: int | None = None,
        auto_detect_start: bool = True,
    ) -> tuple[list[AnswerUpdatedEvent], CollectionStats]:
        """Collect all events at once.

        For smaller datasets or when you want all events in memory.

        :param aggregator_addresses: List of aggregator contract addresses
        :param start_block: Starting block number
        :param end_block: Ending block number (None = latest)
        :param auto_detect_start: If True, automatically find first event block
        :return: Tuple of (all events, collection statistics)
        """
        # Auto-detect the first event block if requested
        if auto_detect_start and start_block == 0:
            print("  Finding first oracle update...")
            first_block = await self.find_first_event_block(aggregator_addresses)
            if first_block is not None:
                start_block = first_block
                print(f"  First oracle update found at block {start_block}")

        all_events = []
        stats = CollectionStats()

        batches = await self.collect_events(
            aggregator_addresses, start_block, end_block
        )
        for batch in batches:
            all_events.extend(batch)

        if all_events:
            stats.total_events = len(all_events)
            stats.start_block = min(e.block_number for e in all_events)
            stats.end_block = max(e.block_number for e in all_events)
            stats.aggregators_found = len(set(e.aggregator_address for e in all_events))
            stats.blocks_scanned = stats.end_block - stats.start_block + 1

        return all_events, stats
