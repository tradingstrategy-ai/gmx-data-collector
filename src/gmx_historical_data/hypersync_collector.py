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
    """Collect Chainlink events using HyperSync with multi-endpoint and multi-token support.

    :param endpoints: HyperSync endpoint URL(s) - string, comma-separated, or list
    :param api_tokens: API token(s) for authenticated access (string or list)
    :param use_simultaneous_queries: If True, query all endpoints simultaneously (race condition)
    """

    def __init__(
        self,
        endpoints: str | list[str],
        api_tokens: str | list[str] | None = None,
        max_concurrent_requests: int = 5,
        use_simultaneous_queries: bool = True,
    ):
        """Initialize HyperSync collector with multi-endpoint and multi-token support.

        :param endpoints: Endpoint URL(s) - single, comma-separated string, or list
                         (e.g., 'https://arbitrum.hypersync.xyz,https://42161.hypersync.xyz')
        :param api_tokens: API token(s) - single token, comma-separated string, or list
        :param max_concurrent_requests: Maximum concurrent HyperSync requests (default: 5)
        :param use_simultaneous_queries: Query all endpoints simultaneously for best latency (default: True)
        """
        self.decoder = EventDecoder()
        self.use_simultaneous_queries = use_simultaneous_queries

        # Parse endpoints
        if isinstance(endpoints, str):
            # Support comma-separated endpoints
            self.endpoints = [e.strip() for e in endpoints.split(",") if e.strip()]
            if not self.endpoints:
                raise ValueError("At least one endpoint URL required")
        else:
            self.endpoints = endpoints if endpoints else []
            if not self.endpoints:
                raise ValueError("At least one endpoint URL required")

        # Keep first endpoint for backward compatibility
        self.endpoint = self.endpoints[0]

        # Parse API tokens
        if api_tokens is None:
            self.tokens = [None]
        elif isinstance(api_tokens, str):
            # Support comma-separated tokens
            self.tokens = [t.strip() for t in api_tokens.split(",") if t.strip()]
            if not self.tokens:
                self.tokens = [None]
        else:
            self.tokens = api_tokens if api_tokens else [None]

        # Create clients for all endpoint x token combinations
        self.clients = []
        self.client_configs = []  # Track which endpoint+token each client uses
        for endpoint in self.endpoints:
            for token in self.tokens:
                config = ClientConfig(url=endpoint, bearer_token=token)
                self.clients.append(hypersync.HypersyncClient(config))
                self.client_configs.append(
                    {
                        "endpoint": endpoint,
                        "token": token[:8] + "..." if token else None,
                    }
                )

        # Current client index for round-robin
        self.current_client_idx = 0

        # Legacy single client for backward compatibility
        self.client = self.clients[0]

        # Semaphore for rate limiting concurrent requests
        self.request_semaphore = asyncio.Semaphore(max_concurrent_requests)

    def _get_next_client(self):
        """Get next client in round-robin rotation."""
        client = self.clients[self.current_client_idx]
        self.current_client_idx = (self.current_client_idx + 1) % len(self.clients)
        return client

    async def _query_single_client(
        self, client, client_idx: int, query: Query, timeout: float
    ):
        """Query a single client with timeout.

        :param client: HyperSync client
        :param client_idx: Client index for logging
        :param query: Query to execute
        :param timeout: Timeout in seconds
        :return: (response, client_idx) tuple on success
        :raises: Exception on failure
        """
        try:
            async with self.request_semaphore:
                response = await asyncio.wait_for(client.get(query), timeout=timeout)
                config = self.client_configs[client_idx]
                print(
                    f"  ✓ Response from {config['endpoint']} (token: {config['token']})"
                )
                return response, client_idx
        except Exception as e:
            config = self.client_configs[client_idx]
            # Don't print errors here - let caller handle them
            raise

    async def _execute_simultaneous_query(
        self,
        query: Query,
        timeout: float = 300.0,
    ):
        """Execute query on all endpoints+tokens simultaneously, return first success.

        This provides better latency (fastest endpoint wins) and redundancy.

        :param query: Query to execute
        :param timeout: Timeout per query attempt
        :return: HyperSync response from first successful endpoint
        :raises: Exception if all endpoints fail
        """
        # Create tasks for all clients
        tasks = []
        for idx, client in enumerate(self.clients):
            coro = self._query_single_client(client, idx, query, timeout)
            task = asyncio.create_task(coro)
            tasks.append(task)

        # Race all clients - return first success
        exceptions = []
        for completed_task in asyncio.as_completed(tasks):
            try:
                response, client_idx = await completed_task
                # Cancel remaining tasks
                for task in tasks:
                    if not task.done():
                        task.cancel()
                return response
            except Exception as e:
                exceptions.append(e)
                # Continue to next completion
                continue

        # All failed - raise last exception
        if exceptions:
            print(f"  All {len(self.clients)} endpoints failed")
            raise exceptions[-1]
        else:
            raise RuntimeError("No clients available")

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
        timeout: float = 300.0,
    ):
        """Execute HyperSync query with exponential backoff retry logic and timeout.

        If use_simultaneous_queries=True: Races all endpoints simultaneously (best latency)
        If use_simultaneous_queries=False: Uses round-robin rotation of API tokens

        Semaphore-based rate limiting controls concurrent request volume.

        :param query: HyperSync query to execute
        :param max_retries: Maximum number of retry attempts
        :param initial_backoff: Initial backoff time in seconds
        :param timeout: Timeout in seconds for each query attempt (default: 300s = 5min)
        :return: HyperSync response
        :raises: Last exception if all retries fail
        """
        last_exception = None
        backoff = initial_backoff
        tokens_tried = 0

        for attempt in range(max_retries + 1):
            try:
                # Simultaneous query mode: race all endpoints
                if self.use_simultaneous_queries and len(self.clients) > 1:
                    if attempt == 0:
                        print(
                            f"  Racing {len(self.clients)} endpoint+token combinations..."
                        )
                    response = await self._execute_simultaneous_query(query, timeout)
                    return response

                # Round-robin mode: try one client at a time
                client = self._get_next_client()

                # Use semaphore to limit concurrent requests
                async with self.request_semaphore:
                    # Add timeout to prevent hanging queries
                    response = await asyncio.wait_for(
                        client.get(query), timeout=timeout
                    )
                    return response

            except asyncio.TimeoutError as e:
                last_exception = e
                if attempt < max_retries:
                    print(
                        f"  [Retry {attempt + 1}/{max_retries}] HyperSync query timeout after {timeout}s"
                    )
                    print(f"  Retrying in {backoff:.1f}s...")
                    await asyncio.sleep(backoff)
                    backoff *= 2  # Exponential backoff
                else:
                    print(f"  All {max_retries} retry attempts failed (timeout)")

            except Exception as e:
                last_exception = e
                error_str = str(e).lower()

                # Check for rate limit errors (429)
                is_rate_limit = (
                    "429" in error_str
                    or "rate limit" in error_str
                    or "too many requests" in error_str
                )

                if is_rate_limit and len(self.clients) > 1:
                    tokens_tried += 1
                    if tokens_tried < len(self.clients):
                        # Try next token immediately (no backoff)
                        print(
                            f"  [Token rotation {tokens_tried}/{len(self.clients)}] Rate limit hit, trying next API token..."
                        )
                        continue
                    else:
                        # All tokens exhausted, apply backoff
                        print(
                            f"  [All {len(self.clients)} tokens rate limited] Applying backoff..."
                        )
                        tokens_tried = 0  # Reset for next retry cycle

                if attempt < max_retries:
                    print(
                        f"  [Retry {attempt + 1}/{max_retries}] HyperSync query failed: {e}"
                    )
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
                aggregator_addresses, current_block, current_block + chunk_size
            )

            try:
                response = await self._execute_query_with_retry(
                    query, max_retries=max_retries
                )

                if response.data.logs:
                    # Found events! Return the first one
                    first_block = min(log.block_number for log in response.data.logs)
                    return first_block

                # No events in this chunk, try next
                current_block += chunk_size

            except Exception as e:
                print(
                    f"  Warning: Error searching blocks {current_block}-{current_block + chunk_size}: {e}"
                )
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
