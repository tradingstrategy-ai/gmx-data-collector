"""RPC-based Chainlink historical data collection.

This module provides a fallback mechanism for collecting historical Chainlink data
when HyperSync fails. It uses JSON-RPC batch requests for efficient data fetching.

Uses getAnswer() and getTimestamp() methods from the older AggregatorInterface
which work with JSON-RPC batching (getRoundData has access control issues).

Reference: https://docs.chain.link/data-feeds/api-reference
"""

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from eth_defi.provider.multi_provider import create_multi_provider_web3
from rich.console import Console
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError

console = Console()


@dataclass
class ChainlinkRound:
    """Chainlink round data.

    :param round_id: Round ID (80-bit: phaseId + aggregatorRoundId)
    :param answer: Price answer (int256)
    :param started_at: Round start timestamp
    :param updated_at: Round update timestamp
    :param answered_in_round: Round ID when answer was computed
    """

    round_id: int
    answer: int
    started_at: int
    updated_at: int
    answered_in_round: int


class ChainlinkRPCCollector:
    """Collect historical Chainlink data via RPC calls.

    Uses Multicall3 contract aggregation for efficient bulk data fetching.
    Falls back to getRoundData for single queries.

    :param web3: Web3 instance connected to Arbitrum RPC
    """

    # Multicall3 contract address (same on all chains)
    MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

    # Multicall3 ABI (only aggregate3 function needed)
    MULTICALL3_ABI = [
        {
            "inputs": [
                {
                    "components": [
                        {"name": "target", "type": "address"},
                        {"name": "allowFailure", "type": "bool"},
                        {"name": "callData", "type": "bytes"},
                    ],
                    "name": "calls",
                    "type": "tuple[]",
                }
            ],
            "name": "aggregate3",
            "outputs": [
                {
                    "components": [
                        {"name": "success", "type": "bool"},
                        {"name": "returnData", "type": "bytes"},
                    ],
                    "name": "returnData",
                    "type": "tuple[]",
                }
            ],
            "stateMutability": "payable",
            "type": "function",
        }
    ]

    # Aggregator V3 Interface ABI (official Chainlink interface)
    AGGREGATOR_V3_ABI = [
        {
            "inputs": [],
            "name": "latestRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"},
            ],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [{"name": "_roundId", "type": "uint80"}],
            "name": "getRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"},
            ],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "description",
            "outputs": [{"name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

    # Chainlink Proxy ABI - phaseAggregators lookup for multi-phase backfill
    PROXY_ABI = [
        {
            "inputs": [{"name": "phaseId", "type": "uint16"}],
            "name": "phaseAggregators",
            "outputs": [{"name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "phaseId",
            "outputs": [{"name": "", "type": "uint16"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

    # Old AggregatorInterface - simpler methods that work with JSON-RPC batching
    AGGREGATOR_OLD_ABI = [
        {
            "inputs": [{"name": "roundId", "type": "uint256"}],
            "name": "getAnswer",
            "outputs": [{"name": "", "type": "int256"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [{"name": "roundId", "type": "uint256"}],
            "name": "getTimestamp",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "latestRound",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

    def __init__(self, web3: Web3 | None = None, rpc_config: str | None = None):
        """Initialize RPC collector with optional multi-provider support.

        :param web3: Web3 instance (backward compatibility)
        :param rpc_config: Space-separated RPC URLs for multi-provider failover
        :raises ValueError: If neither web3 nor rpc_config provided
        """
        if rpc_config:
            # Use multi-provider with space-separated URLs
            self.web3 = create_multi_provider_web3(rpc_config)
            provider_count = (
                len(self.web3.get_fallback_provider().providers)
                if hasattr(self.web3.get_fallback_provider(), "providers")
                else 1
            )
            console.print(
                f"  [green]Chainlink collector: {provider_count} RPC provider(s) with automatic failover[/green]"
            )
            self.rpc_url = rpc_config.split()[0]  # First URL as primary
        elif web3:
            # Use provided Web3 instance (backward compatibility)
            self.web3 = web3
            self.rpc_url = web3.provider.endpoint_uri
            console.print(
                "  [yellow]Chainlink collector: Single RPC provider (no automatic failover)[/yellow]"
            )
        else:
            raise ValueError("Either web3 or rpc_config must be provided")

        # Adaptive batch sizing for 413 Payload Too Large handling
        self._optimal_batch_size = 1500  # Safe default for most RPC providers
        self._batch_size_reduced = False  # Track if we've had to reduce batch size
        self._consecutive_failures = 0  # Track consecutive batch failures

    def _call_with_retry(self, contract_function, max_retries: int = 3, backoff: float = 1.0):
        """Call contract function with retry logic.

        :param contract_function: Web3 contract function to call
        :param max_retries: Maximum retry attempts
        :param backoff: Initial backoff time in seconds
        :return: Function call result
        :raises: Exception if all retries fail
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                return contract_function.call()
            except (ContractLogicError, BadFunctionCallOutput, Exception) as e:
                last_error = e

                # Don't retry contract logic errors
                if isinstance(e, ContractLogicError):
                    raise

                if attempt < max_retries - 1:
                    wait_time = backoff * (2**attempt)
                    time.sleep(wait_time)
                    continue

        raise last_error

    def get_latest_round(self, aggregator_address: str) -> ChainlinkRound | None:
        """Get latest round data from aggregator.

        :param aggregator_address: Aggregator contract address
        :return: Latest round data or None if failed
        """
        try:
            aggregator_address = Web3.to_checksum_address(aggregator_address)
            contract = self.web3.eth.contract(
                address=aggregator_address, abi=self.AGGREGATOR_V3_ABI
            )

            round_id, answer, started_at, updated_at, answered_in_round = self._call_with_retry(
                contract.functions.latestRoundData()
            )

            return ChainlinkRound(
                round_id=round_id,
                answer=answer,
                started_at=started_at,
                updated_at=updated_at,
                answered_in_round=answered_in_round,
            )
        except Exception as e:
            console.print(f"  [red]✗ Failed to get latest round: {e}[/red]")
            return None

    def get_round_data(self, aggregator_address: str, round_id: int) -> ChainlinkRound | None:
        """Get specific round data from aggregator.

        :param aggregator_address: Aggregator contract address
        :param round_id: Round ID to query
        :return: Round data or None if failed
        """
        try:
            aggregator_address = Web3.to_checksum_address(aggregator_address)
            contract = self.web3.eth.contract(
                address=aggregator_address, abi=self.AGGREGATOR_V3_ABI
            )

            round_id_result, answer, started_at, updated_at, answered_in_round = (
                self._call_with_retry(contract.functions.getRoundData(round_id))
            )

            return ChainlinkRound(
                round_id=round_id_result,
                answer=answer,
                started_at=started_at,
                updated_at=updated_at,
                answered_in_round=answered_in_round,
            )
        except ContractLogicError:
            # Round doesn't exist - this is expected when scanning
            return None
        except Exception as e:
            console.print(f"  [yellow]⚠ Round {round_id} query failed: {e}[/yellow]")
            return None

    def get_rounds_batch(
        self, feed_address: str, round_ids: list[int]
    ) -> list[ChainlinkRound | None]:
        """Get multiple rounds using Multicall3 aggregation.

        Uses Multicall3 contract to aggregate getRoundData() calls (V3 interface)
        into a single RPC request for maximum efficiency.

        IMPORTANT: Use the Feed Proxy address (e.g., 0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612
        for ETH/USD), NOT the underlying aggregator address. The aggregator contract
        blocks contract-to-contract calls (Multicall3) with "No access" error.

        :param feed_address: Feed proxy contract address (NOT aggregator!)
        :param round_ids: List of round IDs to query
        :return: List of round data (None for failed/missing rounds)
        """
        if not round_ids:
            return []

        # Check if batch is too large and needs splitting
        if len(round_ids) > self._optimal_batch_size:
            # Split into smaller chunks based on optimal batch size
            chunk_size = self._optimal_batch_size
            all_rounds = []

            for i in range(0, len(round_ids), chunk_size):
                chunk = round_ids[i : i + chunk_size]
                chunk_results = self.get_rounds_batch(feed_address, chunk)
                all_rounds.extend(chunk_results)

            return all_rounds

        feed_address = Web3.to_checksum_address(feed_address)

        # Create contract instances - use V3 ABI with getRoundData
        feed_contract = self.web3.eth.contract(address=feed_address, abi=self.AGGREGATOR_V3_ABI)
        multicall3_contract = self.web3.eth.contract(
            address=self.MULTICALL3_ADDRESS, abi=self.MULTICALL3_ABI
        )

        # Build Multicall3 calls using getRoundData (V3 interface)
        multicall_calls = []
        for round_id in round_ids:
            # getRoundData returns (roundId, answer, startedAt, updatedAt, answeredInRound)
            round_data = feed_contract.encode_abi("getRoundData", [round_id])
            multicall_calls.append(
                {
                    "target": feed_address,
                    "allowFailure": True,  # Don't revert entire batch if one call fails
                    "callData": round_data,
                }
            )

        # Execute Multicall3.aggregate3 with retry logic and 413 handling
        max_retries = 3
        backoff = 2.0  # Initial backoff in seconds
        last_error = None

        for attempt in range(max_retries):
            try:
                results = multicall3_contract.functions.aggregate3(multicall_calls).call()
                # Success - reset failure counter
                self._consecutive_failures = 0
                break
            except Exception as e:
                last_error = e
                error_str = str(e)

                # Check for 413 Payload Too Large error
                if (
                    "413" in error_str
                    or "Payload Too Large" in error_str
                    or "request entity too large" in error_str.lower()
                ):
                    # Reduce optimal batch size
                    new_batch_size = max(500, len(round_ids) // 2)

                    if not self._batch_size_reduced:
                        console.print(
                            f"  [yellow]⚠ Payload too large ({len(round_ids)} rounds). "
                            f"Reducing batch size to {new_batch_size}[/yellow]"
                        )
                        self._optimal_batch_size = new_batch_size
                        self._batch_size_reduced = True

                    # Split this batch into smaller chunks and retry
                    chunk_size = new_batch_size
                    all_rounds = []

                    for i in range(0, len(round_ids), chunk_size):
                        chunk = round_ids[i : i + chunk_size]
                        console.print(
                            f"  [dim]Retrying with smaller batch: {len(chunk)} rounds "
                            f"(chunk {i // chunk_size + 1}/{(len(round_ids) + chunk_size - 1) // chunk_size})[/dim]"
                        )
                        chunk_results = self.get_rounds_batch(feed_address, chunk)
                        all_rounds.extend(chunk_results)

                    return all_rounds

                # Not a payload error, handle as regular retry
                self._consecutive_failures += 1

                if attempt < max_retries - 1:
                    wait_time = backoff * (2**attempt)
                    console.print(
                        f"  [yellow]⚠ Multicall3 attempt {attempt + 1}/{max_retries} failed: {e}[/yellow]"
                    )
                    console.print(f"  [dim]Retrying in {wait_time:.1f}s...[/dim]")
                    time.sleep(wait_time)
                else:
                    console.print(
                        f"  [red]✗ Multicall3 failed after {max_retries} attempts: {e}[/red]"
                    )
                    return [None] * len(round_ids)
        else:
            # All retries failed
            console.print(f"  [red]✗ Multicall3 failed: {last_error}[/red]")
            return [None] * len(round_ids)

        # Decode results - getRoundData returns (roundId, answer, startedAt, updatedAt, answeredInRound)
        rounds = []
        for i, round_id in enumerate(round_ids):
            result = results[i]

            if result[0]:  # Call succeeded
                try:
                    # Decode getRoundData result (uint80, int256, uint256, uint256, uint80)
                    return_data = result[1]

                    # Manual decoding for (uint80, int256, uint256, uint256, uint80)
                    # uint80 is padded to 32 bytes, int256/uint256 are 32 bytes each
                    # Total: 5 * 32 = 160 bytes
                    if len(return_data) >= 160:
                        # roundId: uint80 (first 32 bytes, last 10 bytes significant)
                        result_round_id = int.from_bytes(
                            return_data[0:32], byteorder="big", signed=False
                        )
                        # answer: int256 (bytes 32-64)
                        answer = int.from_bytes(return_data[32:64], byteorder="big", signed=True)
                        # startedAt: uint256 (bytes 64-96)
                        started_at = int.from_bytes(
                            return_data[64:96], byteorder="big", signed=False
                        )
                        # updatedAt: uint256 (bytes 96-128)
                        updated_at = int.from_bytes(
                            return_data[96:128], byteorder="big", signed=False
                        )
                        # answeredInRound: uint80 (bytes 128-160)
                        answered_in_round = int.from_bytes(
                            return_data[128:160], byteorder="big", signed=False
                        )

                        rounds.append(
                            ChainlinkRound(
                                round_id=result_round_id,
                                answer=answer,
                                started_at=started_at,
                                updated_at=updated_at,
                                answered_in_round=answered_in_round,
                            )
                        )
                    else:
                        rounds.append(None)
                except Exception:
                    rounds.append(None)
            else:
                # Call failed - decode error message if possible for debugging
                return_data = result[1]
                if return_data and len(return_data) >= 4:
                    # Check if this is an Error(string) revert
                    selector = return_data[:4].hex()
                    if selector == "08c379a0":  # Error(string) selector
                        try:
                            # Decode error message (skip selector + offset)
                            msg_len = int.from_bytes(return_data[36:68], byteorder="big")
                            error_msg = return_data[68 : 68 + msg_len].decode(
                                "utf-8", errors="ignore"
                            )
                            # Only log first failure to avoid spam
                            if i == 0:
                                console.print(
                                    f"  [dim]Debug: Round {round_id} failed: {error_msg}[/dim]"
                                )
                        except Exception:
                            pass
                rounds.append(None)

        return rounds

    def collect_historical_rounds(
        self,
        feed_address: str,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        max_rounds: int = 2000000,
        batch_size: int = 1500,
        concurrency: int = 4,
        progress_callback: Callable[[str], None] | None = None,
    ) -> list[ChainlinkRound]:
        """Collect historical rounds via RPC using Multicall3 aggregation.

        Strategy:
        1. Get latest round
        2. Binary search to find start/end round IDs
        3. Batch fetch rounds using Multicall3 contract with concurrent workers

        IMPORTANT: Use the Feed Proxy address (e.g., 0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612
        for ETH/USD), NOT the underlying aggregator address. The aggregator contract
        blocks contract-to-contract calls (Multicall3) with "No access" error.

        :param feed_address: Feed proxy contract address (NOT aggregator!)
        :param start_timestamp: Start timestamp (None = collect all)
        :param end_timestamp: End timestamp (None = latest)
        :param max_rounds: Maximum rounds to collect (safety limit)
        :param batch_size: Rounds per batch request (default: 1500, safe for most RPC providers)
        :param concurrency: Number of concurrent batch workers (default: 4)
        :param progress_callback: Optional callback for progress updates
        :return: List of historical rounds
        """
        console.print("  [cyan]Collecting via RPC (Multicall3 aggregation)...[/cyan]")

        # Get latest round
        latest_round = self.get_latest_round(feed_address)
        if not latest_round:
            return []

        console.print(
            f"  [dim]Latest round: {latest_round.round_id:,} "
            f"at timestamp {latest_round.updated_at}[/dim]"
        )

        # Determine end round
        if end_timestamp and latest_round.updated_at > end_timestamp:
            console.print(f"  [dim]Finding round at end timestamp {end_timestamp}...[/dim]")
            end_round_id = self._binary_search_round_by_timestamp(
                feed_address,
                target_timestamp=end_timestamp,
                latest_round_id=latest_round.round_id,
            )
        else:
            end_round_id = latest_round.round_id

        # Determine start round - need to find first valid round
        if start_timestamp:
            console.print(f"  [dim]Finding round at start timestamp {start_timestamp}...[/dim]")
            start_round_id = self._binary_search_round_by_timestamp(
                feed_address,
                target_timestamp=start_timestamp,
                latest_round_id=end_round_id,
                search_backwards=True,
            )
            # Build phase-aware ranges and clip to [start_round_id, end_round_id].
            # A single tuple spanning two phases would have a ~2^64 gap between
            # the phase-1 and phase-2 round-ID spaces, causing list(range(...))
            # to OOM before max_rounds is applied.
            all_ranges = self._get_phase_ranges(feed_address, end_round_id)
            phase_ranges = []
            for first_id, last_id in all_ranges:
                if last_id < start_round_id:
                    continue  # entire phase is before our window
                phase_ranges.append((max(first_id, start_round_id), last_id))
        else:
            # Full backfill: walk all phases so feeds that rotated aggregators
            # (e.g. ETH/USD on Arbitrum switched to Phase 2 in March 2026) are
            # collected in their entirety rather than just the current phase.
            phase_ranges = self._get_phase_ranges(feed_address, end_round_id)

        # Build the flat list of round IDs across all phases.
        # For full backfill (no start_timestamp) the phase ranges already contain
        # exactly the right set of rounds — no artificial cap is applied so the
        # collection automatically scales as new phases or rounds are added in future.
        # For timestamp-bounded queries max_rounds acts as a safety guard only.
        round_ids: list[int] = []
        for first_id, last_id in phase_ranges:
            phase_ids = list(range(first_id, last_id + 1))
            if start_timestamp:
                remaining = max_rounds - len(round_ids)
                if remaining <= 0:
                    break
                if len(phase_ids) > remaining:
                    console.print(
                        f"  [yellow]⚠ Limiting phase to {remaining:,} rounds "
                        f"(phase has {len(phase_ids):,})[/yellow]"
                    )
                    phase_ids = phase_ids[:remaining]
            round_ids.extend(phase_ids)

        total_rounds = len(round_ids)
        console.print(f"  [dim]Total rounds to fetch across all phases: {total_rounds:,}[/dim]")

        num_batches = (len(round_ids) + batch_size - 1) // batch_size
        console.print(
            f"  [dim]Fetching in {num_batches:,} batches of {batch_size} rounds "
            f"({concurrency} concurrent workers)...[/dim]"
        )

        # Create batches
        batches = []
        for i in range(0, len(round_ids), batch_size):
            batch = round_ids[i : i + batch_size]
            batch_idx = i // batch_size
            batches.append((batch_idx, batch))

        # Collect rounds in batches using concurrent workers
        all_results: dict[int, list] = {}  # batch_idx -> results
        collected = 0
        failed = 0
        completed_batches = 0

        def process_batch(batch_info: tuple[int, list[int]]) -> tuple[int, list]:
            """Process a single batch and return (batch_idx, results)."""
            batch_idx, batch_round_ids = batch_info
            results = self.get_rounds_batch(feed_address, batch_round_ids)
            return batch_idx, results

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(process_batch, b): b[0] for b in batches}

            for future in as_completed(futures):
                batch_idx, batch_results = future.result()
                all_results[batch_idx] = batch_results
                completed_batches += 1

                # Count successes/failures for this batch
                batch_collected = sum(1 for r in batch_results if r is not None)
                batch_failed = len(batch_results) - batch_collected
                collected += batch_collected
                failed += batch_failed

                # Progress update every 10 batches or at end
                if completed_batches % 10 == 0 or completed_batches == num_batches:
                    progress_msg = (
                        f"Batch {completed_batches:,}/{num_batches:,}: "
                        f"{collected:,} rounds collected ({failed:,} failed)"
                    )
                    console.print(f"  [dim]{progress_msg}[/dim]")
                    if progress_callback:
                        progress_callback(progress_msg)

        # Merge results in order
        all_rounds = []
        for batch_idx in sorted(all_results.keys()):
            for result in all_results[batch_idx]:
                if result:
                    all_rounds.append(result)

        console.print(
            f"  [green]✓ Collected {len(all_rounds):,} rounds via Multicall3 "
            f"({failed:,} failed/missing)[/green]"
        )
        return all_rounds

    def _get_phase_ranges(
        self,
        feed_address: str,
        latest_round_id: int,
    ) -> list[tuple[int, int]]:
        """Return (first_round_id, last_round_id) for every Chainlink phase.

        Chainlink L2 proxies (Arbitrum) encode phase information in the upper 64
        bits of the 80-bit round ID::

            roundId = (phaseId << 64) + aggregatorRoundId

        When a feed rotates to a new phase (new underlying aggregator), rounds from
        older phases become inaccessible to callers who only look at the current
        phase.  This method walks **all** phases from 1 to current so callers can
        fetch the full history.

        For each past phase the last round is obtained by calling ``latestRound()``
        on the phase's aggregator contract via ``phaseAggregators(phaseId)``.  The
        current phase uses ``latest_round_id`` directly.

        :param feed_address: Feed proxy contract address.
        :param latest_round_id: Latest round ID from ``latestRoundData()``.
        :returns: List of ``(first_round_id, last_round_id)`` tuples, one per phase,
            sorted oldest-first.  Phases that cannot be resolved are skipped with a
            warning.
        """
        current_phase_id = latest_round_id >> 64
        console.print(
            f"  [dim]Feed has {current_phase_id} phase(s) — collecting across all phases[/dim]"
        )

        if current_phase_id <= 1:
            # Single phase: use simple first-valid search, no proxy lookup needed
            first_valid = self._find_first_valid_round_in_phase(feed_address, latest_round_id)
            return [(first_valid, latest_round_id)]

        proxy_address = Web3.to_checksum_address(feed_address)
        proxy_contract = self.web3.eth.contract(address=proxy_address, abi=self.PROXY_ABI)

        ranges: list[tuple[int, int]] = []

        for phase_id in range(1, current_phase_id + 1):
            if phase_id == current_phase_id:
                # Current phase: last round is the live latest
                last_round_id = latest_round_id
            else:
                # Past phase: ask the proxy for this phase's aggregator, then its
                # latestRound() to find where the phase ended.
                try:
                    agg_address = self._call_with_retry(
                        proxy_contract.functions.phaseAggregators(phase_id)
                    )
                    if not agg_address or agg_address == "0x" + "0" * 40:
                        console.print(
                            f"  [yellow]⚠ Phase {phase_id}: no aggregator address, skipping[/yellow]"
                        )
                        continue

                    agg_contract = self.web3.eth.contract(
                        address=Web3.to_checksum_address(agg_address),
                        abi=self.AGGREGATOR_OLD_ABI,
                    )
                    agg_latest_round = self._call_with_retry(
                        agg_contract.functions.latestRound()
                    )
                    # Encode back to proxy round ID space
                    last_round_id = (phase_id << 64) + int(agg_latest_round)
                except Exception as exc:
                    console.print(
                        f"  [yellow]⚠ Phase {phase_id}: could not determine last round ({exc}), skipping[/yellow]"
                    )
                    continue

            # Verify the first round in this phase is actually valid
            first_valid = self._find_first_valid_round_in_phase(
                feed_address, last_round_id, phase_id=phase_id
            )
            console.print(
                f"  [dim]Phase {phase_id}: rounds {first_valid:,} → {last_round_id:,} "
                f"({last_round_id - first_valid + 1:,} rounds)[/dim]"
            )
            ranges.append((first_valid, last_round_id))

        return ranges

    def _find_first_valid_round_in_phase(
        self,
        feed_address: str,
        last_round_id: int,
        phase_id: int | None = None,
    ) -> int:
        """Binary-search for the first valid round within a single phase.

        :param feed_address: Feed proxy contract address.
        :param last_round_id: Last (highest) round ID in this phase.
        :param phase_id: Phase ID override; inferred from ``last_round_id`` if
            ``None``.
        :returns: First valid round ID in the phase.
        """
        if phase_id is None:
            phase_id = last_round_id >> 64

        first_in_phase = (phase_id << 64) + 1
        low, high = first_in_phase, last_round_id
        first_valid = last_round_id

        while low <= high:
            mid = (low + high) // 2
            round_data = self.get_round_data(feed_address, mid)

            if round_data and round_data.updated_at > 0:
                first_valid = mid
                high = mid - 1
            else:
                low = mid + 1

        return first_valid

    def _find_first_valid_round(
        self,
        feed_address: str,
        latest_round_id: int,
    ) -> int:
        """Find first valid round ID in the current phase using binary search.

        .. deprecated::
            Use :meth:`_get_phase_ranges` for full multi-phase backfill.
            This method is kept for backwards compatibility with callers that only
            need the current phase.

        :param feed_address: Feed proxy contract address.
        :param latest_round_id: Latest known round ID.
        :returns: First valid round ID in the current phase.
        """
        phase_id = latest_round_id >> 64
        first_in_phase = (phase_id << 64) + 1
        console.print(
            f"  [dim]Phase ID: {phase_id}, first round in phase: {first_in_phase:,}[/dim]"
        )
        first_valid = self._find_first_valid_round_in_phase(feed_address, latest_round_id)
        console.print(f"  [dim]First valid round: {first_valid:,}[/dim]")
        return first_valid

    def _binary_search_round_by_timestamp(
        self,
        feed_address: str,
        target_timestamp: int,
        latest_round_id: int,
        search_backwards: bool = True,
    ) -> int:
        """Find round ID closest to target timestamp using binary search.

        :param feed_address: Feed proxy contract address
        :param target_timestamp: Target timestamp to find
        :param latest_round_id: Latest known round ID
        :param search_backwards: If True, search backwards; if False, forward
        :return: Round ID closest to target timestamp
        """
        if search_backwards:
            low, high = 1, latest_round_id
        else:
            low, high = latest_round_id, latest_round_id * 2

        result_round_id = latest_round_id

        while low <= high:
            mid = (low + high) // 2
            round_data = self.get_round_data(feed_address, mid)

            if not round_data:
                # Round doesn't exist, adjust search
                if search_backwards:
                    high = mid - 1
                else:
                    low = mid + 1
                continue

            if round_data.updated_at < target_timestamp:
                result_round_id = mid
                low = mid + 1
            elif round_data.updated_at > target_timestamp:
                high = mid - 1
            else:
                # Exact match
                return mid

        return result_round_id
