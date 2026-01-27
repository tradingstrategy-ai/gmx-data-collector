"""RPC-based Chainlink historical data collection.

This module provides a fallback mechanism for collecting historical Chainlink data
when HyperSync fails. It uses JSON-RPC batch requests for efficient data fetching.

Uses getAnswer() and getTimestamp() methods from the older AggregatorInterface
which work with JSON-RPC batching (getRoundData has access control issues).

Reference: https://docs.chain.link/data-feeds/api-reference
"""

import time
from typing import Optional, Callable
from dataclasses import dataclass

import requests
from web3 import Web3
from web3.exceptions import ContractLogicError, BadFunctionCallOutput
from rich.console import Console

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

    Uses JSON-RPC batch requests with getAnswer/getTimestamp for efficient
    bulk data fetching. Falls back to getRoundData for single queries.

    :param web3: Web3 instance connected to Arbitrum RPC
    """

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

    def __init__(self, web3: Web3):
        """Initialize RPC collector.

        :param web3: Web3 instance connected to Arbitrum
        """
        self.web3 = web3
        self.rpc_url = web3.provider.endpoint_uri

    def _call_with_retry(
        self, contract_function, max_retries: int = 3, backoff: float = 1.0
    ):
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

    def get_latest_round(self, aggregator_address: str) -> Optional[ChainlinkRound]:
        """Get latest round data from aggregator.

        :param aggregator_address: Aggregator contract address
        :return: Latest round data or None if failed
        """
        try:
            aggregator_address = Web3.to_checksum_address(aggregator_address)
            contract = self.web3.eth.contract(
                address=aggregator_address, abi=self.AGGREGATOR_V3_ABI
            )

            round_id, answer, started_at, updated_at, answered_in_round = (
                self._call_with_retry(contract.functions.latestRoundData())
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

    def get_round_data(
        self, aggregator_address: str, round_id: int
    ) -> Optional[ChainlinkRound]:
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
        self, aggregator_address: str, round_ids: list[int]
    ) -> list[Optional[ChainlinkRound]]:
        """Get multiple rounds using JSON-RPC batch requests.

        Uses getAnswer() and getTimestamp() from the old AggregatorInterface
        which work with JSON-RPC batching (getRoundData has access control issues).

        :param aggregator_address: Aggregator contract address
        :param round_ids: List of round IDs to query
        :return: List of round data (None for failed/missing rounds)
        """
        if not round_ids:
            return []

        aggregator_address = Web3.to_checksum_address(aggregator_address)
        contract = self.web3.eth.contract(
            address=aggregator_address, abi=self.AGGREGATOR_OLD_ABI
        )

        # Build JSON-RPC batch request - interleave getAnswer and getTimestamp
        batch_requests = []
        for i, round_id in enumerate(round_ids):
            answer_data = contract.encode_abi("getAnswer", [round_id])
            timestamp_data = contract.encode_abi("getTimestamp", [round_id])

            batch_requests.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{"to": aggregator_address, "data": answer_data}, "latest"],
                    "id": i * 2,  # Even IDs for answers
                }
            )
            batch_requests.append(
                {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [{"to": aggregator_address, "data": timestamp_data}, "latest"],
                    "id": i * 2 + 1,  # Odd IDs for timestamps
                }
            )

        # Execute batch request
        try:
            response = requests.post(
                self.rpc_url,
                json=batch_requests,
                headers={"Content-Type": "application/json"},
                timeout=120,
            )
            results = response.json()
        except Exception as e:
            console.print(f"  [yellow]⚠ Batch RPC failed: {e}[/yellow]")
            return [None] * len(round_ids)

        # Handle both list and single response
        if not isinstance(results, list):
            results = [results]

        # Index by ID for fast lookup
        results_by_id = {r.get("id"): r for r in results}

        # Decode results
        rounds = []
        for i, round_id in enumerate(round_ids):
            answer_result = results_by_id.get(i * 2)
            timestamp_result = results_by_id.get(i * 2 + 1)

            if (
                answer_result
                and "result" in answer_result
                and answer_result["result"]
                and timestamp_result
                and "result" in timestamp_result
                and timestamp_result["result"]
            ):
                try:
                    answer = int(answer_result["result"], 16)
                    # Handle signed int256
                    if answer >= 2**255:
                        answer -= 2**256
                    timestamp = int(timestamp_result["result"], 16)

                    rounds.append(
                        ChainlinkRound(
                            round_id=round_id,
                            answer=answer,
                            started_at=timestamp,  # Use same as updated_at
                            updated_at=timestamp,
                            answered_in_round=round_id,
                        )
                    )
                except Exception:
                    rounds.append(None)
            else:
                rounds.append(None)

        return rounds

    def collect_historical_rounds(
        self,
        aggregator_address: str,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        max_rounds: int = 1000000,
        batch_size: int = 2000,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> list[ChainlinkRound]:
        """Collect historical rounds via RPC using JSON-RPC batch requests.

        Strategy:
        1. Get latest round
        2. Binary search to find start/end round IDs
        3. Batch fetch rounds using JSON-RPC batching (~700 rounds/sec)

        :param aggregator_address: Aggregator contract address
        :param start_timestamp: Start timestamp (None = collect all)
        :param end_timestamp: End timestamp (None = latest)
        :param max_rounds: Maximum rounds to collect (safety limit)
        :param batch_size: Rounds per batch request (default: 2000)
        :param progress_callback: Optional callback for progress updates
        :return: List of historical rounds
        """
        console.print("  [cyan]Collecting via RPC (JSON-RPC batch mode)...[/cyan]")

        # Get latest round
        latest_round = self.get_latest_round(aggregator_address)
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
                aggregator_address,
                target_timestamp=end_timestamp,
                latest_round_id=latest_round.round_id,
            )
        else:
            end_round_id = latest_round.round_id

        # Determine start round
        if start_timestamp:
            console.print(f"  [dim]Finding round at start timestamp {start_timestamp}...[/dim]")
            start_round_id = self._binary_search_round_by_timestamp(
                aggregator_address,
                target_timestamp=start_timestamp,
                latest_round_id=end_round_id,
                search_backwards=True,
            )
        else:
            start_round_id = 1

        total_rounds = end_round_id - start_round_id + 1
        console.print(
            f"  [dim]Collecting rounds {start_round_id:,} → {end_round_id:,} "
            f"({total_rounds:,} total)[/dim]"
        )

        # Limit to max_rounds
        if total_rounds > max_rounds:
            console.print(
                f"  [yellow]⚠ Limiting to {max_rounds:,} rounds (requested {total_rounds:,})[/yellow]"
            )
            total_rounds = max_rounds

        # Create round IDs to fetch
        round_ids = list(range(start_round_id, min(start_round_id + total_rounds, end_round_id + 1)))

        num_batches = (len(round_ids) + batch_size - 1) // batch_size
        console.print(f"  [dim]Fetching in {num_batches:,} batches of {batch_size} rounds each...[/dim]")

        # Collect rounds in batches using Multicall3
        all_rounds = []
        collected = 0
        failed = 0

        for i in range(0, len(round_ids), batch_size):
            batch = round_ids[i : i + batch_size]
            batch_results = self.get_rounds_batch(aggregator_address, batch)

            for result in batch_results:
                if result:
                    all_rounds.append(result)
                    collected += 1
                else:
                    failed += 1

            # Progress update
            batch_num = i // batch_size + 1
            if batch_num % 10 == 0 or batch_num == num_batches:
                progress_msg = f"Batch {batch_num:,}/{num_batches:,}: {collected:,} rounds collected ({failed:,} failed)"
                console.print(f"  [dim]{progress_msg}[/dim]")
                if progress_callback:
                    progress_callback(progress_msg)

        console.print(
            f"  [green]✓ Collected {len(all_rounds):,} rounds via Multicall3 "
            f"({failed:,} failed/missing)[/green]"
        )
        return all_rounds

    def _binary_search_round_by_timestamp(
        self,
        aggregator_address: str,
        target_timestamp: int,
        latest_round_id: int,
        search_backwards: bool = True,
    ) -> int:
        """Find round ID closest to target timestamp using binary search.

        :param aggregator_address: Aggregator contract address
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
            round_data = self.get_round_data(aggregator_address, mid)

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
