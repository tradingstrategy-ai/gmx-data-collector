"""RPC-based Chainlink historical data collection.

This module provides a fallback mechanism for collecting historical Chainlink data
when HyperSync fails. It uses direct RPC calls to aggregator contracts following
the official Chainlink documentation.

Reference: https://docs.chain.link/data-feeds/api-reference
"""

import time
from typing import Optional
from dataclasses import dataclass
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

    Uses the Aggregator V3 interface to query historical round data.
    This is slower than HyperSync but more reliable as a fallback.

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

    def __init__(self, web3: Web3):
        """Initialize RPC collector.

        :param web3: Web3 instance connected to Arbitrum
        """
        self.web3 = web3

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

    def collect_historical_rounds(
        self,
        aggregator_address: str,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        max_rounds: int = 10000,
        batch_size: int = 100,
    ) -> list[ChainlinkRound]:
        """Collect historical rounds via RPC using binary search strategy.

        Strategy:
        1. Get latest round
        2. Binary search backwards to find start_timestamp
        3. Collect all rounds between start and end

        :param aggregator_address: Aggregator contract address
        :param start_timestamp: Start timestamp (None = collect all)
        :param end_timestamp: End timestamp (None = latest)
        :param max_rounds: Maximum rounds to collect (safety limit)
        :param batch_size: Rounds to collect per progress update
        :return: List of historical rounds
        """
        console.print("  [cyan]Collecting via RPC (fallback mode)...[/cyan]")

        # Get latest round
        latest_round = self.get_latest_round(aggregator_address)
        if not latest_round:
            return []

        console.print(
            f"  [dim]Latest round: {latest_round.round_id} "
            f"at timestamp {latest_round.updated_at}[/dim]"
        )

        # Determine end round
        if end_timestamp and latest_round.updated_at > end_timestamp:
            # Binary search for round at end_timestamp
            end_round_id = self._binary_search_round_by_timestamp(
                aggregator_address,
                target_timestamp=end_timestamp,
                latest_round_id=latest_round.round_id,
            )
        else:
            end_round_id = latest_round.round_id

        # Determine start round
        if start_timestamp:
            start_round_id = self._binary_search_round_by_timestamp(
                aggregator_address,
                target_timestamp=start_timestamp,
                latest_round_id=end_round_id,
                search_backwards=True,
            )
        else:
            # Start from round 1 (earliest)
            start_round_id = 1

        console.print(
            f"  [dim]Collecting rounds {start_round_id} → {end_round_id}[/dim]"
        )

        # Collect rounds
        rounds = []
        collected = 0

        for round_id in range(start_round_id, end_round_id + 1):
            if collected >= max_rounds:
                console.print(
                    f"  [yellow]⚠ Hit max_rounds limit ({max_rounds})[/yellow]"
                )
                break

            round_data = self.get_round_data(aggregator_address, round_id)
            if round_data:
                rounds.append(round_data)
                collected += 1

                # Progress update
                if collected % batch_size == 0:
                    console.print(f"  [dim]Collected {collected:,} rounds...[/dim]")

        console.print(f"  [green]✓ Collected {len(rounds):,} rounds via RPC[/green]")
        return rounds

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
