"""Discover historical Chainlink aggregator addresses.

Chainlink uses a proxy pattern where the proxy address is constant, but the
underlying aggregator contract can change over time (phase upgrades). Since
AnswerUpdated events are emitted by aggregators, not proxies, we need to
discover all historical aggregator addresses for complete data coverage.
"""

import time

from web3 import Web3
from web3.exceptions import BadFunctionCallOutput, ContractLogicError


class AggregatorDiscovery:
    """Discover all historical aggregator addresses for a Chainlink feed.

    :param web3: Web3 instance connected to Arbitrum RPC
    """

    # ABI for relevant proxy methods
    PROXY_ABI = [
        {
            "constant": True,
            "inputs": [],
            "name": "aggregator",
            "outputs": [{"name": "", "type": "address"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [],
            "name": "phaseId",
            "outputs": [{"name": "", "type": "uint16"}],
            "type": "function",
        },
    ]

    # Event signature for phase changes
    # event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 timestamp)
    ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"

    def __init__(self, web3: Web3):
        """Initialize aggregator discovery.

        :param web3: Web3 instance connected to Arbitrum
        """
        self.web3 = web3

    def _call_with_retry(self, contract_function, max_retries: int = 3, backoff: float = 1.0):
        """Call contract function with retry logic for RPC failures.

        :param contract_function: Web3 contract function to call
        :param max_retries: Maximum number of retry attempts
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

                # Don't retry if it's a contract logic error (wrong ABI, wrong address)
                if isinstance(e, ContractLogicError):
                    raise

                if attempt < max_retries - 1:
                    wait_time = backoff * (2**attempt)
                    time.sleep(wait_time)
                    continue

        raise last_error

    def get_current_aggregator(self, proxy_address: str) -> str:
        """Get current aggregator address from proxy with retry logic.

        :param proxy_address: Chainlink proxy contract address
        :return: Current aggregator contract address
        :raises: Exception if RPC call fails after retries
        """
        proxy_address = Web3.to_checksum_address(proxy_address)
        proxy = self.web3.eth.contract(address=proxy_address, abi=self.PROXY_ABI)
        aggregator = self._call_with_retry(proxy.functions.aggregator())
        return Web3.to_checksum_address(aggregator)

    def get_current_phase_id(self, proxy_address: str) -> int:
        """Get current phase ID from proxy with retry logic.

        :param proxy_address: Chainlink proxy contract address
        :return: Current phase ID
        :raises: Exception if RPC call fails after retries
        """
        proxy_address = Web3.to_checksum_address(proxy_address)
        proxy = self.web3.eth.contract(address=proxy_address, abi=self.PROXY_ABI)
        return self._call_with_retry(proxy.functions.phaseId())

    def get_aggregator_info(self, proxy_address: str) -> dict:
        """Get comprehensive information about a feed's aggregators.

        :param proxy_address: Chainlink proxy contract address
        :return: Dictionary with current aggregator and phase info
        """
        current_aggregator = self.get_current_aggregator(proxy_address)
        current_phase = self.get_current_phase_id(proxy_address)

        return {
            "proxy": Web3.to_checksum_address(proxy_address),
            "current_aggregator": current_aggregator,
            "current_phase": current_phase,
        }
